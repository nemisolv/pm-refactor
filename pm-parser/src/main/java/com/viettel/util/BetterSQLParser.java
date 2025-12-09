package com.viettel.util;

import com.viettel.config.ConfigManager;
import com.viettel.dal.*;
import com.viettel.repository.CommonRepository;
import com.viettel.repository.CounterKpiRepository;
import com.viettel.repository.GroupCounterCounterRepository;
import com.viettel.repository.KpiCachingRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import com.viettel.common.Constant.IntervalUnit;

@Slf4j
@Component
public class BetterSQLParser {
    private final ConfigManager configManager;
    private CounterKpiRepository counterKpiRepository;
    private GroupCounterCounterRepository groupCounterCounterRepository;
    private Util util;
    private KpiCachingRepository kpiCachingRepository;
    private CommonRepository commonRepository;
    private String RAT_TYPE_KEY = "rat_type";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final char SPACE_CHAR = ' ';
    private static final char COMMA_CHAR = ',';
    private static final char SEMI_COLON_CHAR = ';';
    private static final char DOWN_LINE_CHAR = '\n';
    private static final String DATE_TIME_KEY = "DATE_TIME_KEY";
    private static final String JOINED_KEY = "joined_key";

    private static final Map<Integer, Integer> DURATION_MULTIPLIERS = Map.of(
        IntervalUnit.MINUTE.getValue(), 60,
        IntervalUnit.SECOND.getValue(), 1,
        IntervalUnit.HOUR.getValue(), 3600,
        IntervalUnit.DATE.getValue(), 86400
    );

    private static final String PREFIX_JOIN_BASE_TABLE = "base";

    public BetterSQLParser(
        ConfigManager configManager,
        CounterKpiRepository counterKpiRepository,
        GroupCounterCounterRepository groupCounterCounterRepository,
        Util util, KpiCachingRepository kpiCachingRepository, CommonRepository commonRepository) {
        this.counterKpiRepository = counterKpiRepository;
        this.groupCounterCounterRepository = groupCounterCounterRepository;
        this.util = util;
        this.kpiCachingRepository = kpiCachingRepository;
        this.commonRepository = commonRepository;
        this.configManager = configManager;
    }

    public CustomOutput<List<QueryObject>> generateSQLCaching(
        InputStatisticData inputData,
        List<Counter> lstAllCounter,
        List<KpiFormula> lstAllKpi,
        List<CounterCat> lstAllCounterCat,
        HashMap<String, ExtraFieldObject> hmKeyField) {
        return null; // not support
    }

    private boolean isGTnow(String date) {
        try {
            return Calendar.getInstance().getTime().getTime() > sdf.parse(date).getTime();
        } catch (ParseException e) {
            log.error("ParseException: {}", e.getMessage());
            return false;
        }
    }

    public CustomOutput<List<QueryObject>> generateSQL(
        InputStatisticData inputData,
        AllInputKpiCounterObjFlatten allInputKpiCounterObjFlatten,
        List<CounterCat> lstAllCounterCat,
        List<GroupCounterCounter> lstAllGroupCounterCounter,
        LinkedHashMap<String, ExtraFieldObject> hmExtraField,
        HashMap<String, ExtraFieldObject> hmKeyField,
        String dbType) throws ParseException {


        InputStatisticData inputStatisticDataClone = inputData.clone();
        final String fromTime = inputStatisticDataClone.getFromTime();
        final String toTime = isGTnow(inputStatisticDataClone.getToTime())
            ? inputStatisticDataClone.getToTime()
            : sdf.format(Calendar.getInstance().getTime());


        List<QueryObject> lstQueryObjects = new ArrayList<>();
        StringBuilder sbSelect = new StringBuilder();
        StringBuilder sbGroupBy = new StringBuilder();
        StringBuilder sbOrderBy = new StringBuilder();
        StringBuilder sbKeyGroupBy = new StringBuilder();


        Map<String, Set<Integer>> hmGroupCounter = new HashMap<>();  // for querying counters
        Map<String, Set<KpiFormula>> hmGroupKPI = new HashMap<>();  // for querying kpis
        List<KpiFormula> lstKpiFormulaWithMultiTable = new ArrayList<>(); // for querying kpi ( multiple table)

        List<KpiFormula> allKpis = allInputKpiCounterObjFlatten.getAllKpiObj();
        List<Counter> allCounters = allInputKpiCounterObjFlatten.getAllCounterObj();
        Set<Integer> allKpiCounterId = allInputKpiCounterObjFlatten.getAllKpiCounterId();


        List<Integer> allClientRequestInputKpiCounters = Optional.ofNullable(inputStatisticDataClone.getListCounterKPI()).orElseGet(ArrayList::new);

        List<Integer> lstOriginalInputCounterKpi = new ArrayList<>(inputStatisticDataClone.getListCounterKPI());

        // kpi tu nhieu bang
        List<KpiFormula> ignoreComplexKpi = allKpis.stream().filter(kpi ->
            (kpi.getArrKpi() != null && !kpi.getArrKpi().isEmpty())
                || (kpi.getArrGroup() != null && kpi.getArrGroup().split(",").length > 1)
        ).toList();
        ignoreComplexKpi.forEach(kpi -> allClientRequestInputKpiCounters.removeIf(kpiIdx -> kpi.getKpiID() ==  kpiIdx));



        List<KpiFormula> simpleKpis = allKpis.stream().filter(kpi -> !ignoreComplexKpi.contains(kpi)).toList();

        List<Integer> ignoreIfNotShow = new ArrayList<>();

        if(dbType.equalsIgnoreCase(SqlDialect.CLICKHOUSE.name())) {
            lstKpiFormulaWithMultiTable = ignoreComplexKpi;
        }else if(!ignoreComplexKpi.isEmpty()) {
            log.warn("Ingoring complex kPi whcih is not supported yet, current db: {}", dbType);
        }


        simpleKpis.forEach(kpiFormula -> {
            String[] arrGrp = kpiFormula.getArrGroup() != null ? kpiFormula.getArrGroup().split(",") : new String[0];
            String[] arrCounter = kpiFormula.getArrCounter() != null ? kpiFormula.getArrCounter().split(",") : new String[0];

            for (var relatedCounter : arrCounter) {
                allClientRequestInputKpiCounters.add(Integer.parseInt(relatedCounter.trim()));
                if(!inputStatisticDataClone.isShowRecentCounter()) {
                    ignoreIfNotShow.add(Integer.parseInt(relatedCounter.trim()));
                }
            }

            if (arrGrp.length > 0) {
                int groupId = Integer.parseInt(arrGrp[0]);
                lstAllCounterCat.stream()
                    .filter(e -> groupId == e.getCounterCatID())
                    .findAny()
                    .ifPresent(counterCat -> hmGroupKPI.computeIfAbsent(counterCat.getCode(),
                        (key) -> new HashSet<>(Arrays.asList(kpiFormula))).add(kpiFormula));
            }
        });


        // map coutner/kpi vao bang tuong ung
        List<GroupCounterCounter> lstGroupCounterCounters = lstAllGroupCounterCounter.stream()
            .filter(e -> allClientRequestInputKpiCounters.contains(e.getCounterID()))
            .sorted(Comparator.comparing(GroupCounterCounter::getCode))
            .toList();

        lstGroupCounterCounters.forEach(item -> {
            hmGroupCounter.computeIfAbsent(item.getCode(), key -> new HashSet<>(List.of(item.getCounterID())))
                .add(item.getCounterID());
        });


        String listNe = inputStatisticDataClone.getListNE().toString();
        listNe = listNe.substring(1, listNe.length() - 1);

        // TODO: Extra field
        // 1. sbSelect, sbGroupBy, sbOrderBy

        hmExtraField.forEach((key, value) -> {
            if (DATE_TIME_KEY.equalsIgnoreCase(key)) {
                if (SqlDialect.CLICKHOUSE.name().equalsIgnoreCase(dbType)) {
                    // Logic for ClickHouse database
                    sbSelect.append(SPACE_CHAR).append(getDateRoundClickHouse(inputStatisticDataClone.getInterval(),
                        inputStatisticDataClone.getIntervalUnit())).append(COMMA_CHAR);
                    sbOrderBy.append(SPACE_CHAR).append(value.getColumnName()).append(SPACE_CHAR).append("DESC").append(COMMA_CHAR);
                    sbGroupBy.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
                } else {
                    // Logic for other databases
                    sbSelect.append(SPACE_CHAR)
                        .append(getDateRoundMySQL(inputStatisticDataClone.getInterval(),
                            inputStatisticDataClone.getIntervalUnit())) .append(COMMA_CHAR);
                    sbOrderBy.append(SPACE_CHAR).append("report_time").append(SPACE_CHAR).append("DESC") .append(COMMA_CHAR);
                    sbGroupBy.append(SPACE_CHAR).append("report_time").append(COMMA_CHAR);
                }
            } else {
                if(RAT_TYPE_KEY.equalsIgnoreCase(key)) {
//                    sbSelect.append(SPACE_CHAR).append("ifNull(").append(RAT_TYPE_KEY).append(COMMA_CHAR)
//                    .append(SPACE_CHAR).append("'").append(SqlUtil.determineDefaultRatType()).append("'").append(") AS ").append(RAT_TYPE_KEY).append(COMMA_CHAR);

                }else {
                sbSelect.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
                }
                sbOrderBy.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
                sbGroupBy.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
            }
        });

        // sbKeyGroupBy
        hmKeyField.forEach((key, val) -> {
            sbKeyGroupBy.append(SPACE_CHAR).append(val.getColumnName()).append(COMMA_CHAR);
        });
        if(inputStatisticDataClone.getListFieldSummary() != null && !inputStatisticDataClone.getListFieldSummary().isEmpty()) {
            sbSelect.setLength(sbSelect.length() - 1);
            sbGroupBy.setLength(sbGroupBy.length() - 1);
            sbOrderBy.setLength(sbOrderBy.length() - 1);
            sbKeyGroupBy.setLength(sbKeyGroupBy.length() - 1);
        }

        // 2. Filter
        StringBuilder sbExtraFieldFilter = new StringBuilder();
        if (inputStatisticDataClone.getOtherFilters() != null && !inputStatisticDataClone.getOtherFilters().isEmpty()) {
            String otherFilter = OtherFilterUtil.generateExtraFilter(inputStatisticDataClone.getOtherFilters(), hmKeyField);
            sbExtraFieldFilter.append("AND").append(SPACE_CHAR).append("(").append(otherFilter).append(")");
        }

        // minute/duration: convert to second
        String sDuration = calculateDuration(inputStatisticDataClone);

        if (hmGroupCounter.size() >= hmGroupKPI.size()) {
            String finalListNe = listNe;
            hmGroupCounter.forEach((k, v) -> {
                QueryObject queryObject = new QueryObject();

                List<Counter> lstGroupCounter = allCounters.stream()
                    .filter(item -> v.contains(item.getCounterID()))
                    .collect(Collectors.toList());

                // All Counter
                StringBuilder sbAllCounter = new StringBuilder();
                String strAllCounter = "";
                if (SqlDialect.CLICKHOUSE.name().equalsIgnoreCase(dbType)) {
                    lstGroupCounter.forEach(c -> {
                        sbAllCounter.append("AVG(c_").append(c.getCounterID()).append(") AS \"").append("c_")
                            .append(c.getCounterID()).append("\"").append(COMMA_CHAR).append(SPACE_CHAR);
                    });
                    strAllCounter = sbAllCounter.substring(0, sbAllCounter.length() - 2);
                }

                // KPI/Counter can hiện thi ra output
                List<String> lstColumnShow = new ArrayList<>();
                Set<String> column = new HashSet<>();

                if (!inputStatisticDataClone.isShowRecentCounter()) {
                    lstGroupCounter.removeIf(e -> !lstOriginalInputCounterKpi.contains(e.getCounterID()));
                }

                lstGroupCounter.forEach(c -> {
                    lstColumnShow.add("round(" + c.getFormula() + ",2) AS \"" +
                        (inputStatisticDataClone.isReturnCounterKpiId() ? c.getCounterID() : c.getName()) +
                        "\" ");
                        column.add(inputStatisticDataClone.isReturnCounterKpiId() ? String.valueOf(c.getCounterID()) : c.getName());
                });

                Set<KpiFormula> sGroupKpiFormulas = hmGroupKPI.get(k);
                if (sGroupKpiFormulas != null) {
                    sGroupKpiFormulas.forEach(item -> {
                        String formula = item.getFormula();
                        lstColumnShow.add(
                            "round(" + formula + ",2) AS \"" +
                                (inputStatisticDataClone.isReturnCounterKpiId() ? item.getKpiID()
                                    : (item.getKpiName() + "\" ")));
                        column.add(inputStatisticDataClone.isReturnCounterKpiId() ? String.valueOf(item.getKpiID())
                            : (item.getKpiName()));
                    });
                }

                String strSQLOutputCounterKpi = lstColumnShow.toString();
                strSQLOutputCounterKpi = strSQLOutputCounterKpi.substring(1, strSQLOutputCounterKpi.length() - 1);

                // Hour range generate
                StringBuilder timeRangeSQL = new StringBuilder();
                if (inputStatisticDataClone.getRanges() != null) {
                    timeRangeSQL = getStringBuilderTimeRange(inputStatisticDataClone);
                }

                // Build SQL
                try {
                    if (SqlDialect.CLICKHOUSE.name().equalsIgnoreCase(dbType)) {
                        String sqlQuery = createSQLClickHouse(sDuration,
                            fromTime, toTime, finalListNe,
                            sbExtraFieldFilter.toString(),
                            strAllCounter,
                            strSQLOutputCounterKpi,
                            inputStatisticDataClone.getInterval(),
                            inputStatisticDataClone.getIntervalUnit(),
                            k,
                            timeRangeSQL.toString(),
                                sbSelect.toString(),
                            sbGroupBy.toString(),
                            sbOrderBy.toString(),
                            sbKeyGroupBy.toString(), inputStatisticDataClone.getRatTypeList(), inputStatisticDataClone);
                        queryObject.setSql(sqlQuery);
                        queryObject.setColumn(column);
                        lstQueryObjects.add(queryObject);
                    } else {
                         String sqlQuery = createSQLMySQL(sDuration,
                         fromTime, toTime, finalListNe,
                         sbExtraFieldFilter.toString(),
                         strSQLOutputCounterKpi,
                         inputStatisticDataClone.getInterval(),
                         inputStatisticDataClone.getIntervalUnit(),
                         k,
                         timeRangeSQL.toString(),
                                 sbSelect.toString(),
                         sbGroupBy.toString(),
                         sbOrderBy.toString(), inputStatisticDataClone.getRatTypeList(), inputStatisticDataClone);

                         queryObject.setSql(sqlQuery);
                         queryObject.setColumn(column);
                         lstQueryObjects.add(queryObject);
                    }
                } catch (ParseException e) {
                    log.error(e.getMessage());
                    log.error("Format date problem :{}", toTime);
                }
            });
        } else {
            log.error("Number of Group not equal.");
        }

        boolean isExistedKpiFromMultiTbl = !lstKpiFormulaWithMultiTable.isEmpty();

        if (isExistedKpiFromMultiTbl) {
//           handleKpiMutiTable(lstKpiFormulaWithMultiTable, allKpis,allCounters, lstQueryObjects,
//               lstAllGroupCounterCounter, inputStatisticDataClone, hmExtraField, hmKeyField);
        }
        CustomOutput<List<QueryObject>> output = new CustomOutput<>();
        output.setFuncOutput(lstQueryObjects);
        output.setCustomStringOutput("Total SQL Query: " + lstQueryObjects.size());
        return output;
    }

    private String createSQLMySQL(String sDuration, String fromTime, String toTime, String neList, String extraFieldFilter,
                                  String columnShow, int interval, Integer intervalUnit,
                                  String tableName, String timeRange, String sSelect, String sGroupBy, String sOrderBy, List<String> ratTypeList, InputStatisticData inputStatisticDataClone) throws ParseException {
        Date truncToTime = util.getRoundDate(sdf.parse(toTime), interval, intervalUnit);
        StringBuilder sql = new StringBuilder("SELECT");
        sql.append(sSelect);
        if(inputStatisticDataClone!= null && inputStatisticDataClone.getListFieldSummary()!= null && !inputStatisticDataClone.getListFieldSummary().isEmpty()){
            sql.append(COMMA_CHAR);
        }
        sql.append(SPACE_CHAR).append(columnShow).append(DOWN_LINE_CHAR);
        sql.append("FROM ").append(tableName).append(DOWN_LINE_CHAR);
        sql.append("WHERE record_time >= '").append(fromTime).append(" AND record_time < '").append(sdf.format(truncToTime));
        sql.append("' ").append(timeRange).append(sDuration).append(" AND ne_id IN (").append(neList).append(")");
        //add rat_type for mix-mode
        if(DataUtil.isNotNullNotEmpty(ratTypeList)) {
            List<String> ratTypeListWithQuote = ratTypeList.stream().map(ratType -> "'" + ratType + "'").collect(Collectors.toList());
            String ratTypeStr = String.join(",", ratTypeListWithQuote);
            sql.append(" AND rat_type IN ( ").append(ratTypeStr).append(" )");
        }
        sql.append(SPACE_CHAR).append(extraFieldFilter).append(DOWN_LINE_CHAR);
        if(inputStatisticDataClone!= null && inputStatisticDataClone.getListFieldSummary()!= null && !inputStatisticDataClone.getListFieldSummary().isEmpty()) {
            sql.append("GROUP BY").append(sGroupBy).append(DOWN_LINE_CHAR);
            sql.append("ORDER BY").append(sOrderBy);
        }
        sql.append(SEMI_COLON_CHAR);
        return sql.toString();
    }

//    private void handleKpiMutiTable(List<KpiFormula> lstKpiFormulasWithMultiTable, List<KpiFormula> allKpis,
//                                    List<Counter> allCounters,
//                                    List<QueryObject> lstQueryObjects, List<GroupCounterCounter> lstAllGroupCounterCounter,
//                                    InputStatisticData inputStatisticData,
//                                    LinkedHashMap<String, ExtraFieldObject> hmExtraField, HashMap<String, ExtraFieldObject> hmKeyField) {
//
//        Set<Integer> allInputCounterIds = allCounters.stream()
//            .map(Counter::getCounterID)
//            .collect(Collectors.toSet());
//
//        List<GroupCounterCounter> filterGroupGroupCounters = lstAllGroupCounterCounter.stream()
//            .filter(g -> allInputCounterIds.contains(g.getCounterID())).toList();
//
//        List<String> keyForJoinConditionSet = new ArrayList<>();
//        hmKeyField.forEach((k,v) -> keyForJoinConditionSet.add(v.getColumnName()));
//
//        for(var kpi : lstKpiFormulasWithMultiTable) {
//            Set<Integer> arrCounters = new HashSet<>();
//            Set<KpiFormula> arrKpi = new HashSet<>();
//            arrKpi.add(kpi);
//            if(kpi.getArrKpi()!= null && !kpi.getArrKpi().trim().isEmpty()) {
//                Set<Integer> allRelatedIds = new HashSet<>();
//                util.collectAllKpiAndCounters(kpi, allRelatedIds, new HashSet<>(), allKpis);
//                arrCounters.addAll(
//                    allRelatedIds.stream().filter(allInputCounterIds::contains)
//                        .collect(Collectors.toSet())
//                );
//                arrKpi.addAll(
//                    allKpis.stream().filter(inputKpi -> allRelatedIds.contains(inputKpi.getKpiID()))
//                        .collect(Collectors.toSet())
//                );
//            }else if( kpi.getArrCounter() != null && !kpi.getArrCounter().trim().isEmpty()) {
//                arrCounters.addAll(Stream.of(kpi.getArrCounter().split(",")).map(Integer::parseInt).collect(Collectors.toSet()));
//            }
//
//
//
//            log.debug("All Counters ID from KPI: {}", arrCounters);
//            log.debug("All KPI ID from KPI: {}", arrKpi);
//            final Set<Integer> finalArrCounters = new HashSet<>(arrCounters);
//
//            List<GroupCounterCounter> groupCounterCounters = filterGroupGroupCounters.stream()
//                .filter(g -> finalArrCounters.contains(g.getCounterID())).toList();
//
//            Map<String, Set<Integer>> tblAndTheirColumnMap = groupCounterCounters.stream()
//                .collect(Collectors.groupingBy(GroupCounterCounter::getCode,
//                    Collectors.mapping(GroupCounterCounter::getCounterID, Collectors.toSet())));
//
//            log.debug("tblAndTheirColumnMap: {}", tblAndTheirColumnMap);
//
//            StringBuilder sRelatedCounters = new StringBuilder(" ");
//            if(inputStatisticData.isShowRecentCounter()) {
//
//                List<Counter> objCounters = allCounters.stream().filter(c -> arrCounters.contains(c.getCounterID())).toList();
//
//                objCounters.forEach(c -> sRelatedCounters.append("round(").append(c.getFormula())
//                    .append(COMMA_CHAR).append(SPACE_CHAR).append("2) AS \"").append(inputStatisticData.isReturnCounterKpiId()
//                        ? c .getCounterID() : c.getName()).append("\"").append(COMMA_CHAR)
//                );
//            }
//
//            List<String> tblAliases = generateTblAliases(tblAndTheirColumnMap.keySet());
//
//            StringBuilder sbSelect = new StringBuilder("SELECT");
//            StringBuilder sbGroupBy = new StringBuilder();
//            StringBuilder sbOrderBy = new StringBuilder();
//            StringBuilder sbKeyGroupBy = new StringBuilder();
//
//            QueryObject queryObject = new QueryObject();
//            Set<String> column = new HashSet<>();
//
//            column.add(kpi.getKpiName());
//
//            String listNe = inputStatisticData.getListNE().toString();
//            listNe = listNe.substring(1, listNe.length() - 1);
//
//            hmExtraField.forEach((key,value) -> {
//                if(DATE_TIME_KEY.equalsIgnoreCase(key)) {
//                    sbSelect.append(SPACE_CHAR)
//                        .append(getDateRoundClickHouseForMultiTbl(inputStatisticData.getInterval(), inputStatisticData.getIntervalUnit(), tblAliases))
//                        .append(COMMA_CHAR);
//                    sbOrderBy.append(SPACE_CHAR).append(value.getColumnName()).append(SPACE_CHAR).append("DESC").append(COMMA_CHAR);
//                    sbGroupBy.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
//                }else {
//                    String nonNullSelectedFieldColumn = selectNonNullFieldColumn(tblAliases, value);
//                    sbSelect.append(SPACE_CHAR).append(nonNullSelectedFieldColumn).append(COMMA_CHAR);
//                    sbOrderBy.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
//                    sbGroupBy.append(SPACE_CHAR).append(value.getColumnName()).append(COMMA_CHAR);
//                }
//            });
//
//
//            sbSelect.append(DOWN_LINE_CHAR).append(sRelatedCounters);
//
//            arrKpi.forEach(relatedKpi -> {
//                String kpiFormula = relatedKpi.getFormula();
//                sbSelect.append(DOWN_LINE_CHAR).append("round(").append(kpiFormula).append(",2) AS \"")
//                    .append(relatedKpi.getKpiName()).append("\" ").append(COMMA_CHAR);
//
//                column.add(relatedKpi.getKpiName());
//            });
//
//            hmKeyField.forEach((key,val) -> sbKeyGroupBy.append(SPACE_CHAR).append(val.getColumnName()).append(COMMA_CHAR));
//
//            removeLastComma(sbSelect, sbGroupBy, sbOrderBy, sbKeyGroupBy);
//
//            StringBuilder sbExtraFieldFilter = new StringBuilder();
//            if(inputStatisticData.getOtherFilters() != null && !inputStatisticData.getOtherFilters().isEmpty()) {
//                String otherFilter = OtherFilterUtil.generateExtraFilter(inputStatisticData.getOtherFilters(), hmKeyField);
//                sbExtraFieldFilter.append("AND").append(SPACE_CHAR).append("(").append(otherFilter).append(")");
//            }
//
//            String sDuration = calculateDuration(inputStatisticData);
//
//            final String fromTime = inputStatisticData.getFromTime();
//            final String toTime = isGTnow(inputStatisticData.getToTime())
//                ? inputStatisticData.getToTime()
//                : sdf.format(Calendar.getInstance().getTime());
//
//            String finalListNe = listNe;
//            String finalDuration = sDuration;
//            boolean isFirstTable = true;
//            int count = 0;
//            int size = tblAndTheirColumnMap.size();
//
//            // offset: indicating the current table which is being traversed
//            int offset = 0;
//
//            for(var entry: tblAndTheirColumnMap.entrySet()) {
//                count++;
//                String joinTbl = entry.getKey();
//                Set<Integer> counterIds = entry.getValue();
//
//                List<Counter> lstGroupCounter = allCounters.stream()
//                    .filter(item -> counterIds.contains(item.getCounterID()))
//                    .toList();
//
//                StringBuilder sbAllCounter = new StringBuilder();
//                String strAllCounter = "";
//
//                lstGroupCounter.forEach(c -> {
//                    sbAllCounter.append("AVG(c_").append(c.getCounterID()).append(") AS \"").append("c_")
//                        .append(c.getCounterID()).append("\"").append(COMMA_CHAR).append(SPACE_CHAR);
//                });
//
//                strAllCounter = sbAllCounter.substring(0, sbAllCounter.length() - 2);
//
//                if(!inputStatisticData.isShowRecentCounter()) {
//                    lstGroupCounter = new ArrayList<>();
//                }
//
//
//                lstGroupCounter.forEach(c -> column.add(inputStatisticData.isReturnCounterKpiId() ? String.valueOf(c.getCounterID()) : c.getName()));
//
//                StringBuilder timeRangeSQL = new StringBuilder();
//                if(inputStatisticData.getRanges() != null) {
//                    timeRangeSQL = getStringBuilderTimeRange(inputStatisticData);
//                }
//
//                offset++;
//
//                try {
//                    // only support clickhouse for now
//                    String sqlQuery = createSubquerySQLClickhouseForMultiTable(isFirstTable,count ==size,
//                        finalDuration, fromTime, toTime, finalListNe,
//                        sbExtraFieldFilter.toString(),
//                        strAllCounter,
//                        inputStatisticData.getInterval(),
//                        inputStatisticData.getIntervalUnit(),
//                        joinTbl,
//                        timeRangeSQL.toString(),
//                        sbKeyGroupBy.toString(),
//                        keyForJoinConditionSet,
//                        offset,
//                        tblAliases,
//                        inputStatisticData.getRatTypeList()
//                        );
//
//                    sbSelect.append(sqlQuery);
//                }catch (ParseException e) {
//                    log.error("ParseException: {}", e.getMessage());
//                    log.error("Format date problem :{}", toTime);
//                }
//                isFirstTable = false;
//            }
//            if(inputStatisticData.getListFieldSummary() != null && !inputStatisticData.getListFieldSummary().isEmpty()) {
//                sbSelect.append("GROUP BY ").append(sbGroupBy).append(DOWN_LINE_CHAR);
//                sbSelect.append("ORDER BY").append(sbOrderBy).append(SEMI_COLON_CHAR);
//
//            }
//
//            queryObject.setSql(sbSelect.toString());
//            queryObject.setColumn(column);
//            lstQueryObjects.add(queryObject);
//        }
//
//    }

    private String selectNonNullFieldColumn(List<String> tblAliases, ExtraFieldObject value) {
       StringBuilder sb = new StringBuilder("coalesce(");
       boolean isLastTbl = false;
       for(int i = 0;i < tblAliases.size();i++) {
           isLastTbl = i == tblAliases.size() -1;
           final String oneStepOutside = value.getOneStepOutside();
           sb.append("nullif(").append(tblAliases.get(i)).append(".").append(value.getColumnName()).append(COMMA_CHAR)
               .append(SPACE_CHAR).append(oneStepOutside).append(")");
           if(!isLastTbl) {
               sb.append(COMMA_CHAR).append(SPACE_CHAR);
           }else {
               sb.append(") AS ").append(value.getColumnName());
           }

       }
       return sb.toString();
    }

    private List<String> generateTblAliases(Set<String> tableNames) {
        List<String> aliases = new ArrayList<>();
        for(var tblName : tableNames) {
            String alias = DBUtils.getFirstLettersTbl(tblName);
            aliases.add(alias);
        }
        return aliases;
    }

    private void removeLastComma(StringBuilder... builders) {
        for (StringBuilder sb : builders) {
            if (!sb.isEmpty() && sb.charAt(sb.length() - 1) == COMMA_CHAR) {
                sb.setLength(sb.length() - 1);
            }
        }
    }

    private Set<Integer> extractCounterIdsFromKpi(KpiFormula kpi) {

        Set<Integer> counterIds = new HashSet<>();
        if (kpi.getArrCounter() != null && !kpi.getArrCounter().trim().isEmpty()) {
            String[] arrCounterIds = kpi.getArrCounter().split(",");
            for (String counterIdStr : arrCounterIds) {
                try {
                    Integer counterId = Integer.parseInt(counterIdStr.trim());
                    counterIds.add(counterId);
                } catch (NumberFormatException e) {
                    log.error("Invalid counter ID format: {} in KPI {}", counterIdStr, kpi.getKpiID());
                }
            }
        }
        return counterIds;
    }

    private String buildRelatedCountersString(List<GroupCounterCounter> groupCounterCounters, InputStatisticData inputStatisticData) {
        StringBuilder sb = new StringBuilder();
        groupCounterCounters.forEach(g -> {
            sb.append("round(SUM(c_").append(g.getCounterID()).append("),2) AS \"").append(g.getName()).append("\" ").append(COMMA_CHAR);
        });
        return sb.substring(0, sb.length() - 2);
    }


    private String calculateDuration(InputStatisticData inputStatisticData) {
        if (inputStatisticData.getDuration() <= 0) {
            return "";
        }

        Integer multiplier = DURATION_MULTIPLIERS.get(inputStatisticData.getIntervalUnit());
        if (multiplier == null) {
            return "";
        }

        return " AND duration = " + (inputStatisticData.getDuration() * multiplier);
    }

    private String createSubquerySQLClickhouseForMultiTable(boolean isFirstTable, boolean isLastTable,
                                                            String sDuration,
                                                            String fromTime, String toTime, String neList,
                                                            String extraFieldFilter, String allCounter, int interval,
                                                            Integer intervalUnit, String tableName, String timeRange,
                                                            String sKeyGroupBy,
                                                           List<String> keyForJoinConditionSet, Integer offset, List<String> tblAliases,
                                                           List<String> ratTypeList)
        throws ParseException {
        Date truncToTime = util.getRoundDate(sdf.parse(toTime), interval, intervalUnit);
        StringBuilder sql = new StringBuilder();
        String joinedKey = buildJoinedKey(keyForJoinConditionSet);
        if(isFirstTable) {
            sql.append(DOWN_LINE_CHAR).append("FROM (").append(DOWN_LINE_CHAR);
        }
        sql.append("SELECT ");
        sql.append(joinedKey).append(COMMA_CHAR);
        sql.append(DOWN_LINE_CHAR).append(sKeyGroupBy);
        sql.append(COMMA_CHAR).append(SPACE_CHAR).append(allCounter).append(DOWN_LINE_CHAR);

        sql.append("FROM ").append(tableName).append(DOWN_LINE_CHAR);

        sql.append("WHERE record_time >= '").append(fromTime).append("' AND record_time < '").append(sdf.format(truncToTime));
        sql.append("' ").append(timeRange).append(sDuration).append(" AND ne_id IN (").append(neList).append(")");

        //add rat_type for mix-mode
        if(DataUtil.isNotNullNotEmpty(ratTypeList)) {
            List<String> ratTypeListWithQuote = ratTypeList.stream().map(ratType -> "'" + ratType + "'").collect(Collectors.toList());
            String ratTypeStr = String.join(",", ratTypeListWithQuote);
            sql.append(" AND rat_type IN ( ").append(ratTypeStr).append(" )");
        }

        sql.append(SPACE_CHAR).append(extraFieldFilter).append(DOWN_LINE_CHAR);
        sql.append("GROUP BY ").append(sKeyGroupBy).append(DOWN_LINE_CHAR);

        String tblAlias = DBUtils.getFirstLettersTbl(tableName);
        sql.append(")").append(" AS ").append(tblAlias).append(DOWN_LINE_CHAR);

        if(!isFirstTable) {
            String joinCondition = buildConditionJoinTblV2(offset, tblAliases);
            sql.append(joinCondition).append(DOWN_LINE_CHAR);
        }
        sql.append(!isLastTable ? " FULL JOIN (" : "");
        return sql.toString();
    }



    private String buildConditionJoinTblV2(Integer offset, List<String> tblAliases) {
        StringBuilder sql = new StringBuilder();
        if(offset ==2) {
            String firstTbl = tblAliases.get(0);
            String secondTbl = tblAliases.get(1);
            sql.append("ON ").append(firstTbl).append(".").append(JOINED_KEY).append(" = ").append(secondTbl).append(".").append(JOINED_KEY);
        }else {
            sql.append("ON coalesce(");
            for(int i = 0;i < tblAliases.size();i++) {
                String tblAlias = tblAliases.get(i);
                if(i < offset -1) {
                    sql.append("nullIf(").append(tblAlias).append(".").append(JOINED_KEY).append(",").append("'").append("'").append(")");
                    if(i < offset -2) {
                        sql.append(COMMA_CHAR);
                    }
                    sql.append(SPACE_CHAR);
                }else {
                    sql.append(") = ");
                    sql.append(tblAlias).append(".").append(JOINED_KEY);
                }
            }
        }
        return sql.toString();
    }

    private String buildJoinedKey(List<String> keyForJoinConditionSet) {
        StringBuilder joinedKeySb = new StringBuilder("concat(");
        boolean isLast = false;
        final char CHAR_NULL_IF = '-';
        final char CHAR_KEY_DELIMITER = '_';

        for(int i = 0;i < keyForJoinConditionSet.size();i++) {
            isLast = i == keyForJoinConditionSet.size() - 1;
            String key = keyForJoinConditionSet.get(i);
            // in clickhouse TOSTRING() is invalid
            joinedKeySb.append("toString(coalesce(").append(key).append(",'").append(CHAR_NULL_IF).append("'").append("))");
            if(!isLast) {
                joinedKeySb.append(COMMA_CHAR).append("'").append(CHAR_KEY_DELIMITER).append("'").append(", ");
            }
            if(isLast) {
                joinedKeySb.append(") AS ").append(JOINED_KEY);
            }
        }
        return joinedKeySb.toString();
    }


    @Deprecated
    private String buildConditionJoinTbl(String firstLettersTbl) {
        if (firstLettersTbl == null || firstLettersTbl.trim().isEmpty()) {
            throw new IllegalArgumentException("Table alias cannot be null or empty");
        }

        final String[] joinColumns = {
            "record_time",
            "ne_id",
            "cell_name",
            "location",
            "cell_index"
        };

        StringBuilder sql = new StringBuilder();

        for (int i = 0; i < joinColumns.length; i++) {
            String column = joinColumns[i];

            if (i == 0) {
                sql.append("ON ");
            } else {
                sql.append("AND ");
            }

            // Build join condition: base.column = alias.column
            sql.append(PREFIX_JOIN_BASE_TABLE)
                .append(".")
                .append(column)
                .append(" = ")
                .append(firstLettersTbl)
                .append(".")
                .append(column)
                .append(DOWN_LINE_CHAR);
        }
        return sql.toString();
    }




    private StringBuilder getStringBuilderTimeRange(InputStatisticData inputStatisticData) {
        StringBuilder timeRangeSQL = new StringBuilder();
        List<TimeRange> ranges = inputStatisticData.getRanges();
        if (ranges != null && !ranges.isEmpty()) {
            timeRangeSQL.append("AND (");
            for (int idx = 0; idx < ranges.size(); idx++) {
                TimeRange timeRange = ranges.get(idx);
                long hourStart = Long.parseLong(timeRange.getStart().split(":")[0]) * 60 * 60 * 1000;
                long minStart = Long.parseLong(timeRange.getStart().split(":")[1]) * 60 * 1000;
                long hourEnd = Long.parseLong(timeRange.getEnd().split(":")[0]) * 60 * 60 * 1000;
                long minEnd = Long.parseLong(timeRange.getEnd().split(":")[1]) * 60 * 1000;

                if (idx != 0) {
                    timeRangeSQL.append("OR ");
                }

                timeRangeSQL.append(
                        "(EXTRACT(hour FROM record_time)*60*60*1000 + EXTRACT(minute FROM record_time)*60*1000 + " +
                            "EXTRACT(second FROM record_time)*1000 >= ")
                    .append(hourStart + minStart)
                    .append(" AND ")
                    .append("(EXTRACT(hour FROM record_time)*60*60*1000 + EXTRACT(minute FROM record_time)*60*1000 + "
                        +
                        "EXTRACT(second FROM record_time)*1000 < ")
                    .append(hourEnd + minEnd)
                    .append(")");
            }
            timeRangeSQL.append(")");
        }
        return timeRangeSQL;
    }


    private String createSQLClickHouse(
            String sDuration,
            String fromTime,
            String toTime,
            String neList,
            String extraFieldFilter,
            String allCounter,
            String columnShow,
            int interval,
            Integer intervalUnit,
            String tableName,
            String timeRange,
            String sSelect,
            String sGroupBy,
            String sOrderBy,
            String sKeyGroupBy, List<String> ratTypeList, InputStatisticData inputStatisticDataClone) throws ParseException {

        Date truncToTime = util.getRoundDate(sdf.parse(toTime), interval, intervalUnit);
        StringBuilder sql = new StringBuilder("SELECT");
        sql.append(sSelect);
        if(inputStatisticDataClone!= null && inputStatisticDataClone.getListFieldSummary()!= null && !inputStatisticDataClone.getListFieldSummary().isEmpty()){
            sql.append(COMMA_CHAR);
            }
                sql.append(SPACE_CHAR).append(columnShow).append(DOWN_LINE_CHAR);
        sql.append("FROM (");
        sql.append("SELECT").append(SPACE_CHAR).append(sKeyGroupBy);
        sql.append(COMMA_CHAR).append(SPACE_CHAR).append(allCounter).append(DOWN_LINE_CHAR);

        sql.append("FROM ").append(tableName).append(DOWN_LINE_CHAR);

        sql.append("WHERE record_time >= '").append(fromTime).append("' AND record_time < '").append(sdf.format(truncToTime));
        sql.append("' ").append(timeRange).append(sDuration).append(" AND ne_id IN (").append(neList).append(")");

        //add rat_type for mix-mode
        if(DataUtil.isNotNullNotEmpty(ratTypeList)) {
            List<String> ratTypeListWithQuote = ratTypeList.stream().map(ratType -> "'" + ratType + "'").collect(Collectors.toList());
            String ratTypeStr = String.join(",", ratTypeListWithQuote);
            sql.append(" AND rat_type IN ( ").append(ratTypeStr).append(" )");
        }

        sql.append(SPACE_CHAR).append(extraFieldFilter).append(DOWN_LINE_CHAR);
        sql.append("GROUP BY ").append(sKeyGroupBy).append(DOWN_LINE_CHAR);

        sql.append(")").append(DOWN_LINE_CHAR);
        if(inputStatisticDataClone!= null && inputStatisticDataClone.getListFieldSummary() != null) {

            sql.append("GROUP BY").append(sGroupBy).append(DOWN_LINE_CHAR);
            sql.append("ORDER BY ").append(sOrderBy).append(DOWN_LINE_CHAR);
        }
        return sql.toString();
    }

    private String getDateRoundMySQL(int interval, Integer intervalUnit) {
        if (intervalUnit.equals(IntervalUnit.MINUTE.getValue())) {
            if (interval == 5 || interval == 15 || interval == 30 || interval == 60) {
                return "DATE_ADD(DATE_FORMAT(record_time,'%Y-%m-%d %H:00:00'), INTERVAL FLOOR(minute(record_time)/("
                    + interval + ")) *" + interval + " minute) AS report_time";
            } else if (interval == 1440) {
                return "DATE_FORMAT(record_time,'%Y-%m-%d 00:00:00') AS report_time";
            }
        } else if (intervalUnit.equals(IntervalUnit.SECOND.getValue())) {
            return "DATE_ADD(DATE_FORMAT(record_time,'%Y-%m-%d %H:%i:00'), INTERVAL FLOOR(second(record_time)/("
                + interval + ")) *" + interval + " second) AS report_time";
        } else if (intervalUnit.equals(IntervalUnit.HOUR.getValue())) {
            return "DATE_ADD(DATE_FORMAT(record_time,'%Y-%m-%d 00:00:00'), INTERVAL FLOOR(hour(record_time)/("
                + interval + ")) *" + interval + " hour) AS report_time";
        } else if (intervalUnit.equals(IntervalUnit.DATE.getValue())) {
            return "DATE_FORMAT(record_time,'%Y-%m-%d 00:00:00') AS report_time";
        } else if (intervalUnit.equals(IntervalUnit.WEEK.getValue())) {
            return "DATE_ADD(DATE_FORMAT(record_time,'%Y-%m-01 00:00:00'), INTERVAL FLOOR(week(record_time)/("
                + interval + ")) *" + interval + " week) AS report_time";
        } else if (intervalUnit.equals(IntervalUnit.MONTH.getValue())) {
            return "DATE_ADD(DATE_FORMAT(record_time,'%Y-01-00 00:00:00'), INTERVAL FLOOR(month(record_time)/("
                + interval + ")) *" + interval + " month) AS report_time";
        }

        // return "record_time AS report_time";
        return "report_time";
    }

    private StringBuilder getDateRoundClickHouseCommon(int interval, Integer intervalUnit) {
        StringBuilder recordTimeString = new StringBuilder("formatDateTime(");
        if(intervalUnit.equals(IntervalUnit.MINUTE.getValue())) {
            if (interval == 5 || interval == 15 || interval == 30 || interval == 60)  {
                recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL").append(interval).append(" minute");

            }else if(interval == 1440) {
                recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL 1 DAY ))");
            }
        }else if(intervalUnit.equals(IntervalUnit.SECOND.getValue())) {
            recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL").append(interval).append(" SECOND))");
        }else if(intervalUnit.equals(IntervalUnit.HOUR.getValue())) {
            recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL").append(interval).append(" HOUR))");
        }else if(intervalUnit.equals(IntervalUnit.DATE.getValue())) {
            recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL").append(interval).append(" DAY))");
        }else if(intervalUnit.equals(IntervalUnit.WEEK.getValue())) {
            recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL").append(interval).append(" WEEK))");
        }else if(intervalUnit.equals(IntervalUnit.MONTH.getValue())) {
            recordTimeString.append("toDateTime(toStartOfInterval(").append("record_time, INTERVAL").append(interval).append(" MONTH))");
        }else {
            recordTimeString.append("record_time");
        }
        return recordTimeString;  // khong thieu dau ngaoc dong ")", boi vi no se dung build tiep voi 2 ham dung no
    }



    private String getDateRoundClickHouse(int interval, Integer intervalUnit) {
        return getDateRoundClickHouseCommon(interval, intervalUnit).append(", '%Y-%m-%d %H:%M:%S') AS record_time").toString();
    }

    private String getDateRoundClickHouseForMultiTbl(int interval, Integer intervalUnit, List<String> tblAliases) {
        StringBuilder recordTimeString = getDateRoundClickHouseCommon(interval, intervalUnit);
        // replace with coalesce
        StringBuilder nonNullColumnSb = new StringBuilder("coalesce(");
        for(int i = 0;i < tblAliases.size();i++) {
            String alias = tblAliases.get(i);
            nonNullColumnSb.append("nullIf(").append(alias).append(".record_time, toDateTime(0))");
            if(i < tblAliases.size() - 1) {
                nonNullColumnSb.append(COMMA_CHAR).append(DOWN_LINE_CHAR);
            }else {
                nonNullColumnSb.append(")").append(DOWN_LINE_CHAR);
            }
        }
        String recordTime = recordTimeString.toString().replace("record_time", nonNullColumnSb.toString());
        return recordTime + ", '%Y-%m-%d %H:%M:%S') AS record_time";
    }


}
