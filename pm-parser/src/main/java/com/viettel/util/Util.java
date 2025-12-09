package com.viettel.util;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.google.gson.Gson;
import com.viettel.config.ConfigManager;
import com.viettel.config.RoutingContextExecutor;
import com.viettel.config.SystemType;
import com.viettel.config.TenantContextHolder;
import com.viettel.dal.*;
import com.viettel.exception.KpiCycleException;
import com.viettel.troubleshoot.DatasourceVerifier;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import com.viettel.repository.CommonRepository;
import com.viettel.repository.CounterKpiRepository;
import com.viettel.repository.GroupCounterCounterRepository;
import com.viettel.repository.KpiCachingRepository;
import com.viettel.common.*;

import static com.viettel.common.Constant.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class Util {

    private final int MAX_THREAD_POOL = 10;
    private final int FUTURE_TIMEOUT = 120; // seconds
    private static final char SPACE_CHAR = ' ';
    private final ExecutorService sharedExecutor = Executors.newFixedThreadPool(MAX_THREAD_POOL);


    private final ConfigManager configManager;
    private final CounterKpiRepository counterKpiRepository;
    private final GroupCounterCounterRepository groupCounterCounterRepository;
    private final KpiCachingRepository kpiCachingRepository;
    private final CommonRepository commonRepository;
    private final ScriptEngineManager mgr = new ScriptEngineManager();
    private final ScriptEngine scriptEngine  = mgr.getEngineByName("js");
    private final RoutingContextExecutor routingContext;
    private final JdbcTemplate jdbcTemplate;
    private final DatasourceVerifier datasourceVerifier;


    public Date getRoundDate(Date oriDate, int interval, Integer intervalUnit) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(oriDate);
        Date newDate;

        if (intervalUnit.equals(Constant.IntervalUnit.SECOND.getValue())) {
            newDate = new Date(DateUtils.truncate(calendar.getTime(), Calendar.MINUTE).getTime() +
                    (interval * (calendar.get(Calendar.SECOND) / interval * 1000)));
        } else if (intervalUnit.equals(Constant.IntervalUnit.MINUTE.getValue())) {
            if (interval < 60) {
                newDate = new Date(DateUtils.truncate(calendar.getTime(), Calendar.HOUR).getTime() +
                        (interval * (calendar.get(Calendar.MINUTE) / interval * 60 * 1000)));
            } else if (interval < 1440) {
                newDate = DateUtils.truncate(calendar.getTime(), Calendar.HOUR);
            } else {
                newDate = DateUtils.truncate(calendar.getTime(), Calendar.DATE);
            }
        } else if (intervalUnit.equals(Constant.IntervalUnit.HOUR.getValue())) {
            newDate = DateUtils.truncate(calendar.getTime(), Calendar.HOUR_OF_DAY);
        } else {
            newDate = DateUtils.truncate(calendar.getTime(), Calendar.DATE);
        }

        return newDate;
    }


    @SuppressWarnings("unchecked")
    public List<ReportStatisticKPI> getStatisticFromDatabase(InputStatisticData inputData, boolean isUsingCaching) {
        Performance performance = Performance.of("PM", "PMAPI", new Gson().toJson(inputData));
        InputStatisticData inputStatisticDataClone = inputData.clone();
        // caching null
        List<ReportStatisticKPI> result = new ArrayList<>();
        BetterSQLParser sqlParser = new BetterSQLParser(configManager,counterKpiRepository, groupCounterCounterRepository, this,
                kpiCachingRepository, commonRepository);

        // TODO: Load data from database
        Map<Integer, NeLiteInfo> lstNE = commonRepository.getListNeNameByIds(inputStatisticDataClone.getListNE(),
                inputStatisticDataClone.getListNEGroupCode(), inputStatisticDataClone.getNEGroupConditionParam(),
                inputStatisticDataClone.getNEGroupConditionParamList());

        if (inputStatisticDataClone.getListNE() == null) {
            // Trường hợp không truyền list NE trong body thì phải set lại
            List<Integer> lstNEIdInput = new ArrayList<>();
            lstNE.forEach((key, value) -> {
                lstNEIdInput.add(key);
            });
            inputStatisticDataClone.setListNE(lstNEIdInput);
        }

        List<Counter> lstAllCounter = counterKpiRepository.getAllListCounter();
        List<KpiFormula> lstAllKPI = isUsingCaching ? counterKpiRepository.getAllListKpiFomularCaching()
                : counterKpiRepository.getAllListKpiFomular();
        List<CounterCat> lstAllCounterCat = counterKpiRepository.getAllListCounterCat();
        List<GroupCounterCounter> lstAllGroupCounterCounter = groupCounterCounterRepository.getListGroupCounterCat();

        // TODO:
        List<String> lstFieldSummary = inputStatisticDataClone.getListFieldSummary(); // client can lay nhung truong nao
        String objectLevelName = inputStatisticDataClone.getObjectLevel();
        HashMap<String, ExtraFieldObject> hmAllCommonField;
        LinkedHashMap<String, ExtraFieldObject> hmCommonField = new LinkedHashMap<>();
        HashMap<String, ExtraFieldObject> hmKeyCommonField = new HashMap<>();

        if (objectLevelName == null || objectLevelName.isEmpty()) {
            hmAllCommonField = commonRepository.getExtraField();
        } else {
            hmAllCommonField = commonRepository.getExtraField(objectLevelName);
        }

        String configuredDbType = configManager.getTheCurrentSystemConfigDB();

        if (SqlDialect.CLICKHOUSE.name().equalsIgnoreCase(configuredDbType)) {
            hmAllCommonField.put(DATE_TIME_KEY, new ExtraFieldObject(
                    1,
                    DATE_TIME_KEY,
                    Constant.DATE_TIME_VALUE_CLICKHOUSE,
                    Constant.DATE_TIME_DISPLAY,
                    "String",
                    1,
                    0,
                    1));
        } else {
            hmAllCommonField.put(DATE_TIME_KEY, new ExtraFieldObject(
                    1,
                    DATE_TIME_KEY,
                    Constant.DATE_TIME_VALUE,
                    Constant.DATE_TIME_DISPLAY,
                    "String",
                    1,
                    0,
                    1));
        }

        hmAllCommonField.put(Constant.NE_KEY, new ExtraFieldObject(
                2,
                Constant.NE_KEY,
                Constant.NE_VALUE,
                Constant.NE_DISPLAY,
                "String",
                1,
                0,
                1, "0"));

        // TODO:
        if (lstFieldSummary != null && !lstFieldSummary.isEmpty()) {
            for (var fieldSummary : lstFieldSummary) {
                if (hmAllCommonField.containsKey(fieldSummary)) {
                    ExtraFieldObject extraFieldObject = hmAllCommonField.get(fieldSummary);
                    hmCommonField.put(fieldSummary, extraFieldObject);
                }
            }
        } else {
            // hmCommonField.putAll(hmAllCommonField);
        }

        if (hmCommonField.isEmpty()) {
            log.info("hmCommonField is empty!!!");
            // return result;
        }

        // TODO:
        hmAllCommonField.forEach((k, v) -> {
            if (v.getKey() == 1) {
                hmKeyCommonField.put(k, v);
            }
        });

        // TODO: Get name of kpi and counter => header
        List<Integer> counterKpiIdsFromInput = inputStatisticDataClone.getListCounterKPI() != null ? inputStatisticDataClone.getListCounterKPI() : new ArrayList<>();

        Set<String> sAllKpiCounter = lstAllCounter.stream()
                .filter(c -> counterKpiIdsFromInput.contains(c.getCounterID()))
                .map(Counter::getName)
                .collect(Collectors.toSet());

        sAllKpiCounter.addAll(lstAllKPI.stream()
                .filter(kpi -> counterKpiIdsFromInput.contains(kpi.getKpiID()))
                .flatMap(e -> Stream.of(e.getKpiName() + " (" + e.getUnits() + ")"))
                .collect(Collectors.toSet()));

        // TODO: Get kpi info
        List<KpiFormula> lstKpiFormulas = lstAllKPI.stream()
                .filter(kpi -> counterKpiIdsFromInput.contains(kpi.getKpiID()))
                .toList();


        Set<Integer> flattenAllInputKpiCounters = flatAllCounterKpi(lstKpiFormulas, lstAllKPI, inputStatisticDataClone.getListCounterKPI());  // include sub kpi
        List<KpiFormula> allInputKpiObj = lstAllKPI.stream()
        .filter(kpi -> flattenAllInputKpiCounters.contains(kpi.getKpiID())).collect(Collectors.toList());

        List<Counter> allInputCounterObj = lstAllCounter.stream()
        .filter(counter -> flattenAllInputKpiCounters.contains(counter.getCounterID())).collect(Collectors.toList());


        AllInputKpiCounterObjFlatten allInputKpiCounterObjFlatten = AllInputKpiCounterObjFlatten.builder()
            .allKpiObj(allInputKpiObj)
            .allCounterObj(allInputCounterObj)
            .allKpiCounterId(flattenAllInputKpiCounters)
            .build();


        Map<String, KpiFormula> allInputKpiObjMap = allInputKpiObj.stream()
            .collect(Collectors.toMap(
                KpiFormula::getKpiName,
                item -> item
            ));



        // TODO: Call function to generate sql
        CustomOutput<List<QueryObject>> sqlList = performance.trace(() -> {
            try {
                return isUsingCaching ?
                sqlParser.generateSQLCaching(
                    inputStatisticDataClone, lstAllCounter, lstAllKPI, lstAllCounterCat,
                    hmCommonField
                )
                 :
                 sqlParser.generateSQL(
                        inputStatisticDataClone,
                        allInputKpiCounterObjFlatten,
                        lstAllCounterCat,
                        lstAllGroupCounterCounter,
                        hmCommonField,
                        hmKeyCommonField,
                        configuredDbType
                );
            } catch (ParseException e) {
                log.error("generateSQL error: {}", e.getMessage());
            }
            return new CustomOutput<>();
        }, "{}", "PMBuildSql");

        SqlLogger.logSqlList(sqlList.getFuncOutput().stream().map(QueryObject::getSql).collect(Collectors.toList()));


        Set<String> lstColumnName = new HashSet<>();
         if (!sqlList.getFuncOutput().isEmpty()) {
             CompletionService<List<ReportStatisticKPI>> compService = new ExecutorCompletionService<>(sharedExecutor);

             SystemType submitSystem = TenantContextHolder.getCurrentSystem();
             // Choose datasource per dbType: if clickhouse -> try that DS key; else use primary (null lets resolver pick primary)
             String desiredKey = (configuredDbType != null && configuredDbType.equalsIgnoreCase(SqlDialect.CLICKHOUSE.name())) ? "CLICKHOUSE" : null;
             String submitDs = configManager.resolveDatasourceKey(submitSystem, desiredKey);

             HashMap<String, Integer> hmKey = new HashMap<>();

             for (QueryObject queryObject : sqlList.getFuncOutput()) {
                 lstColumnName.addAll(queryObject.getColumn());
                 QueryTask queryTask = new QueryTask(queryObject, lstNE, hmCommonField, allInputKpiObjMap, inputStatisticDataClone);
                 compService.submit(() -> routingContext.callWith(submitSystem, submitDs, () -> {
                     try {
                         return queryTask.call();
                     } catch (Exception e) {
                         throw new RuntimeException(e);
                     }
                 }));
             }

             CustomOutput<List<QueryObject>> finalSqlList = sqlList;

            performance.trace(() -> {
                 for (int i = 0; i < finalSqlList.getFuncOutput().size(); i++) {
                     try {
                         Future<List<ReportStatisticKPI>> future = compService.take();
                         List<ReportStatisticKPI> statisticKPIS = future.get(FUTURE_TIMEOUT, TimeUnit.SECONDS);
                         if (statisticKPIS != null) {
                             parseDataFromSQLToObjectJava(result, statisticKPIS, hmKey);
                         }
                     } catch (ExecutionException | TimeoutException e) {
                         log.error("getStatisticFromDatabase {} ", e.getMessage());
                     } catch (InterruptedException e) {
                         log.error("getStatisticFromDatabase interrupted {} ", e.getMessage());
                         // Restore interrupted state...
                         Thread.currentThread().interrupt();
                     }
                 }
                 // runnable
            }, Constant.LOG_FORMAT,  "PMQueryData");

            // shared executor is managed at bean lifecycle; do not shutdown here
         }

         // Offset KPI/Counter
        performance.trace(() -> offsetKPICounter(result, lstColumnName, allInputKpiObjMap, inputStatisticDataClone), Constant.LOG_FORMAT,
              "PMOffsetKPICounter");
        // performance.trace(() -> calculateKPIMix(inputData, result, lstAllKPI, sAllKpiCounter, lstKpiFormulas), Constants.LOG_FORMAT,
        //         "PMCalculateKPI");/*
        performance.trace(() -> filterResultByCondition(inputData, result), Constant.LOG_FORMAT,
                "PMFilterResultByCondition");
        performance.trace(() -> filterResultByPeakIfExist(inputData, result), Constant.LOG_FORMAT,
                "PMFilterPeak");
        if (inputData.isSeparateTime() && hmCommonField.containsKey(DATE_TIME_KEY)) {
            performance.trace(() -> separateTime(result), Constant.LOG_FORMAT,
                    "PMSeparateTime");
        }
        performance.trace(() -> sortData(result), Constant.LOG_FORMAT,
                "PMSortData");

         performance.log( "Total record: " + result.size());
        return result;
    }

    private void separateTime(List<ReportStatisticKPI> result) {
        for(var reportStatisticKPI : result) {
            String[] arrReportTime = reportStatisticKPI.getExtraField().get(DATE_TIME_KEY).split(" ");
            reportStatisticKPI.getExtraField().remove(DATE_TIME_KEY);
            reportStatisticKPI.getExtraField().put(DATE_KEY, arrReportTime[0]);
            reportStatisticKPI.getExtraField().put(TIME_KEY, arrReportTime[1]);
        }
    }







private void filterResultByCondition(InputStatisticData inputStatisticData, List<ReportStatisticKPI> result) {
    // filter
    List<EmsFilter> emsFilters = inputStatisticData.getEmsFilters();
    if (emsFilters != null && emsFilters.size() > 0) {
        try {
            emsFilters.sort(Comparator.comparing(EmsFilter::getIdx));
            List<ReportStatisticKPI> afterFilter = new ArrayList<>();
            List<String> filterName = Util.GetAllCounterNameFromFilter(emsFilters, counterKpiRepository);
            String script = Util.GenerateFilterJSFormat(emsFilters, counterKpiRepository)
                .replace(" and ", " && ")
                .replace(" or ", " || ");
            log.info("filterResultByCellCondition: {}", script);

            for (ReportStatisticKPI reportStatisticKPI : result) {
                String tmpScript = String.copyValueOf(script.toCharArray());
                for (int i = 0; i < filterName.size(); i++) {
                    String strValue = reportStatisticKPI.getParam().get(filterName.get(i)) == null ?
                        "0" : String.valueOf(reportStatisticKPI.getParam().get(filterName.get(i)));

                    /*if (reportStatisticKPI.getParam().get(filterName.get(i)) == null) {
                        // Chi can 1 gia tri = null thi cong thuc se sai -> bo qua luon
                        tmpScript = null;
                        break;
                    } else {
                        tmpScript = tmpScript.replace(filterName.get(i),
                            String.valueOf(reportStatisticKPI.getParam().get(filterName.get(i))));
                    }*/

                    tmpScript = tmpScript.replace(filterName.get(i), strValue);
                }

                Object compare;
                if (tmpScript == null) {
                    log.debug("Filter have counter/KPI not in Input or value is null");
                    continue;
                }

                compare = scriptEngine.eval(tmpScript);
                if (compare instanceof Boolean && (Boolean) compare) {
                    afterFilter.add(reportStatisticKPI);
                }
            }

            result.clear();
            result.addAll(afterFilter);
        } catch (ScriptException e) {
            log.error("filterResultByCondition: {}", e.getMessage());
        }
    }
}

  


 private void sortData(List<ReportStatisticKPI> result) {
     // sort result
     result.sort(Comparator.comparing(ReportStatisticKPI::getExtraFieldKey, Comparator.reverseOrder()));
 }

 private void filterResultByPeakIfExist(InputStatisticData inputStatisticData, List<ReportStatisticKPI> result) {
     if (result.size() > 0 && inputStatisticData.getPeakTimeBy() != null) {
         Boolean peakType = inputStatisticData.getPeakTimeByType();
         if (peakType != null) {
             if (peakType) { // true: find the maximum value
                 ReportStatisticKPI reportStatisticKPI = Collections.max(result, Comparator.comparing(s -> {
                     Double value = s.getParam()
                             .get(counterKpiRepository.getNameCounterKPI(inputStatisticData.getPeakTimeBy()));
                     return value != null ? value : 0d;
                 }));
                 result.clear();
                 result.add(reportStatisticKPI);
             } else {
                 final List<ReportStatisticKPI> tmp = getPeakTimeByType(result,
                         counterKpiRepository.getNameCounterKPI(inputStatisticData.getPeakTimeBy()), Calendar.DATE);
                 result.clear();
                 result.addAll(tmp);
             }
         }
     }
 }


private void offsetKPICounter(List<ReportStatisticKPI> result, Set<String> lstColumnName, Map<String, KpiFormula> allInputKpiObjMap, InputStatisticData inputStatisticData) {
    try {
        if(result == null || lstColumnName == null) {
            return;
        }

        int columeNameSize = lstColumnName.size();
        for(ReportStatisticKPI reportStatisticKPI : result) {
            if(reportStatisticKPI.getParam().size() == columeNameSize) {
                continue;
            }
            for(String columnName : lstColumnName) {
                if(reportStatisticKPI.getParam().containsKey(columnName)) {
                    continue;
                }
                Optional<KpiFormula> kpiWithUnitOptional = Optional.ofNullable(allInputKpiObjMap.get(columnName));
                if(kpiWithUnitOptional.isPresent()) {
                    if(inputStatisticData.isReturnCounterKpiId()) {
                        int kpiId = kpiWithUnitOptional.get().getKpiID();
                        if(reportStatisticKPI.getParam().get(String.valueOf(kpiId)) != null)  {
                            continue;
                        }
                    }
                    if(!inputStatisticData.isReturnCounterKpiId()) {
                        String kpiName = kpiWithUnitOptional.get().getKpiName();
                        String kpiUnit = kpiWithUnitOptional.get().getUnits();
                        if(reportStatisticKPI.getParam().get(kpiName + " (" + kpiUnit + ")") != null) {
                            continue;
                        }
                    }

                    String formatedColumnName = inputStatisticData.isReturnCounterKpiId() ? String.valueOf(kpiWithUnitOptional.get().getKpiName() + " (" + kpiWithUnitOptional.get().getUnits() + ")") : columnName;
                    reportStatisticKPI.getParam().put(formatedColumnName, null);
                }else {
                    reportStatisticKPI.getParam().put(columnName, null);
                }
            }
        }
    }catch(Exception ex) {
        log.error("offsetKpICoutner: {}", ex.toString());
    }
}





    private void parseDataFromSQLToObjectJava(
            List<ReportStatisticKPI> result,
            List<ReportStatisticKPI> statisticKPIS,
            HashMap<String, Integer> hmKey
    ) {
        if (result.isEmpty()) {
            result.addAll(statisticKPIS);
            for (int idx = 0; idx < statisticKPIS.size(); idx++) {
                hmKey.put(statisticKPIS.get(idx).getExtraFieldKey(), idx);
            }
        } else {
            HashMap<String, Integer> hmKeyRemove = (HashMap<String, Integer>) hmKey.clone();
            for (int idx = 0; idx < statisticKPIS.size(); idx++) {
                // Merge multiple records which have the same extraFieldKey to one
                ReportStatisticKPI statisticKPI = statisticKPIS.get(idx);
                String extraFieldKey = statisticKPI.getExtraFieldKey();
                if (hmKeyRemove.containsKey(extraFieldKey)) {
                    result.get(hmKeyRemove.get(extraFieldKey)).getParam().putAll(statisticKPI.getParam());
                    hmKeyRemove.remove(extraFieldKey);
                } else {
                    result.add(statisticKPI);
                    hmKey.put(extraFieldKey, result.size() - 1);
                }
            }
        }
    }


private List<ReportStatisticKPI> getPeakTimeByType(
    List<ReportStatisticKPI> reportStatisticKPIS, 
    String peakCounterKpiName, int type
) {
    List<ReportStatisticKPI> result = new ArrayList<>();
    HashMap<Long, ReportStatisticKPI> tmpMap = new HashMap<>();
    SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
    for (ReportStatisticKPI reportStatisticKPI : reportStatisticKPIS) {
        try {
            if (peakCounterKpiName == null || reportStatisticKPI.getParam()
                .get(peakCounterKpiName) == null)
                continue;
            Date date = DateUtils.truncate(sdf.parse(reportStatisticKPI
                .getExtraField().get(DATE_TIME_KEY)), type);
            if (tmpMap.get(date.getTime()) != null) {
                if (tmpMap.get(date.getTime()).getParam().get(peakCounterKpiName)
                    < reportStatisticKPI.getParam().get(peakCounterKpiName)) {
                    tmpMap.put(date.getTime(), reportStatisticKPI);
                }
            } else {
                tmpMap.put(date.getTime(), reportStatisticKPI);
            }
        } catch (ParseException e) {
            log.error("getPeakTimeByType {}", e.getMessage());
        }
    }
    for (Long key : tmpMap.keySet()) {
        result.add(tmpMap.get(key));
    }
    return result;
}



















  class QueryTask implements Callable<List<ReportStatisticKPI>> {
        private final QueryObject queryObject;
        private final Map<Integer, NeLiteInfo> hmNe;
        private final LinkedHashMap<String, ExtraFieldObject> hmExtraField;
        private final Map<String, KpiFormula> allInputKpiObjMap;
        private final InputStatisticData inputStatisticData;

        public QueryTask(QueryObject queryObject, Map<Integer, NeLiteInfo> hmNe,
                         LinkedHashMap<String, ExtraFieldObject> hmExtraField,
                         Map<String, KpiFormula> allInputKpiObjMap,
                         InputStatisticData inputStatisticData
                         ) {
            this.queryObject = queryObject;
            this.hmNe = hmNe;
            this.hmExtraField = hmExtraField;
            this.allInputKpiObjMap = allInputKpiObjMap;
            this.inputStatisticData = inputStatisticData;
        }

        @Override
        public List<ReportStatisticKPI> call() {
            List<ReportStatisticKPI> data = null;
            try {
            datasourceVerifier.verifyConnection();
            data = getData(queryObject, hmNe, hmExtraField, allInputKpiObjMap, inputStatisticData);
            }catch (Exception e) {
                e.printStackTrace();
            }
            return data;
        }
    }




public static String GenerateFilterJSFormat( List<EmsFilter> filters, CounterKpiRepository counterKpiRepository) {
    StringBuilder result = new StringBuilder();
    for(int idx = 0;idx < filters.size(); idx++) {
        EmsFilter filter = filters.get(idx);
        if(filter.getParameterList() == null) {
            if(filter.getLogicalOperator()!= null)
            result.append(filter.getLogicalOperator()).append(SPACE_CHAR);
            result.append(filter.toJSString(counterKpiRepository.getNameCounterKPI(filter.getId())))
            .append(SPACE_CHAR);
        }else {
            // parent
            if(filter.getLogicalOperator() != null) {
                result.append(filter.getLogicalOperator()).append(SPACE_CHAR);
                result.append(SPACE_CHAR).append("(")
                .append(GenerateFilterJSFormat(filter.getParameterList(), counterKpiRepository))
                .append(")").append(SPACE_CHAR);
            }
        }

    }
    return result.toString();

}




private List<ReportStatisticKPI> getData(QueryObject queryObject, Map<Integer, NeLiteInfo> hmNE, HashMap<String, ExtraFieldObject> hmCommonField,
                                         Map<String, KpiFormula> allInputKpiObjMap, InputStatisticData inputStatisticData) {
    List<ReportStatisticKPI> result = new ArrayList<>();
    List<ReportStatisticKPI> finalResult = result;
    String configuredDbType = configManager.getTheCurrentSystemConfigDB();

    if ("clickhouse".equalsIgnoreCase(configuredDbType)) {
        //Continue...
        SystemType system = TenantContextHolder.getCurrentSystem();
        routingContext.runWith(system,  "CLICKHOUSE", () -> {
            jdbcTemplate.query(queryObject.getSql(), rs -> {
                List<ReportStatisticKPI> reportStatisticKPIS = new ArrayList<>();
                try {
                    do {
                        ReportStatisticKPI reportStatisticKPI = new ReportStatisticKPI();
                        //Common field
                        HashMap<String, String> commonField = new HashMap<>();
                        StringBuilder sbCommonFieldKey = new StringBuilder();
                        hmCommonField.forEach((k, v) -> {
                            try {
                                String commonFieldValue = rs.getObject(v.getColumnName()) != null ? rs.getObject(v.getColumnName()).toString() : "";
                                if (NE_KEY.equalsIgnoreCase(k))
                                    commonField.put(k, hmNE.get(Integer.parseInt(commonFieldValue)).getName());
                                else
                                    commonField.put(k, commonFieldValue);
                                sbCommonFieldKey.append(commonFieldValue).append("-");
                            } catch (SQLException ex) {
                                log.error("hmCommonField: " + ex.getCause());
                            }
                        });
                        reportStatisticKPI.setExtraField(commonField);
                        reportStatisticKPI.setExtraFieldKey(sbCommonFieldKey.toString());
                        //KPI/Counter field
                        for (String column : queryObject.getColumn()) {
                            // in case KPI from multi table
                            KpiFormula kpi = allInputKpiObjMap.get(column); //
                            // allInputKpiObj.stream().filter(kpiFormula -> kpiFormula.getKpiName().equals(column)).findFirst() .orElse(null);
                            String strVal = rs.getString(column);
                            String finalColumn = column;
                            try {
                                if (kpi != null) {
                                    finalColumn = inputStatisticData.isReturnCounterKpiId() ? String.valueOf(kpi.getKpiID()) : kpi.getKpiName() + " (" + kpi.getUnits() + ")";
                                }
                                reportStatisticKPI.getParam().put(finalColumn, Double.parseDouble(strVal));
                            } catch (Exception e) {
                                reportStatisticKPI.getParam().put(finalColumn, null);
                            }
                        }
                        reportStatisticKPIS.add(reportStatisticKPI);
                    } while (rs.next());
                } catch (Exception e) {
                    log.error("getData: {}", e.getCause());
                }
                finalResult.addAll(reportStatisticKPIS);
            });
        });
    } else {
        jdbcTemplate.query(queryObject.getSql(), rs -> {
              List<ReportStatisticKPI> reportStatisticKPIS = new ArrayList<>();
                try {
                    do {
                        ReportStatisticKPI reportStatisticKPI = new ReportStatisticKPI();
                        //Common field
                        HashMap<String, String> commonField = new HashMap<>();
                        StringBuilder sbCommonFieldKey = new StringBuilder();
                        hmCommonField.forEach((k, v) -> {
                                    try {
                                        String commonFieldValue = rs.getObject(v.getColumnName()) != null ? rs.getObject(v.getColumnName()).toString() : "";
                                        if (NE_KEY.equalsIgnoreCase(k))
                                            commonField.put(k, hmNE.get(Integer.parseInt(commonFieldValue)).getName());
                                        else
                                            commonField.put(k, commonFieldValue);
                                        sbCommonFieldKey.append(commonFieldValue).append("-");
                                    } catch (SQLException ex) {
                                        log.error("hmCommonField: {}", ex.getCause().getMessage());
                                    }
                                });
                                reportStatisticKPI.setExtraField(commonField);
                                reportStatisticKPI.setExtraFieldKey(sbCommonFieldKey.toString());
                                //KPI/Counter field
                                for (String column : queryObject.getColumn()) {
                                    String strVal = rs.getString(column);
                                    try {
                                        reportStatisticKPI.getParam().put(column, strVal == null ? Double.NaN : Double.parseDouble(strVal));
                                    } catch (Exception e) {
                                        reportStatisticKPI.getParam().put(column, null);
                                    }
                                }
                                reportStatisticKPIS.add(reportStatisticKPI);

                        }while (rs.next());
    } catch (Exception e) {
        log.error("getData: {}", e.getCause().getMessage());
    }
    finalResult.addAll(reportStatisticKPIS);
                });
}
return finalResult;
                                         }













public static List<String> GetAllCounterNameFromFilter(
    List<EmsFilter> filters, CounterKpiRepository counterKpiRepository
) {
    List<String> result = new ArrayList();
    for(EmsFilter filter : filters) {
        if(filter.getParameterList() != null && filter.getParameterList().size() > 0 ){
            result.addAll(GetAllCounterNameFromFilter(filter.getParameterList(),counterKpiRepository));
        }else {
            result.add(counterKpiRepository.getNameCounterKPI(filter.getId()));
        }
    }

    return result;
}
public List<String> getRawData(QueryObject queryObject, int limit) {
    List<String> data = new ArrayList<>();
    log.info("[Raw Data] SQL Query: \n\n" + queryObject.getSql());
    long time = System.currentTimeMillis();
    String dbType = configManager.getTheCurrentSystemConfigDB();
    if (dbType.equals("mysql")) {
        jdbcTemplate.query(queryObject.getSql(), rs -> {
            String line = new String();
            for (String col : queryObject.getColumn()) {
                line += (rs.getString(col) + ",");
            }
            if (line != "") {
                data.add(line.substring(0, line.length() - 1));
            }
        });
    } else {
        SystemType system = TenantContextHolder.getCurrentSystem();
        routingContext.runWith(system,  "CLICKHOUSE", () -> {
            jdbcTemplate.query(queryObject.getSql(), rs -> {
                String line = new String();
                for (String col : queryObject.getColumn()) {
                    line += (rs.getString(col) + ",");
                }
                if (line != "") {
                    data.add(line.substring(0, line.length() - 1));
                }
            });
        });
    }

    log.info("[Raw Data] Time Query: " + (System.currentTimeMillis() - time));
    return limit > 0 ? data.stream().limit(limit).collect(Collectors.toList()) : data;
}



    /**
     * Tối ưu: Chuyển đổi danh sách tất cả KPI thành Map để tra cứu O(1).
     * Đồng thời khởi tạo các Set để theo dõi việc duyệt đồ thị phụ thuộc.
     */
    private Set<Integer> flatAllCounterKpi(List<KpiFormula> kpiFormulas, List<KpiFormula> lstAllKpi, List<Integer> listCounterKPI) {
        // Tối ưu 1: Chuyển List sang Map để lookup O(1)
        // Giả sử KpiID là duy nhất, nếu không cần xử lý duplicate key
        Map<Integer, KpiFormula> allKpiMap = lstAllKpi.stream()
                .collect(Collectors.toMap(KpiFormula::getKpiID, Function.identity(), (k1, k2) -> k1));

        // Set kết quả cuối cùng
        Set<Integer> allKpiCounters = new HashSet<>(Optional.ofNullable(listCounterKPI).orElseGet(ArrayList::new));

        // Set để theo dõi các KPI đã được duyệt xong (tối ưu hóa, tránh duyệt lại)
        Set<Integer> visited = new HashSet<>();

        // Set để theo dõi đường dẫn đệ quy hiện tại (phát hiện vòng lặp)
        Set<Integer> recursionStack = new HashSet<>();

        try {
            // Duyệt qua các KPI đầu vào
            for (var kpi : kpiFormulas) {
                // Chỉ gọi đệ quy nếu KPI này chưa được duyệt xong
                if (!visited.contains(kpi.getKpiID())) {
                    collectAllKpiAndCounters(kpi, allKpiCounters, recursionStack, visited, allKpiMap);
                }
            }
        } catch (KpiCycleException e) {
            // Xử lý lỗi: log ra và có thể ném lại hoặc trả về set rỗng
            log.error("Phát hiện phụ thuộc vòng tròn (circular dependency) khi phân tích KPI: {}", e.getMessage());
            // Ném lỗi để báo cho nơi gọi (ví dụ: Access5gController) biết rằng yêu cầu bị lỗi
            throw e;
        }

        return allKpiCounters;
    }

    /**
     * Hàm đệ quy đã được tối ưu để duyệt đồ thị phụ thuộc KPI.
     *
     * @param kpiFormula     KPI hiện tại đang duyệt
     * @param allKpiCounters Set tổng hợp kết quả (được thay đổi trực tiếp)
     * @param recursionStack Set theo dõi đường dẫn đệ quy *hiện tại* (để phát hiện vòng lặp)
     * @param visited        Set theo dõi các node *đã duyệt xong* (để tối ưu, không duyệt lại)
     * @param allKpiMap      Map tra cứu nhanh tất cả KPI
     */
    public void collectAllKpiAndCounters(
            KpiFormula kpiFormula,
            Set<Integer> allKpiCounters,
            Set<Integer> recursionStack,
            Set<Integer> visited,
            Map<Integer, KpiFormula> allKpiMap
    ) {
        Integer kpiId = kpiFormula.getKpiID();

        // 1. PHÁT HIỆN VÒNG LẶP (Cycle Detection)
        // Nếu ID này đã có trong đường dẫn đệ quy -> BÁO LỖI
        if (recursionStack.contains(kpiId)) {
            throw new KpiCycleException("Phát hiện vòng lặp KPI tại ID: " + kpiId + ". Đường dẫn: " + recursionStack);
        }

        // 2. TỐI ƯU HÓA
        // Nếu KPI này đã được duyệt xong (và tất cả con của nó) thì bỏ qua
        if (visited.contains(kpiId)) {
            return;
        }

        // 3. ĐÁNH DẤU: Thêm vào đường dẫn đệ quy (bắt đầu duyệt)
        recursionStack.add(kpiId);

        // 4. XỬ LÝ: Thêm KPI và các Counter phụ thuộc vào kết quả
        allKpiCounters.add(kpiId);
        parseIds(kpiFormula.getArrCounter()).forEach(allKpiCounters::add);

        // 5. ĐỆ QUY: Xử lý các KPI phụ thuộc (arrKpi)
        List<Integer> subKpiIds = parseIds(kpiFormula.getArrKpi());
        allKpiCounters.addAll(subKpiIds); // Thêm các sub-KPI ID vào kết quả

        for (Integer subKpiId : subKpiIds) {
            KpiFormula relatedKpi = allKpiMap.get(subKpiId);

            if (relatedKpi != null) {
                // Gọi đệ quy với các Set tracking
                collectAllKpiAndCounters(relatedKpi, allKpiCounters, recursionStack, visited, allKpiMap);
            } else {
                // Đây chỉ là một Counter/KPI đơn giản không có trong bảng KpiFormula
                // (hoặc lỗi dữ liệu), thêm vào allKpiCounters đã được thực hiện ở trên.
                log.warn("Related KPI not found in map for ID: {}", subKpiId);
            }
        }

        // 6. HOÀN TẤT:
        // Xóa khỏi đường dẫn đệ quy (đã duyệt xong node này)
        recursionStack.remove(kpiId);
        // Thêm vào danh sách đã duyệt xong (để tối ưu)
        visited.add(kpiId);
    }

    /**
     * Hàm tiện ích để parse chuỗi ID (arr_counter, arr_kpi) một cách an toàn.
     */
    private List<Integer> parseIds(String idString) {
        if (idString == null || idString.trim().isEmpty()) {
            return Collections.emptyList();
        }

        List<Integer> ids = new ArrayList<>();
        String[] idArray = idString.split(",");
        for (String idStr : idArray) {
            try {
                ids.add(Integer.parseInt(idStr.trim()));
            } catch (NumberFormatException e) {
                log.error("Invalid ID format in dependency string: '{}'", idStr, e);
            }
        }
        return ids;
    }



//
//
//
//
//
//    private Set<Integer> flatAllCounterKpi(List<KpiFormula> kpiFormulas, List<KpiFormula> lstAllKpi, List<Integer> listCounterKPI) {
//        Set<Integer> processedKpiIds = new HashSet<>();
//        // khởi tạo là kpi / counter từ input
//        Set<Integer> allKpiCounters = new HashSet<>(Optional.ofNullable(listCounterKPI).orElseGet(ArrayList::new));
//
//        for (var kpi : kpiFormulas) {
//            collectAllKpiAndCounters(kpi, allKpiCounters, processedKpiIds, lstAllKpi);
//        }
//
//        return allKpiCounters;
//    }
//
//    public void collectAllKpiAndCounters(KpiFormula kpiFormula, Set<Integer> allKpiCounters,
//                                          Set<Integer> processedKpiIds, List<KpiFormula> lstAllKpi) {
//
//        processedKpiIds.add(kpiFormula.getKpiID());
//
//            // Add current KPI ID
//            allKpiCounters.add(kpiFormula.getKpiID());
//
//            // Process arrCounter if exists
//            if (kpiFormula.getArrCounter() != null && !kpiFormula.getArrCounter().trim().isEmpty()) {
//                String[] arrCounterIds = kpiFormula.getArrCounter().split(",");
//                for (String counterIdStr : arrCounterIds) {
//                    try {
//                        Integer counterId = Integer.parseInt(counterIdStr.trim());
//                        allKpiCounters.add(counterId);
//                    } catch (NumberFormatException e) {
//                        log.error("Invalid counter ID format: {} in KPI {}", counterIdStr, kpiFormula.getKpiID());
//                    }
//                }
//            }
//
//            // Process arrKpi if exists (recursive)
//            if (kpiFormula.getArrKpi() != null && !kpiFormula.getArrKpi().trim().isEmpty()) {
//                String[] arrKpiIds = kpiFormula.getArrKpi().split(",");
//                for (String kpiIdStr : arrKpiIds) {
//                    try {
//                        Integer kpiId = Integer.parseInt(kpiIdStr.trim());
//                        allKpiCounters.add(kpiId);
//
//                        // Tìm KPI liên quan và xử lý đệ quy
//                        KpiFormula relatedKpi = lstAllKpi.stream()
//                            .filter(kpi -> kpi.getKpiID() == kpiId)
//                            .findFirst()
//                            .orElse(null);
//
//                        if (relatedKpi != null) {
//                            collectAllKpiAndCounters(relatedKpi, allKpiCounters, processedKpiIds, lstAllKpi);
//                        } else {
//                            log.warn("Related KPI not found for ID: {}", kpiId);
//                        }
//                    } catch (NumberFormatException e) {
//                        log.error("Invalid KPI ID format: {} in KPI {}", kpiIdStr, kpiFormula.getKpiID());
//                    }
//                }
//            }
//    }


//     private void collectAllKpiAndCounters(KpiFormula kpiFormula, Set<Integer> allKpiCounters,
//                                           Set<Integer> processedKpiIds, List<KpiFormula> lstAllKpi, Set<Integer> childKpiIds) {
//         // Prevent infinite recursion
//         if (processedKpiIds.contains(kpiFormula.getKpiID())) {
// //            log.warn("Circular dependency detected for KPI ID: {} - Skipping this KPI and its dependencies", kpiFormula.getKpiID());

//             // Chỉ xóa KPI hiện tại
//             allKpiCounters.remove(kpiFormula.getKpiID());

//             // Xóa tất cả KPI đang được xử lý trong call stack (cha, ông, cụ...)
//             for (Integer kpiId : processedKpiIds) {
//                 allKpiCounters.remove(kpiId);
//             }

//             return;
//         }

//         processedKpiIds.add(kpiFormula.getKpiID());

//         try {
//             // Add current KPI ID
//             allKpiCounters.add(kpiFormula.getKpiID());

//             // Process arrCounter if exists
//             if (kpiFormula.getArrCounter() != null && !kpiFormula.getArrCounter().trim().isEmpty()) {
//                 String[] arrCounterIds = kpiFormula.getArrCounter().split(",");
//                 for (String counterIdStr : arrCounterIds) {
//                     try {
//                         Integer counterId = Integer.parseInt(counterIdStr.trim());
//                         allKpiCounters.add(counterId);
//                     } catch (NumberFormatException e) {
//                         log.error("Invalid counter ID format: {} in KPI {}", counterIdStr, kpiFormula.getKpiID());
//                     }
//                 }
//             }

//             // Process arrKpi if exists (recursive)
//             if (kpiFormula.getArrKpi() != null && !kpiFormula.getArrKpi().trim().isEmpty()) {
//                 String[] arrKpiIds = kpiFormula.getArrKpi().split(",");
//                 for (String kpiIdStr : arrKpiIds) {
//                     try {
//                         Integer kpiId = Integer.parseInt(kpiIdStr.trim());
//                         allKpiCounters.add(kpiId);

//                         // Tìm KPI liên quan và xử lý đệ quy
//                         KpiFormula relatedKpi = lstAllKpi.stream()
//                             .filter(kpi -> kpi.getKpiID() == kpiId)
//                             .findFirst()
//                             .orElse(null);

//                         if (relatedKpi != null) {
//                             childKpiIds.add(kpiId);
//                             collectAllKpiAndCounters(relatedKpi, allKpiCounters, processedKpiIds, lstAllKpi, childKpiIds);
//                         } else {
//                             log.warn("Related KPI not found for ID: {}", kpiId);
//                         }
//                     } catch (NumberFormatException e) {
//                         log.error("Invalid KPI ID format: {} in KPI {}", kpiIdStr, kpiFormula.getKpiID());
//                     }
//                 }
//             }
//         } finally {
//             // Quan trọng: Luôn xóa KPI khỏi processedKpiIds, kể cả khi có exception
//             processedKpiIds.remove(kpiFormula.getKpiID());
//         }
//     }

//     private void findChildKpis(KpiFormula kpiFormula, List<KpiFormula> lstAllKpi,
//                                Set<Integer> childKpiIds, Set<Integer> visited) {
//         // Prevent infinite recursion
//         if (visited.contains(kpiFormula.getKpiID())) {
//             return;
//         }

//         visited.add(kpiFormula.getKpiID());

//         // Tìm tất cả KPI con (KPI được sử dụng trong arrKpi)
//         if (kpiFormula.getArrKpi() != null && !kpiFormula.getArrKpi().trim().isEmpty()) {
//             String[] arrKpiIds = kpiFormula.getArrKpi().split(",");
//             if (arrKpiIds.length == 0) {
//                 childKpiIds.remove(kpiFormula.getKpiID());
//             }
//             for (String kpiIdStr : arrKpiIds) {
//                 try {
//                     int kpiId = Integer.parseInt(kpiIdStr.trim());
//                     childKpiIds.add(kpiId); // Đây là KPI con

//                     // Đệ quy tìm KPI con của KPI con
//                     KpiFormula childKpi = lstAllKpi.stream()
//                         .filter(kpi -> kpi.getKpiID() == kpiId)
//                         .findFirst()
//                         .orElse(null);

//                     if (childKpi != null) {
//                         findChildKpis(childKpi, lstAllKpi, childKpiIds, visited);
//                     }
//                 } catch (NumberFormatException e) {
//                     log.error("Invalid KPI ID format: {} in KPI {}", kpiIdStr, kpiFormula.getKpiID());
//                 }
//             }
//         }

//         visited.remove(kpiFormula.getKpiID());
//     }


}
