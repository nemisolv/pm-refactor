package com.viettel.ems.perfomance.repository;


import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.RoutingContextExecutor;
import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.config.TenantContextHolder;
import com.viettel.ems.perfomance.object.*;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class CounterCounterCatRepository {
    private final JdbcTemplate jdbcTemplateMySQL;
    private final RoutingContextExecutor routingContext;
    private final ConfigManager configManager;


    @Value("spring.data-lake.counter_cat_id")
    private String counterCatIds;
    @Value("${spring.pm.clickhouse_cluster}")
    private String clickHouseCluster;
    @Value("${spring.kafka.producer.bootstrap-address}")
    private String kafkaBootStrapAddress;


    @PostConstruct
    private void setClickhouseCluster() {
        if(!clickHouseCluster.equals("none")) {
            clickHouseCluster = " on cluster " + clickHouseCluster;
        }else {
            clickHouseCluster = "";
        }
    }

    public List<CounterCounterCatObject> findAll() {
        List<CounterCounterCatObject> result ;
        final String sql = "SELECT c.id, c.counter_cat_id, cc.code, cc.object_level_id, c.is_sub_kpi, c.sk_cat_id, c.kpi_type \n" +
                "FROM counter c, counter_cat cc\n" +
                "WHERE c.status = 1 and ((c.counter_cat_id = cc.id and is_kpi = 0) or (c.is_sub_kpi = 1 and c.sk_cat_id = cc.id)) \n" +
                "ORDER BY c.id;";
        try {
            result = jdbcTemplateMySQL.query(sql, (rs, _row) -> CounterCounterCatObject.fromRs(rs));
            return result;
        }catch (Exception e){
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public HashMap<String, CounterConfigObject> findCounterOran() {
        try {
            final String sql = "Select id, name, position, measurement_identifier, measurement_object, measurement_group \n" +
                    "FROM counter \n" +
                    "WHERE is_kpi = 0 and position IS NOT NULL \n" +
                    "ORDER BY measurement_group, measurement_object, position;";
            HashMap<String, CounterConfigObject> result = new HashMap<>();
            setRsCounterOran(sql, result, jdbcTemplateMySQL);
            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new HashMap<>();
        }
    }

    private void setRsCounterOran(String sql, HashMap<String, CounterConfigObject> result, JdbcTemplate jdbcTemplateMySQL) {
        jdbcTemplateMySQL.query(sql,
                rs -> {
                    int id = rs.getInt("id");
                    String name = rs.getString("name").trim();
                    int position = rs.getInt("position");
                    int measurementIdentifier = rs.getInt("measurement_identifier");
                    String measurementObject = rs.getString("measurement_object").trim().toUpperCase();
                    String measurementGroup = rs.getString("measurement_group").trim();
                    String measurementKey = measurementIdentifier + "_" +  measurementObject;
                    LiteCounterObject liteCounterObject = LiteCounterObject.builder()
                            .id(id)
                            .name(name)
                            .position(position)
                            .build();
                    if(result.containsKey(measurementKey)) {
                        result.get(measurementKey).getHmLiteCounter().put(position, liteCounterObject);
                    }else {
                        HashMap<Integer, LiteCounterObject> hmLiteCounter = new HashMap<>();
                        hmLiteCounter.put(position, liteCounterObject);
                        CounterConfigObject counterConfigObject = CounterConfigObject.builder()
                                .measurementIdentifier(measurementIdentifier)
                                .measurementObject(measurementObject)
                                .measurementGroup(measurementGroup)
                                .hmLiteCounter(hmLiteCounter)
                                .build();
                        result.put(measurementKey,counterConfigObject);
                    }
                });
    }

    public Set<String> getListCounterCatCodeONT() {
        Set<String> codes = new HashSet<>();
        jdbcTemplateMySQL.query(String.format("select code from counter_cat where id in (%s)", counterCatIds),
                (rs) -> {
                        String code = rs.getString("code");
                        if(!Objects.equals(null, code)) {
                            codes.add(code);
                        }
                        return null;
                });
                return codes;
    }


    public List<String> getTableHeader(String tableName) {
        String query = "select * from " + tableName + " limit 1";
        List<String> header = new ArrayList<>();
        SystemType systemType = TenantContextHolder.getCurrentSystem();
        routingContext.runWith(systemType, "CLICKHOUSE", () -> {
            jdbcTemplateMySQL.query(query, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                for(int i = 1;i <= columnCount;i++) {
                    header.add(metaData.getColumnName(i));
                }
                return null;
            });
            log.info("[getTableHeader] tableName: " + tableName);
        });
        return header;
    }

    public Map<String, String> getCounterCatNamesMappedByCounterColumn(Map<String, List<String> > hmHeaders) {
        List<Integer> ids = new ArrayList<>();
        for(List<String> table : hmHeaders.values()) {
            for(String header : table) {
                if(header.toLowerCase().startsWith("c_")) {
                    try {
                        Integer id = Integer.parseInt(header.toLowerCase().replace("c_", ""));
                        ids.add(id);
                    }catch (Exception e) {

                    }
                }
            }
        }

        Map<String, Object> args = new HashMap<>();
        args.put("ids", ids);
        Map<String, String> result = new Hashtable<>();

        SystemType system = TenantContextHolder.getCurrentSystem();
        routingContext.runWith(system, "CLICKHOUSE", () -> {
             NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplateMySQL);
             namedParameterJdbcTemplate.query("SELECT id, `name` FROM counter WHERE id IN (:ids)", args, 
            (rs, rowNum) -> {
                result.put(String.format("c_%d", rs.getInt("id")), rs.getString("name"));
                return null;
            } );
        });
        return result;
    }

    public List<CounterCatObject> getUpdatedCounterCat(String lastTimeUpdate) {
        List<CounterCatObject> counterCatObjects = new ArrayList<>();
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("select id, code, object_level_id, is_sub_kpi_cat from counter_cat");
            if(lastTimeUpdate !=  null) {
                sql.append(String.format(" where (created_date >= '%s' or updated_date >= '%s');", lastTimeUpdate, lastTimeUpdate));
            }
            jdbcTemplateMySQL.query(sql.toString(), (rs) -> {
                CounterCatObject counterCatObject = null;
                try {
                    counterCatObject = CounterCatObject.fromRs(rs);
                }catch (Exception e) {
                    log.error("Error while adding counter cat to list: {}", e.getMessage(), e);
                }
                counterCatObjects.add(counterCatObject);
            });
            counterCatObjects.removeIf(Objects::isNull);
        }catch (Exception e) {
            log.error("Error while getting counter cat list: {}", e.getMessage(), e);
        }
        return counterCatObjects;
    }

   public List<CounterCounterCatObject> getUpdatedCounterCounterCat(String lastTimeUpdate) {
       List<CounterCounterCatObject> counterCounterCatObjects = new ArrayList<>();
       try {
           StringBuilder sql = new StringBuilder();
           sql.append("select c.id, c.counter_cat_id, cc.code, cc.object_level_id, c.is_sub_kpi, c.sk_cat_id, c.kpi_type " +
                   "from counter c,counter_cat cc " +
                   "where (is_kpi = 0 or is_sub_kpi = 1) and c.status = 1 and (( c.counter_cat_id = cc.id and is_kpi = 0) or (c.is_sub_kpi = 1 and c.sk_cat_id = cc.id)) ");
         if(lastTimeUpdate !=  null) {
                sql.append(String.format(" and (c.created_date >= '%s' or c.updated_date >= '%s');", lastTimeUpdate, lastTimeUpdate));
            }
            jdbcTemplateMySQL.query(sql.toString(), (rs) -> {
                CounterCounterCatObject counterCatObject = null;
                try {
                    counterCatObject = CounterCounterCatObject.fromRs(rs);
                counterCounterCatObjects.add(counterCatObject);

                }catch (Exception e) {
                    log.error("Eror while adding counter cat to list: {}", e.getMessage(), e);
                }
            });
            counterCounterCatObjects.removeIf(Objects::isNull);
        }catch (Exception e) {
            log.error("Error while getting counter cat list: {}", e.getMessage(), e);
        }
        return counterCounterCatObjects;
   }


   public void addCounterCatTableToDb(String counterCatCode, HashMap<String, ExtraFieldObject> extraFieldObjectHashMap, List<Integer> counterIdList, boolean isSubCat) {
       StringBuilder sql = new StringBuilder();
       boolean isUsingClickhouse = configManager.getCustomBoolean(TenantContextHolder.getCurrentSystem(), "isUsingClickhouse");
       if(isUsingClickhouse) {
           String clickHouseDatabase = configManager.getCustomValue(TenantContextHolder.getCurrentSystem(), "clickHouseDatabase");
           sql.append("CREATE TABLE IF NOT EXISTS ").append(clickHouseDatabase).append(".")
           .append(counterCatCode).append(clickHouseCluster);

           String tableColumns = getTableColumnString(extraFieldObjectHashMap, counterIdList, false, isSubCat);
           sql.append(tableColumns);

           if(clickHouseCluster.equals("")) {
               sql.append(" ENGINE = ReplacingMergeTree");
           }else {
               sql.append(" ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/")
                       .append(clickHouseDatabase)
                       .append("/{uuid}', '{replica}')");
           }
           sql.append(" PARTITION BY toYYYYMM(record_time)")
                   .append(" PRIMARY KEY (ne_id, record_time");
           if(extraFieldObjectHashMap != null && !extraFieldObjectHashMap.isEmpty())
               sql.append(", ").append(extraFieldObjectHashMap.values().stream().map(extraFieldObject ->
                       String.format("`%s`", extraFieldObject.getColumnName()))
                       .collect(Collectors.joining(", ")));
           sql.append(")")
                 .append(" ORDER BY (ne_id, record_time");
           if(extraFieldObjectHashMap != null &&  !extraFieldObjectHashMap.isEmpty())
               sql.append(", ").append(extraFieldObjectHashMap.values().stream()
               .map(extraFieldObject ->  String.format("`%s`", extraFieldObject.getColumnName()))
               .collect(Collectors.joining(", ")));
          sql.append(")")
               .append(" SETTINGS index_granularity = 8192;");
            executeSqlQuery(sql.toString(), true);
            createClickhouseQueueTable(counterCatCode, extraFieldObjectHashMap, counterIdList, isSubCat);
            sql.setLength(0);
       }
       boolean isUsingMySql = configManager.getCustomBoolean(TenantContextHolder.getCurrentSystem(), "isUsingMySQL");

        if(isUsingMySql) {
            sql.append("create table if not exists ").append(counterCatCode)
            .append(" (record_time datetime not null, ")
            .append("ne_id int not null, ")
            .append("duration int default 300 not null, ");

            if(extraFieldObjectHashMap != null && !extraFieldObjectHashMap.isEmpty()) {
                sql.append(extraFieldObjectHashMap.values().stream()
                .map(extraFieldObject -> String.format("`%s` %s", extraFieldObject.getColumnName()
                , extraFieldObject.getColumnType().equals("string") ? 
                "varchar(200)" : extraFieldObject.getColumnType()))
                .collect(Collectors.joining(", ")))
                .append(", ");
            }
            if(isSubCat) sql.append("cycle_count int default 1, ");
            sql.append("created_date datetime default current_timestamp() not null, ");
            if(!counterIdList.isEmpty()) {
                sql.append(counterIdList.stream().map(counterId -> String.format("%s_%s bigint default 0", isSubCat ? "k": "c", counterId))
                .collect(Collectors.joining(", ")))
                .append(", ");
            }
            sql.append("constraint idx unique (record_time, ne_id");
            if(extraFieldObjectHashMap != null && !extraFieldObjectHashMap.isEmpty()) {
                // Math: 120 chars * 4 bytes * 6 columns = 2880 bytes (Safe < 3072)
                sql.append(", ").append(extraFieldObjectHashMap.values().stream()
                        .map(extraFieldObject -> {
                            if ("string".equalsIgnoreCase(extraFieldObject.getColumnType())) {
                                return String.format("`%s`(120)", extraFieldObject.getColumnName());
                            }
                            return String.format("`%s`", extraFieldObject.getColumnName());
                        })
                        .collect(Collectors.joining(", ")));
            }
            sql.append("))");
            if(!executeSqlQuery(sql.toString(), false)) return;
            sql.setLength(0);
            sql.append("create index ").append(counterCatCode)
            .append("_ne_id on ")
            .append(counterCatCode).append(" (ne_id);");
            executeSqlQuery(sql.toString(), false);
            sql.setLength(0);
            sql.append("create index ").append(counterCatCode).append("_idx on ")
            .append(counterCatCode).append(" (record_time, ne_id");
            if(extraFieldObjectHashMap !=  null && !extraFieldObjectHashMap.isEmpty()) {
                sql.append(",").append(extraFieldObjectHashMap.values().stream()
                        .map(extraFieldObject -> {
                            if ("string".equalsIgnoreCase(extraFieldObject.getColumnType())) {
                                return String.format("`%s`(120)", extraFieldObject.getColumnName());
                            }
                            return String.format("`%s`", extraFieldObject.getColumnName());
                        })
                        .collect(Collectors.joining(", ")) );
            }
            sql.append(");");
            executeSqlQuery(sql.toString(), false);
        }
   }


    private String getTableColumnString(HashMap<String, ExtraFieldObject> extraFieldObjectHashMap,
                                        List<Integer> counterIdList, boolean isCreatingQueuetable, boolean isSubCat) {

        // Use a List to collect column definitions.
        // This prevents "trailing comma" syntax errors automatically.
        List<String> columns = new ArrayList<>();

        // 1. Standard Columns
        columns.add("record_time Datetime NOT NULL");
        columns.add("duration Int32 DEFAULT 300 not null");
        columns.add("ne_id Int32 NOT NULL");

        // 2. Extra Fields (Location, RatType, etc.)
        if (extraFieldObjectHashMap != null && !extraFieldObjectHashMap.isEmpty()) {
            Set<String> addedColumns = new HashSet<>();
            for (ExtraFieldObject field : extraFieldObjectHashMap.values()) {
                if (addedColumns.add(field.getColumnName())) {
                    String colType = field.getColumnType().substring(0, 1).toUpperCase() + field.getColumnType().substring(1);
                    // ClickHouse String type usually doesn't need length, but if you want Nullable:
                    columns.add(String.format("`%s` %s", field.getColumnName(), colType));
                }
            }
        }

        // 3. Sub KPI Column
        if (isSubCat) {
            columns.add("cycle_count Int64 default 1");
        }

        // 4. Created Date (Not needed for Queue tables usually)
        if (!isCreatingQueuetable) {
            columns.add("created_date datetime default now()");
        }

        // 5. Dynamic Counters
        if (counterIdList != null && !counterIdList.isEmpty()) {
            for (Integer counterId : counterIdList) {
                String prefix = isSubCat ? "k_" : "c_";
                columns.add(String.format("%s%s Int64 DEFAULT 0 NOT NULL", prefix, counterId));
            }
        }

        // Join all columns with a comma and wrap in parentheses
        return "(" + String.join(", ", columns) + ") ";
    }


    private void createClickhouseQueueTable(String counterCatCode, HashMap<String, ExtraFieldObject> extraFieldObjectHashMap, List<Integer> counterIdList, boolean isSubCat) {
    String clickHouseDatabase = configManager.getCustomValue(TenantContextHolder.getCurrentSystem(), "clickHouseDatabase");

    StringBuilder sql = new StringBuilder();
    sql.append("Drop table IF EXISTS ")
        .append(clickHouseDatabase).append(".").append(counterCatCode)
        .append("_mv ").append(clickHouseCluster);
    executeSqlQuery(sql.toString(), true);
    sql.setLength(0);

    sql.append("Drop table IF EXISTS ")
        .append(clickHouseDatabase).append(".").append(counterCatCode)
        .append("_queue").append(clickHouseCluster);
    executeSqlQuery(sql.toString(), true);
    sql.setLength(0);

    sql.append("Drop table IF EXISTS ")
        .append(clickHouseDatabase).append(".").append(counterCatCode)
        .append("_view ").append(clickHouseCluster);
    executeSqlQuery(sql.toString(), true);
    sql.setLength(0);

    sql.append("CREATE TABLE IF NOT EXISTS ").append(clickHouseDatabase).append(".")
        .append(counterCatCode).append("_queue").append(clickHouseCluster);
    String tableColumns = getTableColumnString(extraFieldObjectHashMap, counterIdList, true, isSubCat);
    sql.append(tableColumns).append(" ENGINE = Kafka('").append(kafkaBootStrapAddress)
        .append("', '").append(counterCatCode)
        .append("', 'clickhouse', 'JSONEachRow') SETTINGS kafka_num_consumers = 2;");
    executeSqlQuery(sql.toString(), true);
    sql.setLength(0);

    sql.append("CREATE MATERIALIZED VIEW IF NOT EXISTS ").append(clickHouseDatabase).append(".")
        .append(counterCatCode).append("_mv").append(clickHouseCluster);
    sql.append(" TO ").append(clickHouseDatabase).append(".").append(counterCatCode)
        .append(" AS SELECT * FROM ").append(clickHouseDatabase).append(".")
        .append(counterCatCode).append("_queue");
    executeSqlQuery(sql.toString(), true);
    sql.setLength(0);

    sql.append("CREATE VIEW IF NOT EXISTS ").append(clickHouseDatabase).append(".")
        .append(counterCatCode).append("_view ").append(clickHouseCluster);
    sql.append(" AS SELECT * FROM ").append(clickHouseDatabase).append(".")
        .append(counterCatCode).append(" FINAL");
    executeSqlQuery(sql.toString(), true);
}

boolean executeSqlQuery(String sql, boolean isClickHouse) {
    log.debug("Executing SQL: {}", sql);
    try {
        if (isClickHouse) {
            SystemType system = TenantContextHolder.getCurrentSystem();
            routingContext.runWith(system, "CLICKHOUSE", () -> {
                jdbcTemplateMySQL.execute(sql);
            });
        } else {
            jdbcTemplateMySQL.execute(sql);
        }
        return true;
    } catch (Exception e) {
        log.error("Error while execute query {}: {}", sql, e.getMessage(), e);
        return false;
    }
}


public void addCounterToTable(List<CounterCounterCatObject> lstCounterCounterCatObject,
                              Map<Integer, CounterCounterCatObject> hmCounterCounterCat,
                              Map<Integer, HashMap<String, ExtraFieldObject>> extraFieldObjectHashMap,
                              String cacheInterval) {

    cacheInterval = cacheInterval.equals("") ? "" : "_" + cacheInterval;
    boolean isUsingMysql = configManager.getCustomBoolean(TenantContextHolder.getCurrentSystem(), "isUsingMySQL");

    if (isUsingMysql) {
        for (CounterCounterCatObject counterCounterCatObject : lstCounterCounterCatObject) {
            try {
                if (counterCounterCatObject.isSubKpi()) {
                    executeSqlQuery(
                        String.format("Alter table %s_%s%s add column if not exists k_%s bigint default 0;",
                            counterCounterCatObject.getGroupCode(),
                            Constant.KpiType.valueOf(counterCounterCatObject.getKpiType()),
                            cacheInterval,
                            counterCounterCatObject.getCounterId()
                        ),
                        false
                    );
                } else {
                    executeSqlQuery(
                        String.format("Alter table %s%s add column if not exists c_%s bigint default 0;",
                            counterCounterCatObject.getGroupCode(),
                            cacheInterval,
                            counterCounterCatObject.getCounterId()
                        ),
                        false
                    );
                }
            } catch (Exception e) {
                log.error("Error while updating counter with id: {}", counterCounterCatObject.getCounterId(), e);
            }
        }
    }
    boolean isUsingClickhouse = configManager.getCustomBoolean(TenantContextHolder.getCurrentSystem(), "isUsingClickhouse");
    if(isUsingClickhouse) {
        String clickhouseDatabase = configManager.getCustomValue(TenantContextHolder.getCurrentSystem(), "clickHouseDatabase");
        HashMap<String,Integer> counterCatUpdateMap = new HashMap<>();
        for(CounterCounterCatObject counterCounterCatObject : lstCounterCounterCatObject) {
            try {
                counterCatUpdateMap.put(counterCounterCatObject.getGroupCode(), counterCounterCatObject.getObjectLevelId());
                if(counterCounterCatObject.isSubKpi() && !cacheInterval.equals("")) {
                    executeSqlQuery(
                            String.format("Alter table %s.%s_%s%s%s add column if not exists k_%s Int64 default 0",
                                    clickhouseDatabase,
                                    counterCounterCatObject.getGroupCode(),
                                    Constant.KpiType.valueOf(counterCounterCatObject.getKpiType()),
                                    cacheInterval,
                                    clickHouseCluster,
                                    counterCounterCatObject.getCounterId()
                                    ), true
                    );
                }else {
                    executeSqlQuery(
                            String.format("Alter table %s.%s%s%s add column if not exists c_%s Int64 default 0",
                                    clickhouseDatabase,
                                    counterCounterCatObject.getGroupCode(),
                                    cacheInterval,
                                    clickHouseCluster,
                                    counterCounterCatObject.getCounterId()
                            ), true
                    );
                }
            }catch (Exception e) {
                log.error("error while updateing counter with id: {}", counterCounterCatObject.getCounterId(), e);
            }
        }

        for(var counterCatUpdate : counterCatUpdateMap.entrySet()) {
            try {
                boolean isSubCat = lstCounterCounterCatObject.stream().anyMatch(counterCounterCatObject ->
                        counterCounterCatObject.getGroupCode().equals(counterCatUpdate.getKey()) && counterCounterCatObject.isSubKpi());
                if(isSubCat) {
                    for(Constant.KpiType kpiType : Constant.KpiType.values()) {
                        createClickhouseQueueTable(String.format("%s_%s%s", counterCatUpdate.getKey(), kpiType, cacheInterval),
                                extraFieldObjectHashMap.get(counterCatUpdate.getValue()),
                                hmCounterCounterCat.values().stream().filter(
                                        counterCounterCatObject -> counterCounterCatObject.getGroupCode().equals(counterCatUpdate.getKey())
                                        && counterCounterCatObject.getKpiType() == kpiType.getValue()
                                                                                ).map(CounterCounterCatObject::getCounterCatId)
                                        .collect(Collectors.toList()), true
                                );
                    }
                }else {
                    createClickhouseQueueTable(String.format("%s%s", counterCatUpdate.getKey(),  cacheInterval),
                            extraFieldObjectHashMap.get(counterCatUpdate.getValue()),
                            hmCounterCounterCat.values().stream().filter(
                                            counterCounterCatObject -> counterCounterCatObject.getGroupCode().equals(counterCatUpdate.getKey())
                                    ).map(CounterCounterCatObject::getCounterCatId)
                                    .collect(Collectors.toList()), false
                    );
                }
            }catch (Exception e) {
                log.error("error while updating counter with id: {}", counterCatUpdate.getKey(), e);
            }
        }
    }

}
}
