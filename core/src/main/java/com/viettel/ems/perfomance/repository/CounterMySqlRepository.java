package com.viettel.ems.perfomance.repository;

import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.common.ErrorCode;
import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.config.TenantContextHolder;
import com.viettel.ems.perfomance.object.CounterObject;
import com.viettel.ems.perfomance.object.LiteExtraFieldObject;
import com.viettel.ems.perfomance.troubleshoot.DatasourceVerifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

@Repository
@RequiredArgsConstructor
@Slf4j
public class CounterMySqlRepository {

    private final JdbcTemplate jdbcTemplate;
    private final PlatformTransactionManager transactionManager;
    private final DatasourceVerifier datasourceVerifier;
    private final ConfigManager configManager;

    public ErrorCode addCounter(List<CounterObject> lstCounter) {
        SystemType currentSystem = TenantContextHolder.getCurrentSystem();
        datasourceVerifier.verifyConnection("Verifying context DB before building query insert");
        boolean batchInsertEnabled = configManager.getCustomBoolean(currentSystem, "batchInsertCounter");
        log.debug("Batch insert ?{}", batchInsertEnabled);
        if(batchInsertEnabled) {
            return addCounterInBatch(lstCounter);
        }else {
            return addCounterSingle(lstCounter);
        }
    }



    private ErrorCode addCounterSingle(List<CounterObject> lstCounter) {

            try {
                ErrorCode result = ErrorCode.NO_ERROR;
                List<String> arrSQL = new ArrayList<>();
                datasourceVerifier.verifyConnection();


                if(lstCounter == null || lstCounter.isEmpty()) {
                    return ErrorCode.ERROR_UNKNOWN;
                }

                lstCounter.sort(Comparator.comparing(CounterObject::getGroupCode)
                        .thenComparing(CounterObject::getTime)
                        .thenComparing(CounterObject::getDuration)
                        .thenComparing(CounterObject::getSExtraField)
                );

                String currentGroupCode = "";
                Timestamp currentRecordTime = null;
                int currentDuration = -1;
                String currentSExtraField = "";
                Map<String, LiteExtraFieldObject> currentExtraField = null;
                StringBuilder sbSQL = new StringBuilder();
                StringBuilder sbValue = new StringBuilder();
                StringBuilder sbFull = new StringBuilder();

                int countAdd = 0 ;
                boolean isFirst = true;
                for(int i = 0;i < lstCounter.size();i++) {
                    CounterObject counterObject = lstCounter.get(i);
                    if(isFirst) {
                        currentGroupCode = counterObject.getGroupCode();
                        currentRecordTime = counterObject.getTime();
                        currentDuration = counterObject.getDuration();
                        currentSExtraField = counterObject.getSExtraField();
                        currentExtraField = counterObject.getExtraField();

                        sbSQL.append("INSERT INTO ").append(currentGroupCode)
                                .append("(record_time, duration, ne_id");
                        // add extra field
                        StringBuilder sbExtraField = new StringBuilder();
                        currentExtraField.forEach((k, v) -> {
                            sbExtraField.append(", ").append(k);
                        });
                        sbSQL.append(sbExtraField);
                        isFirst = false;
                    }

                    // groupCode, recordTime, duration ,extra field
                    if(currentGroupCode.equals(counterObject.getGroupCode()) && currentRecordTime.compareTo(counterObject.getTime()) ==0
                            && currentDuration == counterObject.getDuration() && currentSExtraField.equals(counterObject.getSExtraField())
                    ) {
                        // add column name
                        sbSQL.append(", c_").append(counterObject.getCounterId());
                        sbValue.append(countAdd == 0 ? counterObject.getCounterValue() : ", " + counterObject.getCounterValue());
                        countAdd++;
                    }else if(!currentGroupCode.equals(counterObject.getGroupCode()) || currentRecordTime.compareTo(counterObject.getTime()) != 0

                            || currentDuration != counterObject.getDuration() || !currentSExtraField.equals(counterObject.getSExtraField())
                    ) {
                        sbSQL.append(") VALUES");
                        // basic info: record_time, duration, ne_id
                        sbFull.setLength(0);
                        sbFull.append(sbSQL)
                                .append(" (")
                                .append(timestampToText(currentRecordTime)).append(", ")
                                .append(currentDuration).append(", ")
                                .append(counterObject.getNeId()).append(", ");

                        sbFull.append(getExtraField(currentExtraField));

                        // counter value
                        sbFull.append(sbValue.toString()).append(");");
                        arrSQL.add(sbFull.toString());
                        // new record
                        sbSQL.setLength(0);
                        sbValue.setLength(0);
                        currentGroupCode = counterObject.getGroupCode();
                        currentRecordTime = counterObject.getTime();
                        currentDuration = counterObject.getDuration();
                        currentSExtraField = counterObject.getSExtraField();
                        currentExtraField = counterObject.getExtraField();
                        //
                        sbSQL.append("INSERT INTO ").append(currentGroupCode)
                                .append("(record_time, duration, ne_id");
                        // add extra field
                        StringBuilder sbExtraField = new StringBuilder();
                        currentExtraField.forEach((k, v) -> {
                            sbExtraField.append(", ").append(k);
                        });
//                        currentExtraField.forEach((code, lteEFO) -> {
//                            sbExtraField.append(", ").append(lteEFO);
//                        });
                        sbSQL.append(sbExtraField);
                        //
                        sbSQL.append(", c_").append(counterObject.getCounterId());
                        sbValue.append(counterObject.getCounterValue());
                        countAdd = 1;
                    }
                    if(i >= lstCounter.size() -1 ) {
                        // the last record
                        sbSQL.append(") VALUES");
                        // basic info: record_time, duration, ne_id
                        sbFull.setLength(0);
                        sbFull.append(sbSQL.toString())
                                .append(" (")
                                .append(timestampToText(currentRecordTime)).append(", ")
                                .append(currentDuration).append(", ")
                                .append(counterObject.getNeId()).append(", ");

                        // extra info: cell, tac, lac...
                        sbFull.append(getExtraField(currentExtraField));
                        // counter value
                        sbFull.append(sbValue.toString()).append(");");
                        arrSQL.add(sbFull.toString());
                    }
                }           ;
                if(arrSQL.isEmpty()) {
                    return ErrorCode.ERROR_UNKNOWN;
                }
                for(String sqlInsert : arrSQL) {
                    result = executeQuery(sqlInsert, 0);
                    log.debug("Insert data: {}, result : {}", sqlInsert, result);
                }
                return result;
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
                return ErrorCode.NO_ERROR; // Use existing error code
            }
        }






    private String getExtraField(Map<String, LiteExtraFieldObject> currentExtraField) {
        StringBuilder sbExtraField = new StringBuilder();
        try {
            currentExtraField.forEach((code, lteEFO) -> {
                String columnCode = code;
                String columnType = lteEFO.getColumnType();
                String columnValue = lteEFO.getColumnValue();
                if ("string".equalsIgnoreCase(columnType)) {
                    sbExtraField.append(getSingleQuotedText(columnValue)).append(", ");
                } else if ("datetime".equalsIgnoreCase(columnType) || "date".equalsIgnoreCase(columnType) || "time".equalsIgnoreCase(columnType)) {
                    sbExtraField.append("null, ");
                } else {
                    sbExtraField.append(columnValue).append(", ");
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
        return sbExtraField.toString();
    }

    //  ========================== batching version ================================
    private ErrorCode addCounterInBatch(List<CounterObject> lstCounter) {
        try {
            if (lstCounter == null || lstCounter.isEmpty()) {
                return ErrorCode.ERROR_UNKNOWN;
            }

            // PHASE 1: Group counters by table (groupCode)
            // Example: {pm_cell: [c1, c2, c3], pm_node: [c4, c5]}
            Map<String, List<CounterObject>> countersByTable = lstCounter.stream()
                    .collect(Collectors.groupingBy(CounterObject::getGroupCode));

            // PHASE 2: Choose execution strategy based on number of tables
            if (countersByTable.size() > 1) {
                // Multiple tables → Use batch insert with transaction
                return executeBatchInTransaction(countersByTable);
            } else {
                // Single table → Use sequential insert with transaction
                return executeSequentialInsert(countersByTable);
            }

        } catch (Exception ex) {
            log.error("Error in addCounter: {}", ex.getMessage(), ex);
            return ErrorCode.ERROR_UNKNOWN;
        }
    }




    private ErrorCode executeSequentialInsert(Map<String, List<CounterObject>> countersByTable) {
        // Manual transaction management (same as executeParallelInsert)
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        TransactionStatus status = transactionManager.getTransaction(def);

        try {
            for (Map.Entry<String, List<CounterObject>> entry : countersByTable.entrySet()) {
                String sql = buildMultiRowInsertSQL(entry.getKey(), entry.getValue());
                ErrorCode result = executeQuery(sql, 0 );
                if (result != ErrorCode.NO_ERROR) {
                    // Rollback on error
                    transactionManager.rollback(status);
                    log.error("Insert failed for table {}, rolling back", entry.getKey());
                    return result;
                }
            }

            // All succeeded → commit
            transactionManager.commit(status);
            log.debug("Single table inserted successfully, transaction committed");
            return ErrorCode.NO_ERROR;

        } catch (Exception e) {
            // Rollback on exception
            transactionManager.rollback(status);
            log.error("Exception during insert, transaction rolled back", e);
            return ErrorCode.ERROR_UNKNOWN;
        }
    }


    private ErrorCode executeBatchInTransaction(Map<String, List<CounterObject>> countersByTable) {
        // Manual transaction management for true atomicity
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        TransactionStatus status = transactionManager.getTransaction(def);

        try {
            // Execute sequentially within ONE transaction
            for (Map.Entry<String, List<CounterObject>> tableEntry : countersByTable.entrySet()) {
                String sql = buildMultiRowInsertSQL(tableEntry.getKey(), tableEntry.getValue());
                ErrorCode result = executeQuery(sql, 0 );
                if (result != ErrorCode.NO_ERROR) {
                    // Rollback on first error
                    transactionManager.rollback(status);
                    log.error("Insert failed for table {}, rolling back all", tableEntry.getKey());
                    return result;
                }
            }

            // All succeeded → commit
            transactionManager.commit(status);
            log.debug("All tables inserted successfully, transaction committed");
            return ErrorCode.NO_ERROR;

        } catch (Exception e) {
            // Rollback on exception
            transactionManager.rollback(status);
            log.error("Exception during insert, transaction rolled back", e);
            return ErrorCode.ERROR_UNKNOWN;
        }
    }


    private String buildMultiRowInsertSQL(String tableName, List<CounterObject> counters) {
        if (counters == null || counters.isEmpty()) {
            return null;
        }

        // PHASE 1: Group counters by RowKey
        // Counters with same (time, duration, ne_id, extra_field) → Same row
        Map<RowKey, List<CounterObject>> countersByRow = counters.stream()
                .collect(Collectors.groupingBy(counter ->
                    new RowKey(
                        counter.getTime(),
                        counter.getDuration(),
                        counter.getNeId(),
                        counter.getSExtraField(),
                        counter.getExtraField()
                    )
                ));

        // PHASE 2: Collect all unique counter IDs
        // Need columns for ALL counters that appear in ANY row
        Set<Integer> allCounterIds = counters.stream()
                .map(CounterObject::getCounterId)
                .collect(Collectors.toSet());

        // Collect ALL unique extra field keys from ALL counters (not just first one!)
        // CRITICAL: Different counters may have different extra fields
        // Example: counter[0] has {cell}, counter[1] has {cell, tac} → Must include BOTH
        Set<String> allExtraFieldKeys = new HashSet<>();
        for (CounterObject counter : counters) {
            if (counter.getExtraField() != null) {
                allExtraFieldKeys.addAll(counter.getExtraField().keySet());
            }
        }

        // Sort keys for consistent column order (prevent data corruption)
        List<String> sortedExtraFieldKeys = new ArrayList<>(allExtraFieldKeys);
        Collections.sort(sortedExtraFieldKeys);

        StringBuilder sql = new StringBuilder();

        // PHASE 3A: Build column list
        sql.append("INSERT INTO ").append(tableName)
           .append(" (record_time, duration, ne_id");

        // Add extra field columns in SORTED order
        for (String columnName : sortedExtraFieldKeys) {
            sql.append(", ").append(columnName);
        }

        // Add counter columns (c_1, c_2, c_3, ...) - sorted for consistency
        List<Integer> sortedCounterIds = new ArrayList<>(allCounterIds);
        Collections.sort(sortedCounterIds);
        for (Integer counterId : sortedCounterIds) {
            sql.append(", c_").append(counterId);
        }

        sql.append(") VALUES ");

        // PHASE 3B: Build VALUES rows
        List<String> valueRows = new ArrayList<>();
        for (Map.Entry<RowKey, List<CounterObject>> rowEntry : countersByRow.entrySet()) {
            RowKey rowKey = rowEntry.getKey();
            List<CounterObject> rowCounters = rowEntry.getValue();

            // Map: counterId → value for this specific row
            Map<Integer, Long> counterValues = rowCounters.stream()
                    .collect(Collectors.toMap(
                        CounterObject::getCounterId,
                        CounterObject::getCounterValue
                    ));

            StringBuilder rowValue = new StringBuilder("(");

            // Add fixed fields: record_time, duration, ne_id
            rowValue.append(timestampToText(rowKey.recordTime)).append(", ")
                    .append(rowKey.duration).append(", ")
                    .append(rowKey.neId);

            // Add extra field values in SAME SORTED order as columns
            // CRITICAL: Must use sortedExtraFieldKeys to match column order
            for (String key : sortedExtraFieldKeys) {
                LiteExtraFieldObject fieldObj = rowKey.extraField.get(key);
                if (fieldObj != null) {
                    String columnType = fieldObj.getColumnType();
                    String columnValue = fieldObj.getColumnValue();

                    rowValue.append(", ");
                    if ("string".equalsIgnoreCase(columnType)) {
                        rowValue.append(getSingleQuotedText(columnValue));
                    } else if ("datetime".equalsIgnoreCase(columnType) ||
                               "date".equalsIgnoreCase(columnType) ||
                               "time".equalsIgnoreCase(columnType)) {
                        rowValue.append("null");
                    } else {
                        rowValue.append(columnValue);
                    }
                } else {
                    rowValue.append(", null");
                }
            }

            // Add counter values (in same order as column list)
            // If counter doesn't exist for this row → NULL
            // IMPORTANT: value=0 is VALID, only use NULL when counter is missing
            for (Integer counterId : sortedCounterIds) {
                Long value = counterValues.get(counterId);
                rowValue.append(", ").append(value != null ? value : "NULL");
            }

            rowValue.append(")");
            valueRows.add(rowValue.toString());
        }

        sql.append(String.join(", ", valueRows));
        sql.append(";");

        return sql.toString();
    }


    private static class RowKey {
        final Timestamp recordTime;
        final int duration;
        final int neId;
        final String sExtraField;
        final Map<String, LiteExtraFieldObject> extraField;

        RowKey(Timestamp recordTime, int duration, int neId,
               String sExtraField, Map<String, LiteExtraFieldObject> extraField) {
            this.recordTime = recordTime;
            this.duration = duration;
            this.neId = neId;
            this.sExtraField = sExtraField;
            this.extraField = extraField;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowKey rowKey = (RowKey) o;
            return duration == rowKey.duration &&
                   neId == rowKey.neId &&
                   Objects.equals(recordTime, rowKey.recordTime) &&
                   Objects.equals(sExtraField, rowKey.sExtraField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordTime, duration, neId, sExtraField);
        }
    }




    private ErrorCode executeQuery(String sqlInsert, int retry) {
        ErrorCode result = ErrorCode.NO_ERROR;
        try {
            datasourceVerifier.verifyConnection();
            jdbcTemplate.update(sqlInsert);
        }catch (Exception e) {
            log.error(e.getMessage(), e);
            if(e.getMessage().contains("Unknown column") && e.getMessage().contains("field list")) {
                if(addColumn(sqlInsert) && retry < Constant.MAX_RETRY_DB) {
                    result = executeQuery(sqlInsert, retry + 1);
                }else if(e.getMessage().contains("Duplicate entry")) {
                    result = ErrorCode.ERROR_DUPLICATE_RECORD;
                }
            }
        }
        return result;
    }




    private boolean addColumn(String input) {
        try {
            String tableNameSQL = "";
            List<String> lstColumnSQL = new ArrayList<>();
            List<String> lstColumnDB;
            List<String> lstColumnMiss = new ArrayList<>();
            // get column name from string SQL
            String regexColumn = "(c_\\d+)";
            Pattern pattern = Pattern.compile(regexColumn);
            Matcher matcher = pattern.matcher(input);
            while (matcher.find()) {
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    lstColumnSQL.add(matcher.group(i));
                }
            }
            // get table name from string sql
            String regexTable = "INSERT INTO (.+?)\\(";
            Pattern patternTable = Pattern.compile(regexTable);
            Matcher matcherTable = patternTable.matcher(input);
            while (matcherTable.find()) {
                for (int i = 1; i <= matcherTable.groupCount(); i++) {
                    tableNameSQL = matcherTable.group(i);
                }
            }
            if (lstColumnSQL.isEmpty() || tableNameSQL.isBlank()) {
                return false;
            }
            // get column in table
            lstColumnDB = jdbcTemplate.query(" SHOW COLUMNS FROM " + tableNameSQL + " like 'c\\_%;'", (rs, rowNum) -> rs.getString("Field"));
            if (lstColumnDB.isEmpty()) {
                return false;
            }
            lstColumnSQL.forEach(item -> {
                if (!lstColumnDB.contains(item)) {
                    lstColumnMiss.add(item);
                }
            });
            if (lstColumnMiss.isEmpty()) {
                return false;
            }
            int sizeColumnMiss = lstColumnMiss.size();
            StringBuilder sqlAddColumn = new StringBuilder("ALTER TABLE ");
            sqlAddColumn.append(tableNameSQL);
            for (int i = 0; i < sizeColumnMiss - 1; i++) {
                sqlAddColumn.append(" ADD").append(lstColumnMiss.get(i)).append(" bigint(15) default null, ");

            }
            sqlAddColumn.append(" ADD ").append(lstColumnMiss.get(sizeColumnMiss - 1)).append(" bigint(15) default null;");
            log.debug("INSERT new counter: {}", sqlAddColumn.toString());
            jdbcTemplate.execute(String.valueOf(sqlAddColumn));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
        return true;
    }


    private String timestampToText(Timestamp val) {
        if (val == null) return "null";
        Calendar date = new GregorianCalendar();
        date.setTime(new Date(val.getTime()));
        String strFormat = "'%1$d-%2$d-%3$d %4$d:%5$d:%6$d.%7$03d'";
        return String.format(strFormat, date.get(Calendar.YEAR), date.get(Calendar.MONTH) + 1,
                date.get(Calendar.DAY_OF_MONTH), date.get(Calendar.HOUR_OF_DAY), date.get(Calendar.MINUTE),
                date.get(Calendar.SECOND), date.get(Calendar.MILLISECOND));
    }


    private String getSingleQuotedText(String s) {
        return (s != null && !s.isEmpty()) ? "'" + s + "'" : null;
    }
}
