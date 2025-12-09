package com.viettel.ems.perfomance.service.refactor.repository;

import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import com.viettel.ems.perfomance.troubleshoot.DatasourceVerifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Repository
@RequiredArgsConstructor
@Slf4j
public class BetterCounterMySqlRepository {

    private final JdbcTemplate jdbcTemplate;
    private final DatasourceVerifier datasourceVerifier;

    /**
     * Insert từng dòng (Sequential Insert)
     * Thay thế hoàn toàn logic Batch/Insert Ignore cũ để debug lỗi mất dữ liệu.
     */
    public void insertSequential(Map<String, List<UnifiedRecord>> dataMap) {
        if (dataMap == null || dataMap.isEmpty()) return;


        // Duyệt qua từng bảng
        for (Map.Entry<String, List<UnifiedRecord>> entry : dataMap.entrySet()) {
            String tableName = entry.getKey();
            List<UnifiedRecord> records = entry.getValue();

            if (records == null || records.isEmpty()) continue;

            int successCount = 0;
            int duplicateCount = 0;
            int errorCount = 0;

            // Duyệt từng bản ghi trong bảng
            for (UnifiedRecord record : records) {
                try {
                    String sql = buildSingleInsertSql(tableName, record);
                    jdbcTemplate.update(sql);
                    successCount++;
                } catch (DuplicateKeyException e) {
                    duplicateCount++;
                    log.warn("Duplicate record: {}", e.getMessage());
                } catch (Exception e) {
                    // Các lỗi nghiêm trọng khác (sai kiểu dữ liệu, thiếu cột...)
                    errorCount++;
                    log.error("Error inserting into {}: Data={}. Error={}", tableName, record.getData(), e.getMessage());
                }
            }

            // Log tổng kết cho bảng này
            if (errorCount > 0 || duplicateCount > 0) {
                log.info("Table [{}]: Requested={} record(s), Inserted={}, Duplicates={}, Errors={}",
                        tableName, records.size(), successCount, duplicateCount, errorCount);
            }
        }
    }

    private String buildSingleInsertSql(String tableName, UnifiedRecord record) {
        Map<String, Object> data = record.getData();

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (record_time, ne_id, duration");

        // Sắp xếp key để đảm bảo thứ tự cột nhất quán
        Set<String> sortedKeys = new TreeSet<>();
        if (data != null) {
            sortedKeys.addAll(data.keySet());
        }

        // 1. Build Column Names
        for (String col : sortedKeys) {
            sql.append(", `").append(col).append("`");
        }
        sql.append(") VALUES (");

        // 2. Build Values
        // Fixed fields
        sql.append("'").append(record.getRecordTime()).append("', ")
                .append(record.getNeId()).append(", ")
                .append(record.getDuration());

        // Dynamic fields
        for (String col : sortedKeys) {
            sql.append(", ");
            Object val = (data != null) ? data.get(col) : null;
            sql.append(formatSqlValue(val));
        }
        sql.append(")");

        return sql.toString();
    }

    private String formatSqlValue(Object val) {
        if (val == null) return "NULL";
        if (val instanceof Number) return val.toString();
        // Escape string đơn giản (thay ' bằng \')
        return "'" + val.toString().replace("'", "\\'") + "'";
    }
}