package com.viettel.util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class SqlLogger {
    private static final Logger log = LoggerFactory.getLogger(SqlLogger.class);

    public static void logSqlList(List<String> sqlList) {
        // Kiểm tra xem log level INFO có được bật không để tránh xử lý không cần thiết
        if (!log.isInfoEnabled()) {
            return;
        }

        if (sqlList == null || sqlList.isEmpty()) {
            log.info("No SQL generated.");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n=====================================\n");
        sb.append("      SQL GENERATED (").append(sqlList.size()).append(" statements)\n");
        sb.append("=====================================\n");

        int i = 1;
        for (String sql : sqlList) {
            sb.append("\n--- SQL #").append(i++).append(" ---\n");
            sb.append(sql);
            // Thêm dấu chấm phẩy nếu nó chưa có
            if (!sql.trim().endsWith(";")) {
                sb.append(";");
            }
            sb.append("\n");
        }

        sb.append("=====================================\n");

        log.info(sb.toString());
    }
}