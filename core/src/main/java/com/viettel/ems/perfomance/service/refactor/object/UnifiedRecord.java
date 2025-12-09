package com.viettel.ems.perfomance.service.refactor.object;

import com.viettel.ems.perfomance.common.Constant;
import lombok.Builder;
import lombok.Data;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Data
    @Builder
    public  class UnifiedRecord {
        private Timestamp recordTime;
        private Integer neId;
        private Integer duration;
        // Chứa cả ExtraField (ví dụ: cell_id) và Counter Value (ví dụ: c_101)
        private Map<String, Object> data;

        /**
         * Hàm này dùng để làm phẳng dữ liệu (Flat JSON) cho ClickHouse Kafka
         * Output ví dụ: { "record_time": 171500000, "ne_id": 101, "c_68": 10, "cell_name": "A" ... }
         */
        public Map<String, Object> toFlatMap() {
            // 1. Tạo map mới từ data gốc để tránh thay đổi tham chiếu
            Map<String, Object> flatMap = new HashMap<>();
            if (data != null) {
                flatMap.putAll(data);
            }

            // 2. Put các trường khóa chính vào (Key phải khớp với column ClickHouse)
            if (recordTime != null) {
                // ClickHouse thường nhận DateTime ở dạng String 'yyyy-MM-dd HH:mm:ss' hoặc Epoch Long (tùy config)
                // Ở đây ta gửi Long (Epoch Millis) cho an toàn hoặc String nếu bạn dùng DateTime64
                // flatMap.put("record_time", recordTime.getTime());
                // Hoặc String:
                flatMap.put("record_time", Constant.dtf.format(recordTime.toLocalDateTime()));
            }

            if (neId != null) flatMap.put("ne_id", neId);
            if (duration != null) flatMap.put("duration", duration);

            return flatMap;
        }
    }