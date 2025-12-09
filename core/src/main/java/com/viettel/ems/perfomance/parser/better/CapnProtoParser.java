package com.viettel.ems.perfomance.parser.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.config.TenantContextHolder;
import com.viettel.ems.perfomance.object.*;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import com.viettel.ems.perfomance.tools.CounterSchema;
import lombok.extern.slf4j.Slf4j;
import org.capnproto.MessageReader;
import org.capnproto.ReaderOptions;
import org.capnproto.Serialize;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;

@Slf4j
public class CapnProtoParser extends BaseParser{

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final ReaderOptions readerOptions = new ReaderOptions(1024L * 1024L * 8L * 1000L, 128);

    public CapnProtoParser(Map<String, NEObject> neObjectMap, Map<Integer,
            CounterCounterCatObject> counterCounterCatObjectMap,
                           Map<Integer, HashMap<String, ExtraFieldObject>> extraFieldMap) {
        super(neObjectMap, counterCounterCatObjectMap, extraFieldMap);
    }

    @Override
    public boolean support(String fileName) {
        return fileName.contains(Constant.NEType.GNODEB.name());
    }

    @Override
    public Map<String, List<UnifiedRecord>> parse(CounterDataObject dataObject) {
        String fileFullPath = dataObject.getPath() + "/" + dataObject.getFileName();
        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();

        // List dùng để lưu snapshot dữ liệu gốc từ file (để đối chứng)
        List<Map<String, Object>> rawInputSnapshot = new ArrayList<>();

        try {
            ByteBuffer capMsg = dataObject.getBufferData();
            capMsg.rewind();
            MessageReader rd = Serialize.read(capMsg, readerOptions);
            CounterSchema.CounterDataCollection.Reader root = rd.getRoot(CounterSchema.CounterDataCollection.factory);

            String neName = root.getUnit().toString();
            NEObject neObj = neObjectMap.get(neName);
            if (neObj == null) {
                log.warn("NodeName {} not found. Ignoring file {}", neName, fileFullPath);
                return Collections.emptyMap();
            }
            int neId = neObj.getId();

            // --- LOOP BLOCKS ---
            for (CounterSchema.CounterData.Reader r : root.getData()) {
                // 1. Lấy dữ liệu thô
                long timeRaw = r.getTime();
                Timestamp recordTime = new Timestamp(timeRaw);
                int duration = r.getDuration();
                String location = r.getLocation().toString().trim();
                long cellIndex = r.getCell();

                // Snapshot: Lưu thông tin header của block
                Map<String, Object> blockSnapshot = new HashMap<>();
                blockSnapshot.put("time", recordTime.toString());
                blockSnapshot.put("location", location);
                blockSnapshot.put("cell", cellIndex);
                List<String> rawMetrics = new ArrayList<>(); // Lưu dạng "ID:Value" cho gọn

                // 2. Xử lý Logic
                Map<String, Object> dimMap = parseDimensions(location, cellIndex);
                Map<String, Map<String, Object>> tableBuffer = new HashMap<>();

                // --- LOOP METRICS ---
                for (CounterSchema.CounterValue.Reader val : r.getData()) {
                    int counterId = val.getId();
                    double value = val.getValue(); // Giả sử là double

                    // Snapshot: Lưu cặp ID-Value gốc
                    rawMetrics.add(counterId + ":" + value);

                    CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
                    if (mapping != null) {
                        String tableName = mapping.getGroupCode();
                        tableBuffer.computeIfAbsent(tableName, k -> new HashMap<>())
                                .put("c_" + counterId, value);
                    } else {
                        log.warn("[{}] Counter ID-Value: ({}-{}) is out of scope! it can be a new counter, file: {}",
                                TenantContextHolder.getCurrentSystem().name(), counterId, value,
                                fileFullPath);                    }
                }

                // Đưa list metric thô vào snapshot của block
                blockSnapshot.put("raw_metrics", rawMetrics);
                rawInputSnapshot.add(blockSnapshot);

                // 3. Đóng gói Result (Code cũ)
                for (Map.Entry<String, Map<String, Object>> entry : tableBuffer.entrySet()) {
                    String tableName = entry.getKey();
                    Map<String, Object> metrics = entry.getValue();
                    Map<String, Object> finalData = filterExtraFields(tableName, dimMap);
                    finalData.putAll(metrics);

                    UnifiedRecord record = UnifiedRecord.builder()
                            .recordTime(recordTime)
                            .neId(neId)
                            .duration(duration)
                            .data(finalData)
                            .build();

                    resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
                }
            }

            // =========================================================================
            // LOG AUDIT: In ra 1 dòng JSON duy nhất chứa cả INPUT GỐC và OUTPUT KẾT QUẢ
            // =========================================================================
                Map<String, Object> auditLog = new HashMap<>();
                auditLog.put("file_name", fileFullPath);
                auditLog.put("ne_name", neName);
                auditLog.put("INPUT_RAW_SNAPSHOT", rawInputSnapshot); // Dữ liệu đọc được từ file
                auditLog.put("OUTPUT_PARSED_RESULT", resultMap);      // Dữ liệu sau khi mapping

                log.debug("AUDIT_TRACE: {}", objectMapper.writeValueAsString(auditLog));

        } catch (Exception ex) {
            log.error("CapnProto parsing error: {}", ex.getMessage());
            throw new RuntimeException(ex);
        }
        return resultMap;
    }

    /**
     * Parse Location String thành Map các dimensions
     */
    private Map<String, Object> parseDimensions(String location, long cellIndex) {
        Map<String, Object> dims = new HashMap<>();
        // Parse chuỗi location "k1=v1,k2=v2"
        if (location != null && !location.isEmpty()) {
            String[] pairs = location.split(",");
            for (String pair : pairs) {
                String[] kv = pair.trim().split("=");
                if (kv.length == 2) {
                    dims.put(kv[0].toLowerCase().trim(), kv[1].trim());
                }
            }
            dims.put("location", location);
        }

        // Thêm các field tính toán
        dims.put("cell_index", cellIndex);

        // Rat Type logic
        String nodeFunction = (String) dims.get("nodefunction");
        String ratType = RatType.fromNodeFunction(nodeFunction);
        dims.put("rat_type", ratType);

        if (dims.containsKey("cellname")) {
            dims.put("cell_name", dims.get("cellname"));
        }

        return dims;
    }



}
