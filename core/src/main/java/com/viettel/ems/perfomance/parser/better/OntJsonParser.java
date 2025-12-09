package com.viettel.ems.perfomance.parser.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.config.SystemConfig;
import com.viettel.ems.perfomance.object.CounterCounterCatObject;
import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.object.ExtraFieldObject;
import com.viettel.ems.perfomance.object.NEObject;
import com.viettel.ems.perfomance.object.ont.MeasCollectFileObject;
import com.viettel.ems.perfomance.object.ont.MeasDataObject;
import com.viettel.ems.perfomance.object.ont.MeasValuesObject;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

@Slf4j
public class OntJsonParser extends BaseParser {


    public OntJsonParser(Map<String, NEObject> neMap,
                         Map<Integer, CounterCounterCatObject> counterCounterCatObjectMap,
                         Map<Integer, HashMap<String, ExtraFieldObject>> efMap,
                         ObjectMapper objectMapper) {
        super(neMap, counterCounterCatObjectMap, efMap);
//        this.objectMapper = objectMapper;
    }

    @Override
    public boolean support(String fileName) {
        return fileName.toUpperCase().contains(Constant.NEType.ONT.name());
    }

    @Override
    public Map<String, List<UnifiedRecord>> parse(CounterDataObject preCounter) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();

        // Buffer để gom nhóm dữ liệu trước khi tạo UnifiedRecord
        // Key: "TableName|ExtraFieldHash" -> Map<Col, Val>
        // (Do ONT có thể có nhiều dòng cùng Table nhưng khác ExtraField trong cùng 1 file)
        Map<String, Map<String, Object>> rowBufferMap = new HashMap<>();

        // Mảng đếm LAN (UE_LAN, UE_Wifi, UE_Ethernet, Wifi_2.4G, Wifi_5G)
        // Index: 0=Total, 1=WifiTotal, 2=Ethernet, 3=2.4G, 4=5G
        long[] lanCounters = {0, 0, 0, 0, 0};

        try {
            String inputJson = new String(preCounter.getBufferData().array(), StandardCharsets.UTF_8); // Hoặc dùng decode

            // Parse JSON
            MeasCollectFileObject root = objectMapper.readValue(inputJson, MeasCollectFileObject.class);

            if (root == null || root.getMeasData() == null || root.getMeasData().isEmpty()) {
                return Collections.emptyMap();
            }

            Timestamp recordTime = Timestamp.valueOf(root.getBeginTime()); // Check format timestamp
            int duration = root.getDuration();

            // Parse NE Name từ UserLabel (Ví dụ: ...:DSNW28ce6b58)
            String[] arrUserLabel = root.getUserLabel().trim().split(":");
            if (arrUserLabel.length == 0) {
                log.warn("Wrong UserLabel format: {}", root.getUserLabel());
                return Collections.emptyMap();
            }
            String neName = arrUserLabel[arrUserLabel.length - 1].trim();

            NEObject neObj = neObjectMap.get(neName);
            if (neObj == null) {
                log.info("NE {} not found", neName);
                return Collections.emptyMap();
            }
            int neId = neObj.getId();

            // --- LOOP MEAS DATA ---
            for (MeasDataObject measData : root.getMeasData()) {
                String measTypesStr = measData.getMeasTypes(); // Chuỗi ID: "101, 102, 103..."
                List<MeasValuesObject> measValuesList = measData.getMeasValues();

                if (measTypesStr == null || measValuesList == null || measValuesList.isEmpty()) continue;

                // Logic tính toán LAN Counter (Side effect)
                calculateLanCounters(measData, measValuesList, lanCounters);

                String[] arrCounterIDs = measTypesStr.trim().split(",");

                // --- LOOP MEAS VALUES ---
                for (MeasValuesObject measValueObj : measValuesList) {
                    // 1. Parse Dimensions (Extra Fields) từ MeasObjLdn
                    // Ví dụ: "Ssid=1, Radio=0..."
                    Map<String, String> rawExtraFields = parseMeasObj(measValueObj.getMeasObjLdn());

                    // 2. Parse Values
                    String resultsStr = measValueObj.getMeasResults();
                    if (resultsStr == null) continue;

                    String[] arrValues = resultsStr.trim().split(",");

                    // 3. Map Counter ID -> Value
                    for (int i = 0; i < arrValues.length; i++) {
                        if (i >= arrCounterIDs.length) break;

                        try {
                            int counterId = Integer.parseInt(arrCounterIDs[i].trim());
                            long val = Long.parseLong(arrValues[i].trim());

                            CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
                            if (mapping != null) {
                                String tableName = mapping.getGroupCode();

                                // Tạo Key gom nhóm: Table + ExtraFields (để phân biệt các dòng khác nhau)
                                // Ví dụ: Table Wifi, SSID 1 khác với Table Wifi, SSID 2
                                String uniqueRowKey = tableName + "|" + rawExtraFields.toString();

                                Map<String, Object> rowData = rowBufferMap.computeIfAbsent(uniqueRowKey, k -> {
                                    Map<String, Object> newRow = new HashMap<>(rawExtraFields); // Copy dimensions
                                    return newRow;
                                });

                                rowData.put("c_" + counterId, val);
                            }
                        } catch (NumberFormatException e) {
                            // ignore bad data
                        }
                    }
                }
            }

            // --- PROCESS LAN COUNTERS (Add thêm các dòng tổng hợp) ---
            // LanCounter[1] = [3] + [4] (Wifi Total)
            // LanCounter[0] = [1] + [2] (LAN Total)
            lanCounters[1] = lanCounters[3] + lanCounters[4];
            lanCounters[0] = lanCounters[1] + lanCounters[2];

            // Add LAN counters vào buffer (Giả sử chúng có ID định nghĩa trong SystemConfig)
            // Cần biết TableName của các counter này để add vào đúng chỗ
            addLanCountersToBuffer(lanCounters, rowBufferMap, recordTime, neId, duration);


            // --- CONVERT BUFFER TO UNIFIED RECORD ---
            for (Map.Entry<String, Map<String, Object>> entry : rowBufferMap.entrySet()) {
                String key = entry.getKey();
                String tableName = key.split("\\|")[0];
                Map<String, Object> rawData = entry.getValue();

                // Filter & Standardize Data
                Map<String, Object> finalData = filterExtraFields(tableName, rawData);

                // Copy counter values (c_*)
                rawData.forEach((k, v) -> {
                    if (k.startsWith("c_")) finalData.put(k, v);
                });

                UnifiedRecord record = UnifiedRecord.builder()
                        .recordTime(recordTime)
                        .neId(neId)
                        .duration(duration)
                        .data(finalData)
                        .build();

                resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
            }

        } catch (Exception ex) {
            log.error("ONT JSON Parse Error", ex);
        }
        return resultMap;
    }

    // --- Helper: Tính toán LAN Counter ---
    private void calculateLanCounters(MeasDataObject measData, List<MeasValuesObject> values, long[] lanCounters) {
        // Logic business từ code cũ
        String infoName = measData.getMeasInfoName();

        // SystemConfig.UE_ETHERNET -> Index 2
        if (infoName.equalsIgnoreCase(SystemConfig.UE_ETHERNET)) { // "Ethernet Statistics"
            lanCounters[2] = values.size();
        }

        // SystemConfig.UE_WIFI -> Index 3 or 4
        if (infoName.equalsIgnoreCase(SystemConfig.UE_WIFI)) { // "Wifi Client"
            for (MeasValuesObject val : values) {
                Map<String, String> extra = parseMeasObj(val.getMeasObjLdn());
                String code = extra.get(SystemConfig.WIFI_CLIENT_CODE); // "wifi_client"

                if (code != null) {
                    if (code.equalsIgnoreCase(SystemConfig.WIFI_24GHZ)) {
                        lanCounters[3]++;
                    } else if (code.equalsIgnoreCase(SystemConfig.WIFI_5GHZ)) {
                        lanCounters[4]++;
                    }
                }
            }
        }
    }

    // --- Helper: Add LAN Counter vào Buffer ---
    private void addLanCountersToBuffer(long[] lanCounters, Map<String, Map<String, Object>> buffer,
                                        Timestamp time, int neId, int duration) {
        // Lấy ID từ config (giả sử bạn có class SystemConfig chứa các ID này)
        int[] ids = SystemConfig.LanCounterID; // [id0, id1, id2, id3, id4]

        for (int i = 0; i < lanCounters.length; i++) {
            int counterId = ids[i];
            long value = lanCounters[i];

            CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
            if (mapping != null) {
                String tableName = mapping.getGroupCode();

                // LAN counter thường không có extra field hoặc extra field rỗng -> Key đơn giản
                String rowKey = tableName + "|LAN_STATS";

                Map<String, Object> rowData = buffer.computeIfAbsent(rowKey, k -> new HashMap<>());
                rowData.put("c_" + counterId, value);
            }
        }
    }
    private HashMap<String, String> parseMeasObj(String measObjInstId) {
        HashMap<String, String> hmMeas = new HashMap<>();
        if (measObjInstId == null || measObjInstId.isEmpty()) {
            return hmMeas;
        }
        String[] arrMeas = measObjInstId.split(",");
        for (String extra : arrMeas) {
            String[] kv = extra.trim().replace("'", "\\'").split("=");
            if (kv.length != 2) {
                continue;
            }
            hmMeas.put(kv[0].toLowerCase().trim(), kv[1].trim());
        }
        return hmMeas;
    }
}