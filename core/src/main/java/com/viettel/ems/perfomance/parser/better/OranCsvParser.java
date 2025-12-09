package com.viettel.ems.perfomance.parser.better;

import com.viettel.ems.perfomance.object.*;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class OranCsvParser extends BaseParser{


    private final int RX_TX_WINDOW_TRANSPORT_LENGTH = 8;
    private final int TRANSCEIVER_LENGTH = 28;
    private final int EPE_LENGTH = 8;
    private final int FILE_NAME_LENGTH = 7;
    private final int START_INDEX = 4;


    private final Map<String, CounterConfigObject> counterConfigObjectMap;

    public OranCsvParser(Map<String, NEObject> neObjectMap, Map<Integer,
            CounterCounterCatObject> counterCounterCatObjectMap, Map<Integer,
            HashMap<String, ExtraFieldObject>> extraFieldMap, Map<String, CounterConfigObject> counterConfigObjectMap) {
        super(neObjectMap, counterCounterCatObjectMap, extraFieldMap);
        this.counterConfigObjectMap = counterConfigObjectMap;
    }

    @Override
    public boolean support(String fileName) {
        return fileName.contains("_GNODEB_") && fileName.contains("RU");
    }

    @Override
    public Map<String, List<UnifiedRecord>> parse(CounterDataObject dataObject) {
        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();
        try {
            String fileName = dataObject.getFileName();
            String[] lstFileName = fileName.split("_");

            if (lstFileName.length != FILE_NAME_LENGTH) {
                log.warn("File {}: Wrong format!", fileName);
                return resultMap;
            }

            String neName = lstFileName[3].trim();
            String ruIndex = lstFileName[5].trim();
            NEObject neObj = neObjectMap.get(neName);

            if (neObj == null) {
                log.warn("NE {} not found or disabled in file: {}", neName, fileName);
                return resultMap;
            }
            int neId = neObj.getId();

            List<String> lstLineData =  dataObject.getLineData();
            if (lstLineData == null || lstLineData.isEmpty()) {
                log.warn("File: {} Data is empty!", fileName);
                return resultMap;
            }

            for (String line : lstLineData) {
                String[] lineValues = line.split(",");
                if (lineValues.length < 4) continue;

                String measureKey = lineValues[0].trim() + "_" + lineValues[1].trim().toUpperCase();

                // Parse Timestamp
                Timestamp startTime = parseTimestamp(lineValues[2]);
                Timestamp endTime = parseTimestamp(lineValues[3]);
                int duration = (int) ((endTime.getTime() - startTime.getTime()) / 1000);

                CounterConfigObject config = counterConfigObjectMap.get(measureKey);
                if (config == null) {
                    // log.debug("{} does not exist!", measureKey);
                    continue;
                }

                if (!validateLineFormat(config.getMeasurementGroup(), lineValues.length)) {
                    continue;
                }

                // Parse Line -> UnifiedRecord Map
                Map<String, List<UnifiedRecord>> lineRecords = parseCounterOranLine(
                        config, startTime, neId, ruIndex, duration, lineValues
                );

                // Merge results
                lineRecords.forEach((table, records) ->
                        resultMap.computeIfAbsent(table, k -> new ArrayList<>()).addAll(records)
                );
            }
        } catch (Exception e) {
            log.error("CSV Parse Error", e);
            throw new RuntimeException(e);
        }
        return resultMap;
    }

    private Map<String, List<UnifiedRecord>> parseCounterOranLine(
            CounterConfigObject config, Timestamp time, int neId, String ruIndex, int duration, String[] values) {

        Map<String, List<UnifiedRecord>> result = new HashMap<>();
        HashMap<Integer, LiteCounterObject> measurements = config.getHmLiteCounter();

        // Map gom nhóm. Key phải bao gồm cả SFP Index để phân biệt dòng
        // Key: "TableName|SfpIndex"
        Map<String, Map<String, Object>> rowBufferMap = new HashMap<>();

        for (int idx = START_INDEX; idx < values.length; idx++) {
            String valStr = values[idx].trim();
            if ("-".equals(valStr) || valStr.isEmpty()) continue;

            LiteCounterObject liteCounter = measurements.get(idx);
            if (liteCounter == null) continue;

            int counterId = liteCounter.getId();
            CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);

            if (mapping != null) {
                try {
                    long value = Long.parseLong(valStr);
                    String tableName = mapping.getGroupCode();

                    // Lấy sfp_index của cột này. Ví dụ: "1", "2", hoặc "-1"
                    String sfpIndex = getSfpIndex(liteCounter.getName().toLowerCase());

                    // [MIGRATION LOGIC]: Code cũ dùng sExtraField để phân tách dòng.
                    // sExtraField thay đổi khi sfpIndex thay đổi.
                    // => Ta phải dùng (TableName + sfpIndex) làm khóa phân tách.
                    String compositeKey = tableName + "|" + sfpIndex;

                    Map<String, Object> rowData = rowBufferMap.computeIfAbsent(compositeKey, k -> {
                        Map<String, Object> newRow = new HashMap<>();
                        // Các field chung
                        newRow.put("ru_index", ruIndex);
                        // Field riêng phân biệt dòng
                        if (!"-1".equals(sfpIndex)) {
                            newRow.put("sfp_index", sfpIndex);
                        }
                        return newRow;
                    });

                    rowData.put("c_" + counterId, value);

                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }

        // Convert map buffer thành List UnifiedRecord
        for (Map.Entry<String, Map<String, Object>> entry : rowBufferMap.entrySet()) {
            String compositeKey = entry.getKey();
            String tableName = compositeKey.split("\\|")[0]; // Lấy lại tên bảng
            Map<String, Object> rawData = entry.getValue();

            // Filter extra field để loại bỏ các field thừa (giống logic cũ)
            Map<String, Object> finalData = filterExtraFields(tableName, rawData);

            // Put lại counter values vào finalData
            rawData.forEach((k, v) -> {
                if (k.startsWith("c_")) finalData.put(k, v);
            });

            UnifiedRecord record = UnifiedRecord.builder()
                    .recordTime(time)
                    .neId(neId)
                    .duration(duration)
                    .data(finalData)
                    .build();

            result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
        }

        return result;
    }



    private Timestamp parseTimestamp(String s) {
        String clean = s.trim().replace("T", " ").replace("Z", "");
        return Timestamp.valueOf(clean);
    }

    private boolean validateLineFormat(String group, int length) {
        if ("tx-measurement-objects".equalsIgnoreCase(group) || "rx-window-stats".equalsIgnoreCase(group)) {
            return length == RX_TX_WINDOW_TRANSPORT_LENGTH;
        } else if ("transceiver-stats".equalsIgnoreCase(group)) {
            return length == TRANSCEIVER_LENGTH;
        } else if ("epe-stats".equalsIgnoreCase(group)) {
            return length == EPE_LENGTH;
        }
        log.warn("Line is wrong format: {}", length);
        return false;
    }


}
