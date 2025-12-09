//package com.viettel.ems.perfomance.parser;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import com.viettel.ems.perfomance.common.Constant;
//import com.viettel.ems.perfomance.config.SystemConfig;
//import com.viettel.ems.perfomance.config.TenantContextHolder;
//import com.viettel.ems.perfomance.object.*;
//import com.viettel.ems.perfomance.object.ont.MeasCollectFileObject;
//import com.viettel.ems.perfomance.object.ont.MeasDataObject;
//import com.viettel.ems.perfomance.object.ont.MeasValuesObject;
//import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
//import com.viettel.ems.perfomance.tools.CounterSchema;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.capnproto.MessageReader;
//import org.capnproto.ReaderOptions;
//import org.capnproto.Serialize;
//
//import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
//import java.sql.Timestamp;
//import java.util.*;
//
//@Slf4j
//@Data
//@NoArgsConstructor
//public class BetterParserCounter {
//    private static final ReaderOptions readerOptions = new ReaderOptions(1024L * 1024L * 8L * 1000L, 128);
//    private Map<String, NEObject> neObjectMap = new HashMap<>();
//    private Map<Integer, CounterCounterCatObject> counterCounterCatObjectMap = new HashMap<>();
//    private Map<Integer, HashMap<String, ExtraFieldObject>> extraFieldObjectMap = new HashMap<>();
//
//    private HashMap<String, CounterConfigObject> counterConfigObjectMap = new HashMap<>();
//
//    private final int RX_TX_WINDOW_TRANSPORT_LENGTH = 8;
//    private final int TRANSCEIVER_LENGTH = 28;
//    private final int EPE_LENGTH = 8;
//    private final int FILE_NAME_LENGTH = 7;
//    private final int START_INDEX = 4;
//
//    public BetterParserCounter(HashMap<String, NEObject> neObjectMap,
//                               Map<Integer, CounterCounterCatObject> counterCounterCatObjectMap,
//                               Map<Integer, HashMap<String, ExtraFieldObject>> extraFieldObjectMap,
//                               HashMap<String, CounterConfigObject> counterConfigObjectMap
//                             ) {
//        this.neObjectMap = neObjectMap;
//        this.counterCounterCatObjectMap = counterCounterCatObjectMap;
//        this.extraFieldObjectMap = extraFieldObjectMap;
//        this.counterConfigObjectMap = counterConfigObjectMap;
//    }
//
//
//
//    public Map<String, List<UnifiedRecord>> parseCounter(CounterDataObject preCounter) {
//        try {
//            String fileName = preCounter.getFileName().toUpperCase();
//            //gNodeB_gHI04305_1616233232_2332_4fdsfasasdf           --> Access(gNodeB)
//            //A20210323.0953+2323-2332+2323_SMF_-_SMF01              --> Core (AMF, SMF, DMF, UMF...)
//            if (fileName.contains(Constant.NEType.GNODEB.name())) {
//                if (fileName.contains("_GNODEB_") && fileName.contains("RU")) {
//                    return parseCounter5GAccessCSV(preCounter);
//                } else if (fileName.contains("_GNODEB_")) {
//                    return parseCounter5GCoreProtobuf(preCounter);
//                } else {
//                    return parseCounter45GAccessCapnProto(preCounter);
//                }
//            } else if (fileName.contains(Constant.NEType.AMF.name()) || fileName.contains(Constant.NEType.SMF.name())
//                    || fileName.contains(Constant.NEType.UPF.name()) || fileName.contains(Constant.NEType.NRF.name())
//                    || fileName.contains(Constant.NEType.NSSF.name()) || fileName.contains(Constant.NEType.MSC.name())
//                    || fileName.contains(Constant.NEType.UDM.name()) || fileName.contains(Constant.NEType.MME.name())
//                    || fileName.contains(Constant.NEType.SGWC.name()) || fileName.contains(Constant.NEType.IMS.name())
//                    || fileName.contains(Constant.NEType.NEF.name()) || fileName.contains(Constant.NEType.NWDAF.name())
//                    || fileName.contains(Constant.NEType.AUSF.name()) || fileName.contains(Constant.NEType.UDR.name())
//                    || fileName.contains(Constant.NEType.PCF.name()) || fileName.contains(Constant.NEType.GMLC.name())
//                    || fileName.contains(Constant.NEType.CHF.name()) || fileName.contains(Constant.NEType.LMF.name())
//                    || fileName.contains(Constant.NEType.AF.name()) || fileName.contains(Constant.NEType.DN.name())
//                    ) {
//                return parseCounter5GCoreProtobuf(preCounter);
//            }
//            else if (
//                    fileName.contains(Constant.NEType.ONT.name())
//            ) {
//                return parseCounterONTJSON(preCounter);
//            } else {
//                log.warn("⚠️ Unrecognized or unsupported file format: {}", fileName);
//                return Collections.emptyMap();
//            }
//
//        } catch (Exception ex) {
//            log.error("❌ Error parsing counter data from file {}: {}", preCounter.getFileName(), ex.getMessage());
//            return Collections.emptyMap();
//        }
//    }
//
//
//
//
//
//    // Khai báo ObjectMapper (Nên inject từ Bean, nhưng new ở đây cho gọn code snippet)
//    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
//
//    private Map<String, List<UnifiedRecord>> parseCounter45GAccessCapnProto(CounterDataObject preCounter) {
//        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();
//
//        // List dùng để lưu snapshot dữ liệu gốc từ file (để đối chứng)
//        List<Map<String, Object>> rawInputSnapshot = new ArrayList<>();
//
//        try {
//            ByteBuffer capMsg = preCounter.getBufferData();
//            capMsg.rewind();
//            MessageReader rd = Serialize.read(capMsg, readerOptions);
//            CounterSchema.CounterDataCollection.Reader root = rd.getRoot(CounterSchema.CounterDataCollection.factory);
//
//            String neName = root.getUnit().toString();
//            NEObject neObj = neObjectMap.get(neName);
//            if (neObj == null) {
//                log.warn("NodeName {} not found. Ignoring file {}", neName, preCounter.getFileName());
//                return Collections.emptyMap();
//            }
//            int neId = neObj.getId();
//
//            // --- LOOP BLOCKS ---
//            for (CounterSchema.CounterData.Reader r : root.getData()) {
//                // 1. Lấy dữ liệu thô
//                long timeRaw = r.getTime();
//                Timestamp recordTime = new Timestamp(timeRaw);
//                int duration = r.getDuration();
//                String location = r.getLocation().toString().trim();
//                long cellIndex = r.getCell();
//
//                // Snapshot: Lưu thông tin header của block
//                Map<String, Object> blockSnapshot = new HashMap<>();
//                blockSnapshot.put("time", recordTime.toString());
//                blockSnapshot.put("location", location);
//                blockSnapshot.put("cell", cellIndex);
//                List<String> rawMetrics = new ArrayList<>(); // Lưu dạng "ID:Value" cho gọn
//
//                // 2. Xử lý Logic
//                Map<String, Object> dimMap = parseDimensions(location, cellIndex);
//                Map<String, Map<String, Object>> tableBuffer = new HashMap<>();
//
//                // --- LOOP METRICS ---
//                for (CounterSchema.CounterValue.Reader val : r.getData()) {
//                    int counterId = val.getId();
//                    double value = val.getValue(); // Giả sử là double
//
//                    // Snapshot: Lưu cặp ID-Value gốc
//                    rawMetrics.add(counterId + ":" + value);
//
//                    CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
//                    if (mapping != null) {
//                        String tableName = mapping.getGroupCode();
//                        tableBuffer.computeIfAbsent(tableName, k -> new HashMap<>())
//                                .put("c_" + counterId, value);
//                    } else {
//                        log.warn("[{}] Counter ID-Value: ({}-{}) is out of scope! it can be a new counter, file: {}",
//                                TenantContextHolder.getCurrentSystem().name(), counterId, value,
//                                preCounter.getPath() + "/" + preCounter.getFileName());                    }
//                }
//
//                // Đưa list metric thô vào snapshot của block
//                blockSnapshot.put("raw_metrics", rawMetrics);
//                rawInputSnapshot.add(blockSnapshot);
//
//                // 3. Đóng gói Result (Code cũ)
//                for (Map.Entry<String, Map<String, Object>> entry : tableBuffer.entrySet()) {
//                    String tableName = entry.getKey();
//                    Map<String, Object> metrics = entry.getValue();
//                    Map<String, Object> finalData = filterExtraFields(tableName, dimMap);
//                    finalData.putAll(metrics);
//
//                    UnifiedRecord record = UnifiedRecord.builder()
//                            .recordTime(recordTime)
//                            .neId(neId)
//                            .duration(duration)
//                            .data(finalData)
//                            .build();
//
//                    resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
//                }
//            }
//
//            // =========================================================================
//            // LOG AUDIT: In ra 1 dòng JSON duy nhất chứa cả INPUT GỐC và OUTPUT KẾT QUẢ
//            // =========================================================================
//            if (log.isDebugEnabled()) {
//                Map<String, Object> auditLog = new HashMap<>();
//                auditLog.put("file_name", preCounter.getFileName());
//                auditLog.put("ne_name", neName);
//                auditLog.put("INPUT_RAW_SNAPSHOT", rawInputSnapshot); // Dữ liệu đọc được từ file
//                auditLog.put("OUTPUT_PARSED_RESULT", resultMap);      // Dữ liệu sau khi mapping
//
//                log.debug("AUDIT_TRACE: {}", objectMapper.writeValueAsString(auditLog));
//            }
//
//        } catch (Exception ex) {
//            log.error("CapnProto parsing error: {}", ex.getMessage());
//            throw new RuntimeException(ex);
//        }
//        return resultMap;
//    }
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//    /// //////////////////////////////////////////////////////////////////////// helper
//    /**
//     * extract sftp index
//     */
//
//    private String getSfpIndex(String counterName) {
//        for (int i = 1; i <= 4; i++) {
//            if (counterName.contains("sfp" + i)) {
//                return String.valueOf(i);
//            }
//        }
//        return "-1";
//    }
//
//
//
//    /**
//     * Lọc ExtraField dựa trên cấu hình bảng (CounterCat) để tránh dư thừa dữ liệu
//     */
//    private Map<String, Object> filterExtraFields(String tableName, Map<String, Object> rawDims) {
//        Map<String, Object> filtered = new HashMap<>();
//
//        // Tìm cấu hình extra field cho bảng này (cần logic mapping ngược từ tableName -> objectLevelId -> extraFields)
//        // Ở đây giả sử ta lấy objectLevelId từ một sample counter thuộc bảng đó (đã optimize)
//        // Code demo logic đơn giản:
//
//        // TIP: Để tối ưu, bạn nên cache mapping TableName -> ObjectLevelId trong constructor
//        Integer objectLevelId = getObjectLevelIdByTable(tableName);
//
//        if (objectLevelId != null) {
//            HashMap<String, ExtraFieldObject> config = extraFieldObjectMap.get(objectLevelId);
//            if (config != null) {
//                rawDims.forEach((k, v) -> {
//                    if (config.containsKey(k)) {
//                        ExtraFieldObject efo = config.get(k);
//                        // Dùng columnName làm key chuẩn
//                        filtered.put(efo.getColumnName(), v);
//                    }
//                });
//            }
//        }
//
//        return filtered;
//    }
//
//    // Helper method (cần implement cache để nhanh hơn)
//    private Integer getObjectLevelIdByTable(String tableName) {
//        // Tìm bất kỳ counter nào thuộc table này để lấy objectLevelId
//        return counterCounterCatObjectMap.values().stream()
//                .filter(c -> c.getGroupCode().equals(tableName))
//                .map(CounterCounterCatObject::getObjectLevelId)
//                .findFirst()
//                .orElse(null);
//    }
//
//
//
//
//    /**
//     * Parse Location String thành Map các dimensions
//     */
//    private Map<String, Object> parseDimensions(String location, long cellIndex) {
//        Map<String, Object> dims = new HashMap<>();
//        // Parse chuỗi location "k1=v1,k2=v2"
//        if (location != null && !location.isEmpty()) {
//            String[] pairs = location.split(",");
//            for (String pair : pairs) {
//                String[] kv = pair.trim().split("=");
//                if (kv.length == 2) {
//                    dims.put(kv[0].toLowerCase().trim(), kv[1].trim());
//                }
//            }
//            dims.put("location", location);
//        }
//
//        // Thêm các field tính toán
//        dims.put("cell_index", cellIndex);
//
//        // Rat Type logic
//        String nodeFunction = (String) dims.get("nodefunction");
//        String ratType = RatType.fromNodeFunction(nodeFunction);
//        dims.put("rat_type", ratType);
//
//        if (dims.containsKey("cellname")) {
//            dims.put("cell_name", dims.get("cellname"));
//        }
//
//        return dims;
//    }
//
//
//
//
//
//    private Map<String, List<UnifiedRecord>> parseCounter5GCoreProtobuf(CounterDataObject preCounter) {
//        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();
////        try {
////            ByteBuffer bufferData = preCounter.getBufferData();
////            bufferData.rewind();
////
////            // 1. Parse Root Protobuf Messages
////            // (Giả sử bạn đã có các class generated từ file .proto)
////            VesEvent vesEvent = VesEvent.parseFrom(bufferData);
////            Timestamp recordTime = new Timestamp(vesEvent.getCommonEventHeader().getStartEpochMicrosec() / 1000);
////            Perf3gppFields perf3gppFields = Perf3gppFields.parseFrom(vesEvent.getEventFields());
////            MeasDataCollection measDataCollection = perf3gppFields.getMeasDataCollection();
////
////            int duration = measDataCollection.getGranularityPeriod();
////            String neName = measDataCollection.getMeasuredEntityUserName();
////
////            // 2. Validate NE
////            NEObject neObj = neObjectMap.get(neName);
////            if (neObj == null) {
////                log.info("Oops! Cannot find nodeName: {}! ignore counter file {}", neName, preCounter.getFileName());
////                return Collections.emptyMap();
////            }
////            int neId = neObj.getId();
////
////            // 3. Loop MeasInfo (Mỗi MeasInfo là một nhóm các loại counter)
////            for (MeasDataCollectionOuterClass.MeasInfo measInfo : measDataCollection.getMeasInfoList()) {
////
////                // A. Lấy danh sách Counter ID (MeasTypes)
////                List<Integer> lstCounterId = new ArrayList<>();
////                MeasDataCollectionOuterClass.MeasInfo.MeasTypesCase measTypesCase = measInfo.getMeasTypesCase();
////
////                if (MeasDataCollectionOuterClass.MeasInfo.MeasTypesCase.IMEASTYPES.name().equals(measTypesCase.name())) {
////                    MeasDataCollectionOuterClass.MeasInfo.IMeasTypes iMeasTypes = measInfo.getIMeasTypes();
////                    for (Integer iMeasType : iMeasTypes.getIMeasTypeList()) {
////                        lstCounterId.add(iMeasType);
////                    }
////                }
////                // TODO: Handle case MeasTypes (String name) if needed -> Map name back to ID
////
////                if (lstCounterId.isEmpty()) continue;
////
////                // B. Loop MeasValue (Các dòng dữ liệu cụ thể)
////                for (MeasDataCollectionOuterClass.MeasValue measValue : measInfo.getMeasValueList()) {
////
////                    // B1. Parse Extra Fields (Dimensions) từ MeasObjInstId
////                    // Ví dụ: "function=AMF, region=Hanoi"
////                    Map<String, Object> rawDims = new HashMap<>();
////                    MeasDataCollectionOuterClass.MeasValue.MeasObjInstIdCase measObjInstIdCase = measValue.getMeasObjInstIdCase();
////
////                    if (MeasDataCollectionOuterClass.MeasValue.MeasObjInstIdCase.SMEASOBJINSTID.name().equals(measObjInstIdCase.name())) {
////                        // Helper parse chuỗi "k=v,k=v" thành Map
////                        Map<String, String> parsedMeasObj = parseMeasObj(measValue.getSMeasObjInstId());
////                        rawDims.putAll(parsedMeasObj);
////                    }
////
////                    // B2. Gom nhóm dữ liệu theo Bảng (Table)
////                    // Một dòng MeasValue có thể chứa counter của nhiều bảng khác nhau -> Cần tách ra.
////                    // Key: TableName -> Map<Col, Val>
////                    Map<String, Map<String, Object>> tableBuffer = new HashMap<>();
////
////                    List<MeasDataCollectionOuterClass.MeasResult> results = measValue.getMeasResultsList();
////
////                    for (int idx = 0; idx < results.size(); idx++) {
////                        if (idx >= lstCounterId.size()) break;
////
////                        int counterId = lstCounterId.get(idx);
////
////                        // Lấy giá trị (p -> position/index, hoặc lấy value trực tiếp tùy proto version)
////                        // Code cũ của bạn: measResults.getIValue()
////                        long val = results.get(idx).getIValue();
////
////                        CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
////                        if (mapping != null) {
////                            String tableName = mapping.getGroupCode();
////                            tableBuffer.computeIfAbsent(tableName, k -> new HashMap<>())
////                                    .put("c_" + counterId, val);
////                        }
////                    }
////
////                    // B3. Tạo UnifiedRecord cho từng bảng
////                    for (Map.Entry<String, Map<String, Object>> entry : tableBuffer.entrySet()) {
////                        String tableName = entry.getKey();
////                        Map<String, Object> counters = entry.getValue();
////
////                        // Filter Extra Fields chuẩn theo config của bảng đó
////                        Map<String, Object> finalData = filterExtraFields(tableName, rawDims);
////
////                        // Put counters vào final data
////                        finalData.putAll(counters);
////
////                        UnifiedRecord record = UnifiedRecord.builder()
////                                .recordTime(recordTime)
////                                .neId(neId)
////                                .duration(duration)
////                                .data(finalData)
////                                .build();
////
////                        resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
////                    }
////                }
////            }
////
////        } catch (Exception ex) {
////            log.error("Protobuf parsing error for file {}", preCounter.getFileName(), ex);
////            // return empty or rethrow
////        }
//        return resultMap;
//    }
//
//
//    // --- PARSE CSV (ORAN / 5G Access) ---
//
//    private Map<String, List<UnifiedRecord>> parseCounter5GAccessCSV(CounterDataObject preCounter) {
//        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();
//        try {
//            String fileName = preCounter.getFileName();
//            String[] lstFileName = fileName.split("_");
//
//            if (lstFileName.length != FILE_NAME_LENGTH) {
//                log.warn("File {}: Wrong format!", fileName);
//                return resultMap;
//            }
//
//            String neName = lstFileName[3].trim();
//            String ruIndex = lstFileName[5].trim();
//            NEObject neObj = neObjectMap.get(neName);
//
//            if (neObj == null) {
//                log.warn("NE {} not found or disabled in file: {}", neName, fileName);
//                return resultMap;
//            }
//            int neId = neObj.getId();
//
//            List<String> lstLineData = preCounter.getLineData();
//            if (lstLineData == null || lstLineData.isEmpty()) {
//                log.warn("File: {} Data is empty!", fileName);
//                return resultMap;
//            }
//
//            for (String line : lstLineData) {
//                String[] lineValues = line.split(",");
//                if (lineValues.length < 4) continue;
//
//                String measureKey = lineValues[0].trim() + "_" + lineValues[1].trim().toUpperCase();
//
//                // Parse Timestamp
//                Timestamp startTime = parseTimestamp(lineValues[2]);
//                Timestamp endTime = parseTimestamp(lineValues[3]);
//                int duration = (int) ((endTime.getTime() - startTime.getTime()) / 1000);
//
//                CounterConfigObject config = counterConfigObjectMap.get(measureKey);
//                if (config == null) {
//                    // log.debug("{} does not exist!", measureKey);
//                    continue;
//                }
//
//                if (!validateLineFormat(config.getMeasurementGroup(), lineValues.length)) {
//                    continue;
//                }
//
//                // Parse Line -> UnifiedRecord Map
//                Map<String, List<UnifiedRecord>> lineRecords = parseCounterOranLine(
//                        config, startTime, neId, ruIndex, duration, lineValues
//                );
//
//                // Merge results
//                lineRecords.forEach((table, records) ->
//                        resultMap.computeIfAbsent(table, k -> new ArrayList<>()).addAll(records)
//                );
//            }
//        } catch (Exception e) {
//            log.error("CSV Parse Error", e);
//            throw new RuntimeException(e);
//        }
//        return resultMap;
//    }
//
//    private Timestamp parseTimestamp(String s) {
//        String clean = s.trim().replace("T", " ").replace("Z", "");
//        return Timestamp.valueOf(clean);
//    }
//
//    private boolean validateLineFormat(String group, int length) {
//        if ("tx-measurement-objects".equalsIgnoreCase(group) || "rx-window-stats".equalsIgnoreCase(group)) {
//            return length == RX_TX_WINDOW_TRANSPORT_LENGTH;
//        } else if ("transceiver-stats".equalsIgnoreCase(group)) {
//            return length == TRANSCEIVER_LENGTH;
//        } else if ("epe-stats".equalsIgnoreCase(group)) {
//            return length == EPE_LENGTH;
//        }
//        log.warn("Line is wrong format: {}", length);
//        return false;
//    }
//
//    private Map<String, List<UnifiedRecord>> parseCounterOranLine(
//            CounterConfigObject config, Timestamp time, int neId, String ruIndex, int duration, String[] values) {
//
//        Map<String, List<UnifiedRecord>> result = new HashMap<>();
//        HashMap<Integer, LiteCounterObject> measurements = config.getHmLiteCounter();
//
//        // Map gom nhóm. Key phải bao gồm cả SFP Index để phân biệt dòng
//        // Key: "TableName|SfpIndex"
//        Map<String, Map<String, Object>> rowBufferMap = new HashMap<>();
//
//        for (int idx = START_INDEX; idx < values.length; idx++) {
//            String valStr = values[idx].trim();
//            if ("-".equals(valStr) || valStr.isEmpty()) continue;
//
//            LiteCounterObject liteCounter = measurements.get(idx);
//            if (liteCounter == null) continue;
//
//            int counterId = liteCounter.getId();
//            CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
//
//            if (mapping != null) {
//                try {
//                    long value = Long.parseLong(valStr);
//                    String tableName = mapping.getGroupCode();
//
//                    // Lấy sfp_index của cột này. Ví dụ: "1", "2", hoặc "-1"
//                    String sfpIndex = getSfpIndex(liteCounter.getName().toLowerCase());
//
//                    // [MIGRATION LOGIC]: Code cũ dùng sExtraField để phân tách dòng.
//                    // sExtraField thay đổi khi sfpIndex thay đổi.
//                    // => Ta phải dùng (TableName + sfpIndex) làm khóa phân tách.
//                    String compositeKey = tableName + "|" + sfpIndex;
//
//                    Map<String, Object> rowData = rowBufferMap.computeIfAbsent(compositeKey, k -> {
//                        Map<String, Object> newRow = new HashMap<>();
//                        // Các field chung
//                        newRow.put("ru_index", ruIndex);
//                        // Field riêng phân biệt dòng
//                        if (!"-1".equals(sfpIndex)) {
//                            newRow.put("sfp_index", sfpIndex);
//                        }
//                        return newRow;
//                    });
//
//                    rowData.put("c_" + counterId, value);
//
//                } catch (NumberFormatException e) {
//                    // ignore
//                }
//            }
//        }
//
//        // Convert map buffer thành List UnifiedRecord
//        for (Map.Entry<String, Map<String, Object>> entry : rowBufferMap.entrySet()) {
//            String compositeKey = entry.getKey();
//            String tableName = compositeKey.split("\\|")[0]; // Lấy lại tên bảng
//            Map<String, Object> rawData = entry.getValue();
//
//            // Filter extra field để loại bỏ các field thừa (giống logic cũ)
//            Map<String, Object> finalData = filterExtraFields(tableName, rawData);
//
//            // Put lại counter values vào finalData
//            rawData.forEach((k, v) -> {
//                if (k.startsWith("c_")) finalData.put(k, v);
//            });
//
//            UnifiedRecord record = UnifiedRecord.builder()
//                    .recordTime(time)
//                    .neId(neId)
//                    .duration(duration)
//                    .data(finalData)
//                    .build();
//
//            result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
//        }
//
//        return result;
//    }
//
//}
