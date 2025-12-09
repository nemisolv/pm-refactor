package com.viettel.ems.perfomance.parser.better;

import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.object.CounterCounterCatObject;
import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.object.ExtraFieldObject;
import com.viettel.ems.perfomance.object.NEObject;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CoreProtobufParser extends BaseParser{
    public CoreProtobufParser(Map<String, NEObject> neObjectMap, Map<Integer, CounterCounterCatObject> counterCounterCatObjectMap, Map<Integer, HashMap<String, ExtraFieldObject>> extraFieldMap) {
        super(neObjectMap, counterCounterCatObjectMap, extraFieldMap);
    }

    @Override
    public boolean support(String fileName) {
        return fileName.contains(Constant.NEType.AMF.name()) || fileName.contains(Constant.NEType.SMF.name())
                || fileName.contains(Constant.NEType.UPF.name()) || fileName.contains(Constant.NEType.NRF.name())
                || fileName.contains(Constant.NEType.NSSF.name()) || fileName.contains(Constant.NEType.MSC.name())
                || fileName.contains(Constant.NEType.UDM.name()) || fileName.contains(Constant.NEType.MME.name())
                || fileName.contains(Constant.NEType.SGWC.name()) || fileName.contains(Constant.NEType.IMS.name())
                || fileName.contains(Constant.NEType.NEF.name()) || fileName.contains(Constant.NEType.NWDAF.name())
                || fileName.contains(Constant.NEType.AUSF.name()) || fileName.contains(Constant.NEType.UDR.name())
                || fileName.contains(Constant.NEType.PCF.name()) || fileName.contains(Constant.NEType.GMLC.name())
                || fileName.contains(Constant.NEType.CHF.name()) || fileName.contains(Constant.NEType.LMF.name())
                || fileName.contains(Constant.NEType.AF.name()) || fileName.contains(Constant.NEType.DN.name());
    }

    @Override
    public Map<String, List<UnifiedRecord>> parse(CounterDataObject dataObject) {
        Map<String, List<UnifiedRecord>> resultMap = new HashMap<>();
//        try {
//            ByteBuffer bufferData = preCounter.getBufferData();
//            bufferData.rewind();
//
//            // 1. Parse Root Protobuf Messages
//            // (Giả sử bạn đã có các class generated từ file .proto)
//            VesEvent vesEvent = VesEvent.parseFrom(bufferData);
//            Timestamp recordTime = new Timestamp(vesEvent.getCommonEventHeader().getStartEpochMicrosec() / 1000);
//            Perf3gppFields perf3gppFields = Perf3gppFields.parseFrom(vesEvent.getEventFields());
//            MeasDataCollection measDataCollection = perf3gppFields.getMeasDataCollection();
//
//            int duration = measDataCollection.getGranularityPeriod();
//            String neName = measDataCollection.getMeasuredEntityUserName();
//
//            // 2. Validate NE
//            NEObject neObj = neObjectMap.get(neName);
//            if (neObj == null) {
//                log.info("Oops! Cannot find nodeName: {}! ignore counter file {}", neName, preCounter.getFileName());
//                return Collections.emptyMap();
//            }
//            int neId = neObj.getId();
//
//            // 3. Loop MeasInfo (Mỗi MeasInfo là một nhóm các loại counter)
//            for (MeasDataCollectionOuterClass.MeasInfo measInfo : measDataCollection.getMeasInfoList()) {
//
//                // A. Lấy danh sách Counter ID (MeasTypes)
//                List<Integer> lstCounterId = new ArrayList<>();
//                MeasDataCollectionOuterClass.MeasInfo.MeasTypesCase measTypesCase = measInfo.getMeasTypesCase();
//
//                if (MeasDataCollectionOuterClass.MeasInfo.MeasTypesCase.IMEASTYPES.name().equals(measTypesCase.name())) {
//                    MeasDataCollectionOuterClass.MeasInfo.IMeasTypes iMeasTypes = measInfo.getIMeasTypes();
//                    for (Integer iMeasType : iMeasTypes.getIMeasTypeList()) {
//                        lstCounterId.add(iMeasType);
//                    }
//                }
//                // TODO: Handle case MeasTypes (String name) if needed -> Map name back to ID
//
//                if (lstCounterId.isEmpty()) continue;
//
//                // B. Loop MeasValue (Các dòng dữ liệu cụ thể)
//                for (MeasDataCollectionOuterClass.MeasValue measValue : measInfo.getMeasValueList()) {
//
//                    // B1. Parse Extra Fields (Dimensions) từ MeasObjInstId
//                    // Ví dụ: "function=AMF, region=Hanoi"
//                    Map<String, Object> rawDims = new HashMap<>();
//                    MeasDataCollectionOuterClass.MeasValue.MeasObjInstIdCase measObjInstIdCase = measValue.getMeasObjInstIdCase();
//
//                    if (MeasDataCollectionOuterClass.MeasValue.MeasObjInstIdCase.SMEASOBJINSTID.name().equals(measObjInstIdCase.name())) {
//                        // Helper parse chuỗi "k=v,k=v" thành Map
//                        Map<String, String> parsedMeasObj = parseMeasObj(measValue.getSMeasObjInstId());
//                        rawDims.putAll(parsedMeasObj);
//                    }
//
//                    // B2. Gom nhóm dữ liệu theo Bảng (Table)
//                    // Một dòng MeasValue có thể chứa counter của nhiều bảng khác nhau -> Cần tách ra.
//                    // Key: TableName -> Map<Col, Val>
//                    Map<String, Map<String, Object>> tableBuffer = new HashMap<>();
//
//                    List<MeasDataCollectionOuterClass.MeasResult> results = measValue.getMeasResultsList();
//
//                    for (int idx = 0; idx < results.size(); idx++) {
//                        if (idx >= lstCounterId.size()) break;
//
//                        int counterId = lstCounterId.get(idx);
//
//                        // Lấy giá trị (p -> position/index, hoặc lấy value trực tiếp tùy proto version)
//                        // Code cũ của bạn: measResults.getIValue()
//                        long val = results.get(idx).getIValue();
//
//                        CounterCounterCatObject mapping = counterCounterCatObjectMap.get(counterId);
//                        if (mapping != null) {
//                            String tableName = mapping.getGroupCode();
//                            tableBuffer.computeIfAbsent(tableName, k -> new HashMap<>())
//                                    .put("c_" + counterId, val);
//                        }
//                    }
//
//                    // B3. Tạo UnifiedRecord cho từng bảng
//                    for (Map.Entry<String, Map<String, Object>> entry : tableBuffer.entrySet()) {
//                        String tableName = entry.getKey();
//                        Map<String, Object> counters = entry.getValue();
//
//                        // Filter Extra Fields chuẩn theo config của bảng đó
//                        Map<String, Object> finalData = filterExtraFields(tableName, rawDims);
//
//                        // Put counters vào final data
//                        finalData.putAll(counters);
//
//                        UnifiedRecord record = UnifiedRecord.builder()
//                                .recordTime(recordTime)
//                                .neId(neId)
//                                .duration(duration)
//                                .data(finalData)
//                                .build();
//
//                        resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
//                    }
//                }
//            }
//
//        } catch (Exception ex) {
//            log.error("Protobuf parsing error for file {}", preCounter.getFileName(), ex);
//            // return empty or rethrow
//        }
        return resultMap;
    }
}
