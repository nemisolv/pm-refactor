package com.viettel.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CSVService {

    @Qualifier("alias-to-order-map")
    private final Map<String, Integer> aliasToOrderMap;

    @Qualifier("alias-to-view-name-map")
    private final Map<String, String> aliasToViewNameMap;

    private String formatList(String str) {
        return str.replace("[", "").replace("]", "").replace("\"", "").replace(", ", ";");
    }

    @SuppressWarnings("unchecked")
    private List<OntStatisticDTO> convertListsInMapIntoMultipleRows(List<OntStatisticDTO> ontStatisticDTOs) {
        List<OntStatisticDTO> replacingOntStatisticDTOs = new ArrayList<>();
        for (OntStatisticDTO dto : ontStatisticDTOs) {
            Map<String, Object> map = dto.statisticValues();

            String key = map.keySet().stream()
                    .filter(e -> e.contains("record_time"))
                    .findFirst()
                    .orElse("");

            if (key.isEmpty()) {
                continue;
            }

            Map.Entry<String, Object> entry = map.entrySet().stream()
                    .filter(e -> !e.getKey().contains(key))
                    .findFirst()
                    .orElse(null);

            if (entry == null) {
                continue;
            }

            List<LocalDateTime> recordTimes = new ArrayList<>();
            if (map.get(key) instanceof List) {
                recordTimes.addAll((List<LocalDateTime>) map.get(key));
            } else if (map.get(key) instanceof LocalDateTime) {
                recordTimes.add((LocalDateTime) map.get(key));
            }

            recordTimes.sort(Collections.reverseOrder());
            for (LocalDateTime recordTime : recordTimes) {
                replacingOntStatisticDTOs.add(
                        OntStatisticDTO.builder()
                                .neId(dto.neId())
                                .pppoeUsername(dto.pppoeUsername())
                                .productClass(dto.productClass())
                                .serialNumber(dto.serialNumber())
                                .statisticValues(new TreeMap<>(Map.of(key, recordTime, entry.getKey(), entry.getValue())))
                                .build()
                );
            }
        }
        return replacingOntStatisticDTOs;
    }

    private ObjectMapper getCustomObjectMapper() {
        ObjectMapper customObjectMapper = new ObjectMapper();
        JavaTimeModule module = new JavaTimeModule();
        LocalDateTimeSerializer localDateTimeSerializer = new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        module.addSerializer(LocalDateTime.class, localDateTimeSerializer);
        customObjectMapper.registerModule(module);
        customObjectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return customObjectMapper;
    }

    private String convertSnakeCaseToWord(String input) {
        return StringUtils.capitalize(input.replace("_", " "));
    }

    public String getFileName(HttpReqOntStatistic reqOntStatistic) {
        return String.format("OntStatistic.%s.%s.Limit%d.csv",
                reqOntStatistic.sortBy(),
                reqOntStatistic.fromDate(),
                reqOntStatistic.limit());
    }

    public String getFileName(HttpReqOntRaw request) {
        return String.format("OntStatisticRaw.%s_%s.csv", request.fromDate(), request.toDate());
    }

    public void exportOntStatisticData(
            List<OntStatisticDTO> ontStatisticDTOs,
            List<String> headersIfEmptyData,
            Writer writer,
            String highlightColumn
    ) throws IOException {
        CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT);
        List<String> headers = new ArrayList<>(List.of("No", "PPPoE Account", "Serial Number", "Product Class"));

        if (ontStatisticDTOs.isEmpty()) {
            if (headersIfEmptyData.isEmpty()) {
                headersIfEmptyData.addAll(aliasToOrderMap.keySet());
            }
            if (headersIfEmptyData.size() == 1) {
                headersIfEmptyData.add("Record Time");
            }
            headersIfEmptyData.sort(Comparator.comparing(e -> {
                if (e.equals(highlightColumn)) {
                    return -1;
                }
                if (!aliasToOrderMap.containsKey(e)) {
                    return 0;
                }
                return aliasToOrderMap.get(e);
            }));
            headers.addAll(headersIfEmptyData.stream()
                    .map(e -> aliasToViewNameMap.containsKey(e) ? aliasToViewNameMap.get(e) : convertSnakeCaseToWord(e))
                    .collect(Collectors.toList()));
            printer.printRecord(headers);
            return;
        }

        ObjectMapper objectMapper = getCustomObjectMapper();

        List<Map.Entry<String, Object>> entries = new ArrayList<>(ontStatisticDTOs.get(0).statisticValues().entrySet());
        sortDataUsingOrder(highlightColumn, entries);

        headers.addAll(entries.stream()
                .map(Map.Entry::getKey)
                .map(e -> aliasToViewNameMap.containsKey(e) ? aliasToViewNameMap.get(e) : convertSnakeCaseToWord(e))
                .collect(Collectors.toList()));
        printer.printRecord(headers);

        if (ontStatisticDTOs.get(0).statisticValues().entrySet().stream().anyMatch(e -> e.getKey().contains("record_time"))
                && ontStatisticDTOs.get(0).statisticValues().size() == 2) {
            ontStatisticDTOs = convertListsInMapIntoMultipleRows(ontStatisticDTOs);
        }

        int count = 1;
        for (OntStatisticDTO data : ontStatisticDTOs) {
            entries.clear();
            entries.addAll(data.statisticValues().entrySet());
            sortDataUsingOrder(highlightColumn, entries);

            List<String> values = new ArrayList<>();
            values.add(String.valueOf(count++));
            values.add(data.pppoeUsername());
            values.add(data.serialNumber());
            values.add(data.productClass());
            values.addAll(entries.stream()
                    .map(k -> {
                        try {
                            return formatList(objectMapper.writeValueAsString(k.getValue()));
                        } catch (JsonProcessingException e) {
                            return "";
                        }
                    })
                    .collect(Collectors.toList()));
            printer.printRecord(values);
        }
    }

    private void sortDataUsingOrder(String highlightColumn, List<Map.Entry<String, Object>> entries) {
        entries.sort(Comparator.comparing(e -> {
            if (e.getKey().equals(highlightColumn)) {
                return -1;
            }
            if (!aliasToOrderMap.containsKey(e.getKey())) {
                return 0;
            }
            return aliasToOrderMap.get(e.getKey());
        }));
    }
}
