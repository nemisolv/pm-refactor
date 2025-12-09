package com.viettel.controller;

import com.google.gson.Gson;

import com.viettel.service.*;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Validated
@RestController
@RequestMapping("/ont")
@RequiredArgsConstructor
public class OntController {
    private final ONUStatistical onuStatistical;
    private final OntStatsService ontStatsService;
    private final OntStatisticService ontStatisticService;
    private final OntRawStatisticService ontRawStatisticService;
    private final CSVService csvService;

    private void setCSVFileAttachmentHeaders(HttpServletResponse response, String fileName) {
        response.setContentType("application/csv");
        response.addHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
    }

    @PostMapping("/statistic")
    public HttpRespOntStatistic getOntStatistic(@Valid @RequestBody HttpReqOntStatistic reqOntStatistic) {
        HelperUtils.printPrettyLog( "info",  "Ont Statistic Input", reqOntStatistic);
        List<OntStatisticDTO> ontStatisticDTOs = ontStatisticService.getOntStatisticDataFromPrecomputedDatabase(reqOntStatistic);
        HttpRespOntStatistic httpRespOntStatistic = new HttpRespOntStatistic();
        httpRespOntStatistic.responseCode(HttpStatus.OK.value())
                .responseMessage("Successfully")
                .totalRecord(ontStatisticDTOs.size())
                .data(ontStatisticDTOs);
        return httpRespOntStatistic;
    }

    @PostMapping("/statistic/export")
    public void getOntStatisticExcel(@Valid @RequestBody HttpReqOntStatistic reqOntStatistic, HttpServletResponse response) throws IOException {
        HelperUtils.printPrettyLog( "info",  "Export Ont Statistic Input", reqOntStatistic);
        List<OntStatisticDTO> ontStatisticDTOs = ontStatisticService.getOntStatisticDataFromPrecomputedDatabase(reqOntStatistic);
        String fileName = csvService.getFileName(reqOntStatistic);
        setCSVFileAttachmentHeaders(response, fileName);
        csvService.exportOntStatisticData(ontStatisticDTOs, reqOntStatistic.statisticsBy(), response.getWriter(), highlightColumn: null);
    }

    @PostMapping("/statistic/raw")
    public HttpRespOntStatistic getOntStatisticFromRaw(@Valid @RequestBody HttpReqOntRaw request) {
        HelperUtils.printPrettyLog(level: "info", name: "Export Ont Statistic Input Raw", request);
        List<OntStatisticDTO> ontStatisticDTOs = ontRawStatisticService.getOntStatisticDataFromRawDatabase(request);
        HttpRespOntStatistic httpRespOntStatistic = new HttpRespOntStatistic();
        httpRespOntStatistic.responseCode(HttpStatus.OK.value())
                .responseMessage("Successfully")
                .totalRecord(ontStatisticDTOs.size())
                .data(ontStatisticDTOs);
        return httpRespOntStatistic;
    }

    @PostMapping("/statistic/raw/export")
    public void exportStatisticFromRaw(@Valid @RequestBody HttpReqOntRaw request, HttpServletResponse response) throws IOException {
        HelperUtils.printPrettyLog(level: "info", name: "Export Ont Statistic Input", request);
        List<OntStatisticDTO> ontStatisticDTOs = ontRawStatisticService.getOntStatisticDataFromRawDatabase(request);
        String fileName = csvService.getFileName(request);
        setCSVFileAttachmentHeaders(response, fileName);
        csvService.exportOntStatisticData(ontStatisticDTOs, new ArrayList<>(), response.getWriter(), request.filter() == null ? null : request.filter().name());
    }

    @PostMapping("/stats")
    public HttpRespOntStats getOntStats(@RequestBody List<@Valid HttpReqOntStats> httpReqOntStats) {
        log.info("OntStats Input: {}", new Gson().toJson(httpReqOntStats));

        List<OntStatsData> ontStatsData = ontStatsService.getOntStatsDataFromDB(httpReqOntStats);

        log.info("OntStats Response: {}", new Gson().toJson(ontStatsData));

        return new HttpRespOntStats()
                .responseCode(200)
                .responseMessage("Successfully")
                .statsDataList(ontStatsData);
    }

    @PostMapping("/kpi-stats")
    public HttpResKpiStats getKpiStats(@RequestBody @Valid HttpReqKpiStats httpReqKpiStats) {
        log.info("KpiStats Input: {}", new Gson().toJson(httpReqKpiStats));

        List<KpiStatsData> kpiStatsData = ontStatsService.getKpiStatsDataFromDB(httpReqKpiStats);

        log.info("KpiStats Response: {}", new Gson().toJson(kpiStatsData));

        if (kpiStatsData == null) {
            return new HttpResKpiStats()
                    .responseCode(400)
                    .responseMessage("No data for this request")
                    .kpiStatsDataList(null);
        }

        return new HttpResKpiStats()
                .responseCode(200)
                .responseMessage("Successfully")
                .kpiStatsDataList(kpiStatsData);
    }

    @PostMapping("/onu-by-mac")
    public ResponseEntity<?> getOnuByMAC(@RequestBody RequestONUByMAC filter) {
        log.info("ONU By MAC Input: {}", HelperUtils.toPrettyJson(filter));
        try {
            Object response = onuStatistical.getListONUBYMAC(filter);

            log.info("ONU By MAC Result: {}", HelperUtils.toPrettyJson(response));
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            log.error("Exception: {} {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
            return ResponseEntity.badRequest().body(ExceptionUtils.getStackTrace(e));
        }
    }

    @PostMapping("/detail-by-onu")
    public ResponseEntity<?> detailByONU(@RequestBody RequestONUByMAC filter) {
        log.info("Detail ONU By MAC Input: {}", HelperUtils.toPrettyJson(filter));
        try {
            Object response = onuStatistical.getDetailByONU(filter);

            log.info("Detail ONU By MAC Result: {}", HelperUtils.toPrettyJson(response));
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            log.error("Exception: {} {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
            return ResponseEntity.badRequest().body(ExceptionUtils.getStackTrace(e));
        }
    }

        @PostMapping("/onu-by-rssi")
    public ResponseEntity<?> getONUByRssi(@RequestBody RequestONUByRssi filter) {
        log.info("ONU By RSSI Input: {}", HelperUtils.toPrettyJson(filter));
        try {
            Object response = onuStatistical.getListONUBYRssi(filter);

            log.info("ONU By RSSI Result: {}", HelperUtils.toPrettyJson(response));
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            log.error("Exception: {} {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
            return ResponseEntity.badRequest().body(ExceptionUtils.getStackTrace(e));
        }
    }

    @PostMapping("/detail-by-rssi")
    public ResponseEntity<?> detailByRssi(@RequestBody RequestONUByRssi filter) {
        log.info("Detail ONU By RSSI Input: {}", HelperUtils.toPrettyJson(filter));
        try {
            Object response = onuStatistical.getDetailByRssi(filter);

            log.info("Detail ONU By RSSI Result: {}", HelperUtils.toPrettyJson(response));
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            log.error("Exception: {} {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
            return ResponseEntity.badRequest().body(ExceptionUtils.getStackTrace(e));
        }
    }

    @PostMapping("/list-config-error")
    public ResponseEntity<?> listConfigError(@RequestBody RequestConfigError filter) {
        log.info("List Config Error Input: {}", HelperUtils.toPrettyJson(filter));
        try {
            Object response = onuStatistical.listConfigError(filter);

            log.info("List Config Error Result: {}", HelperUtils.toPrettyJson(response));
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            log.error("Exception: {} {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
            return ResponseEntity.badRequest().body(ExceptionUtils.getStackTrace(e));
        }
    }

    @PostMapping("/applied-config")
    public ResponseEntity<?> appliedConfig(@RequestBody StandardConfigApplied applied) {
        log.info("Standard Config Applied Input: {}", HelperUtils.toPrettyJson(applied));
        try {
            Object response = onuStatistical.standardConfigApplied(applied);

            log.info("Standard Config Applied Result: {}", HelperUtils.toPrettyJson(response));
            return ResponseEntity.ok().body(response);
        } catch (Exception e) {
            log.error("Exception: {} {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
            return ResponseEntity.badRequest().body(Map.of("message", ExceptionUtils.getStackTrace(e)));
        }
    }
}
