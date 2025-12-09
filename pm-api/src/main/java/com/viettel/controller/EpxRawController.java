//package com.viettel.controller;
//
//import com.google.gson.Gson;
//import com.viettel.dal.EmsFilter;
//import com.viettel.dal.HttpReqExpRaw;
//import com.viettel.dal.HttpRespExpRaw;
//import com.viettel.dal.OntStatisticDTO;
//import com.viettel.service.ExpRawService;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestBody;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;
//
//import javax.servlet.http.HttpServletResponse;
//import javax.validation.Valid;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.List;
//
//@Slf4j
//@RestController
//@RequiredArgsConstructor
//@RequestMapping("/exp_raw")
//public class EpxRawController {
//    private final ExpRawService expRawService;
//
//    @PostMapping("/get_data")
//    public void getRawData(@Valid @RequestBody HttpReqExpRaw input, HttpServletResponse response) {
//        log.info("[ExpRaw] Input request: " + new Gson().toJson(input));
//        try {
//            String fileName = expRawService.getFileName(baseName: "RawData");
//            setFileAttachmentHeaders(response, fileName);
//            expRawService.export(input, response.getWriter());
//        } catch (Exception e) {
//            log.error("[ExpRaw] Cannot Export Data!\n" + e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    @PostMapping("/get_ne")
//    public List<String> getNeByCondition(@Valid @RequestBody HttpReqExpRaw input) {
//        log.info("[ExpRaw] Input request: " + new Gson().toJson(input));
//        String result = null;
//        try {
//            String fileName = expRawService.getFileName(baseName: "RawData");
//            return expRawService.exportForCondition(input);
//        } catch (Exception e) {
//            log.error("[ExpRaw] Cannot Export Data!\n" + e.getMessage());
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    private void setFileAttachmentHeaders(HttpServletResponse response, String fileName) {
//        response.setContentType("application/csv");
//        response.addHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
//    }
//}
