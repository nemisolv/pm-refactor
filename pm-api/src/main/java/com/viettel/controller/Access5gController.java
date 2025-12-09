package com.viettel.controller;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.viettel.config.ConfigManager;
import com.viettel.config.SystemType;
import com.viettel.config.TenantContextHolder;
import com.viettel.dal.*;
import com.viettel.troubleshoot.DatasourceVerifier;
import com.viettel.util.Util;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/access5g") // must refactor the endpoint
public class Access5gController {
    private final Util util;
    private final DatasourceVerifier dataSourceVerifier;
    private final ConfigManager configManager;
    private final Gson gson;


    @PostMapping("/getall")
    public HttpResponseData getAllData(@Valid @RequestBody InputStatisticData inputStatisticData) {

        SystemType systemType = TenantContextHolder.getCurrentSystem();
        boolean isUsingCaching = configManager.getCustomBoolean(systemType,"isUsingCaching");

        log.info("Query-Mode: {}", isUsingCaching ? "Caching" : "Raw");

        HttpResponseData httpResponse = new HttpResponseData();
        List<ReportStatisticKPI> result = new ArrayList<>();
        dataSourceVerifier.verifyConnection();

        inputStatisticData.sortAllListInside();

        try {
            result = util.getStatisticFromDatabase(inputStatisticData, isUsingCaching);
        } catch (Exception e) {
            log.error("Exception in Access5gController.getAllData: ", e);
        }

        int resultSize = result.size();
        boolean hasData = resultSize > 0;
        httpResponse.setResponseCode( hasData ? 1 : -1);
        httpResponse.setResponseData(new ResponseData(result, resultSize, 0));
        httpResponse.setResponseMessage(hasData ? "Request success" : "No data for this request");

        return httpResponse;
    }
}
