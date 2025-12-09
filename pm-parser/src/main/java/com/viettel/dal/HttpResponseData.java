package com.viettel.dal;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class HttpResponseData {
    @JsonProperty("response_code")
    private int responseCode;
    @JsonProperty("respnose_message")
    private String responseMessage;
    @JsonProperty("response_data")
    private ResponseData responseData;

}
