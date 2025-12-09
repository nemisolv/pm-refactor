package com.viettel.dal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReportStatisticKPI {
    
    @JsonProperty("extra_field")
    private HashMap<String, String> extraField = new HashMap<>();

    @JsonProperty("param_value")
    private Map<String, Double> param = new TreeMap<>();

    @JsonIgnore
    private String extraFieldKey;

    public Map<String, Double> getParam() {
        return param;
    }

    public void setParam(HashMap<String, Double> param) {
        this.param = param;
    }

    public HashMap<String, String> getExtraField() {
        return extraField;
    }

    public void setExtraField(HashMap<String, String> extraField) {
        this.extraField = extraField;
    }

    public void setExtraFieldKey(String extraFieldKey) {
        this.extraFieldKey = extraFieldKey;
    }

    public String getExtraFieldKey() {
        return extraFieldKey;
    }
}
