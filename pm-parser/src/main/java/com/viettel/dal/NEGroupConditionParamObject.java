package com.viettel.dal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NEGroupConditionParamObject {
    @JsonProperty("param")
    @SerializedName("param")
    private String param;


    @JsonProperty("operator")
    @SerializedName("operator")
    private String operator;

    @JsonProperty("logicOperator")
    @SerializedName("logicOperator")
    private String operatorLogical;
    @JsonProperty("condition")
    @SerializedName("condition")
    private List<NEGroupConditionParamObject> customConditionList;
    @JsonProperty("value")
    @SerializedName("value")
    private List<String> value;
}
