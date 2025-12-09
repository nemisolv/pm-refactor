package com.viettel.dal;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class TimeRangeRelative {
    @JsonProperty("unit")
    @SerializedName("unit")
    private String unit;
    @JsonProperty("value")
    @SerializedName("value")
    private Integer value;

    public int toMinute() {
        switch(unit) {
            case "minute":
                return value;
            case "hour":
                return value * 60;
            case "day":
                return value * 24 * 60;
            case "week":
                return value * 7 * 24 * 60;
            case "month":
                return value * 30 * 24 * 60;  
            default:
                return 0;
        
        }
    }

}
