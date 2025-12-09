package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CounterObject {
    private int neId;
    private Timestamp time;
    private Integer duration;
    private Map<String, LiteExtraFieldObject> extraField = new HashMap<>();
    private String sExtraField;
    private Integer counterId;
    private Long counterValue;
    private String groupCode;
    private String ratType;
}


