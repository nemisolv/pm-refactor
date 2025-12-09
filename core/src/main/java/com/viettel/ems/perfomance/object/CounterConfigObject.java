package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CounterConfigObject {
    private int measurementIdentifier;
    private String measurementObject;
    private String measurementGroup;
    private HashMap<Integer, LiteCounterObject> hmLiteCounter;
}
