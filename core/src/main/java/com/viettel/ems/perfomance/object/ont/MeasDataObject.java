package com.viettel.ems.perfomance.object.ont;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MeasDataObject {
    private String measInfoName;
    private String measTypes;
    private List<MeasValuesObject> measValues;
    public MeasDataObject(String measInfoName) {
        this.measInfoName = measInfoName;
    }
}