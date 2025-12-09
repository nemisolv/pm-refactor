package com.viettel.ems.perfomance.object.ont;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MeasValuesObject  {
    private String measObjLdn;
    private String measResults;
    
    public MeasValuesObject(String measObjLdn) {this.measObjLdn = measObjLdn;}
}