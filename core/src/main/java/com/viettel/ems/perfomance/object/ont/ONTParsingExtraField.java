package com.viettel.ems.perfomance.object.ont;

import com.viettel.ems.perfomance.object.ont.CounterCat;
import com.viettel.ems.perfomance.object.ont.MeasDataObject;
import com.viettel.ems.perfomance.object.ont.MessageCPEObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public class ONTParsingExtraField {
    private CounterCat counterCat;
    private Map<String, List<MessageCPEObject>> hmLstExtraField = new HashMap<>();
    private Map<String, Map<String, MessageCPEObject>> hmLstCounter = new HashMap<>();
    private List<String> lstCounterName;
    private MeasDataObject measDataObject;
    private boolean allCounterIsNull = true;
}