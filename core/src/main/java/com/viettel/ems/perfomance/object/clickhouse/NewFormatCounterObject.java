package com.viettel.ems.perfomance.object.clickhouse;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class NewFormatCounterObject {
    // Basic info
    private Integer neId; // ID of NE
    private LocalDateTime time; // Begin time
    private Integer duration; // Duration: PT900S (900 seconds - 15 minutes), PT300S (300 seconds - 5 minutes)...
    private HashMap<String, GroupCounterValueObject> hmGroupCounterValues;
    @JsonIgnore
    private String fileName;
    @JsonIgnore
    @Builder.Default
    private boolean checkONUConfig = false;
    @JsonIgnore
    private String pr;

    private List<NewFormatCounterObject> newFormatCounterObjectList;

    public GroupCounterValueObject getGroupCounterValueObject(String groupName) {
        return hmGroupCounterValues.getOrDefault(groupName, null);
    }

    public CounterValueObject getCounterValueObject(String groupName, String strExtraField) {
       return hmGroupCounterValues.getOrDefault(groupName, null)
               .getHmCounterValueObjects().getOrDefault(strExtraField, null);
    }

    public void addGroupCounterValueObject(String groupCounterValue) {
        if (!hmGroupCounterValues.containsKey(groupCounterValue)) {
            hmGroupCounterValues.put(groupCounterValue,
                    GroupCounterValueObject.builder()
                           .hmCounterValueObjects(new HashMap<>())
                           .lstCounterId(new ArrayList<>())
                            .build());
        }
    }

    public void addCounterValueObject(String groupName, String strExtraField) {
        addGroupCounterValueObject(groupName);
       if (!hmGroupCounterValues.get(groupName).getHmCounterValueObjects().containsKey(strExtraField)) {
           hmGroupCounterValues.get(groupName).getHmCounterValueObjects().put(strExtraField,
                   CounterValueObject.builder()
                           .hmExtraFields(new HashMap<>())
                           .lstCounterValues(new ArrayList<>())
                           .build());
       }

    }
}
