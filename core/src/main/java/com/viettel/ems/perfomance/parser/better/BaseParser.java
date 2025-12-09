package com.viettel.ems.perfomance.parser.better;

import com.viettel.ems.perfomance.object.CounterCounterCatObject;
import com.viettel.ems.perfomance.object.ExtraFieldObject;
import com.viettel.ems.perfomance.object.NEObject;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public abstract class BaseParser implements ICounterParser {
    
    protected final Map<String, NEObject> neObjectMap;
    protected final Map<Integer, CounterCounterCatObject> counterCounterCatObjectMap;
    protected final Map<Integer, HashMap<String, ExtraFieldObject>> extraFieldMap;

    // Logic filter extra field dùng chung cho mọi parser
    protected Map<String, Object> filterExtraFields(String tableName, Map<String, Object> rawDims) {
        Map<String, Object> filtered = new HashMap<>();
        Integer objectLevelId = getObjectLevelIdByTable(tableName);

        if (objectLevelId != null) {
            HashMap<String, ExtraFieldObject> config = extraFieldMap.get(objectLevelId);
            if (config != null) {
                rawDims.forEach((k, v) -> {
                    if (config.containsKey(k)) {
                        filtered.put(config.get(k).getColumnName(), v);
                    }
                });
            }
        }
        return filtered;
    }

    protected Integer getObjectLevelIdByTable(String tableName) {
        return counterCounterCatObjectMap.values().stream()
                .filter(c -> c.getGroupCode().equals(tableName))
                .map(CounterCounterCatObject::getObjectLevelId)
                .findFirst()
                .orElse(null);
    }
    
    protected String getSfpIndex(String counterName) {
        for (int i = 1; i <= 4; i++) {
            if (counterName.contains("sfp" + i)) return String.valueOf(i);
        }
        return "-1";
    }
}