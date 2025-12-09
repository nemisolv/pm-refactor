package com.viettel.ems.perfomance.object.clickhouse;

import com.viettel.ems.perfomance.object.LiteExtraFieldObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CounterValueObject {
    List<Long> lstCounterValues;
    Map<String, LiteExtraFieldObject> hmExtraFields;
}
