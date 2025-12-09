package com.viettel.ems.perfomance.object.clickhouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class GroupCounterValueObject {
    List<Integer> lstCounterId;
    HashMap<String, CounterValueObject> hmCounterValueObjects;
}
