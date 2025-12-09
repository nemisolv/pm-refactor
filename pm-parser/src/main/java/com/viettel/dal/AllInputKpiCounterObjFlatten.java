package com.viettel.dal;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
@Builder
public class AllInputKpiCounterObjFlatten {
    private List<KpiFormula> allKpiObj;
    private List<Counter> allCounterObj;
    private Set<Integer> allKpiCounterId;

}
