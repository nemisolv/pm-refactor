package com.viettel.dal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KpiFormula {
    private int kpiID;
    private String kpiName;
    private String description;
    private String formula;
    private String units;
    private String createdDate;
    private String createBy;
    private int status;
    private int kpiType;
    private int accountID;
    private String originalFormula;
    private boolean peakHour;
    private String arrCounter;
    private boolean singleGroup;
    private String arrGroup;
    private String arrKpi;
    private List<KpiFormula> lstSubKpi;
    // field for caching
    private String skFormula;
    private int skCatID;

    @Override
    public int hashCode() {
        return this.kpiID;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public KpiFormula(int kpiID, String kpiName, String description, String formula, String units, String createdDate, String createBy, int status, int kpiType, int accountID, String originalFormula, boolean peakHour, String arrCounter, boolean singleGroup, String arrGroup, String arrKpi) {
        this.kpiID = kpiID;
        this.kpiName = kpiName;
        this.description = description;
        this.formula = formula;
        this.units = units;
        this.createdDate = createdDate;
        this.createBy = createBy;
        this.status = status;
        this.kpiType = kpiType;
        this.accountID = accountID;
        this.originalFormula = originalFormula;
        this.peakHour = peakHour;
        this.arrCounter = arrCounter;
        this.singleGroup = singleGroup;
        this.arrGroup = arrGroup;
        this.arrKpi = arrKpi;
        this.lstSubKpi = new ArrayList<>();
        this.skFormula = null;
        this.skCatID = -1;
    }
}
