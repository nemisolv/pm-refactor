package com.viettel.ems.perfomance.object;

import lombok.Builder;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;

@Data
@Builder

public class CounterCounterCatObject {
    private Integer counterId;
    private int counterCatId;
    private int objectLevelId;
    private String groupCode;
    private boolean isSubKpi;
    private int subCatId;
    private int kpiType;

    

    public static CounterCounterCatObject fromRs(ResultSet rs) throws SQLException {
        return new CounterCounterCatObject(
                rs.getInt("id"),
                rs.getInt("counter_cat_id"),
            rs.getInt("object_level_id"),
            rs.getString("code"),
            rs.getBoolean("is_sub_kpi"),
            rs.getInt("sk_cat_id"),
            rs.getInt("kpi_type")
        );

    }


    // public String buildCounterCounterCatKey() {
    //     return String.format("%d_%d", counterId, objectLevelId);
    // }

    // public static String buildCounterCounterCatKey(Integer counterId, String ratType) {
    //     return String.format("%d_%s", counterId, ratType == null ? "": ratType);
    // }
}
