package com.viettel.ems.perfomance.object;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CounterCatObject {
    private Integer id;
    private String code;
    private int objectLevelId;
    private String createdDate;
    private String updatedDate;
    private boolean isSubCat;


    public static CounterCatObject fromRs(ResultSet rs) throws SQLException {
        return CounterCatObject.builder()
                .id(rs.getInt("id"))
                .code(rs.getString("code"))
                .objectLevelId(rs.getInt("object_level_id"))
                .isSubCat(rs.getBoolean("is_sub_kpi_cat"))
                .build();
    }

}
