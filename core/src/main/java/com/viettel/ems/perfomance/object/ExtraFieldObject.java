package com.viettel.ems.perfomance.object;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ExtraFieldObject {
    private Integer id;
    private int objectLevelId;
    private String objectLevelName;
    private String columnCode;
    private String columnName;
    private String columnType;
    private int visible;
    private int crud;


    public static ExtraFieldObject fromRs(ResultSet rs) throws SQLException {
        return ExtraFieldObject.builder()
                .id(rs.getInt("id"))
                .objectLevelId(rs.getInt("object_level_id"))
                .objectLevelName(rs.getString("object_level_name").trim().toLowerCase())
                .columnCode(rs.getString("column_code").trim().toLowerCase())
                .columnName(rs.getString("column_name").trim())
                .columnType(rs.getString("column_type").trim().toLowerCase())
                .visible(rs.getInt("is_visible"))
                .crud(rs.getInt("is_crud"))
                .build();
    }

}
