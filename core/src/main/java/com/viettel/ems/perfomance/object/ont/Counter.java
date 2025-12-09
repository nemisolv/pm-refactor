package com.viettel.ems.perfomance.object.ont;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.sql.ResultSet;
import java.sql.SQLException;

@Getter
@Setter
@Accessors(fluent = true)
@RequiredArgsConstructor

public class Counter {
    String name;
    String description;
    Integer id;
    boolean isExtraField = false;

    public static Counter fromRs (ResultSet rs) {
        Counter ce = new Counter();
        try {
            ce.name(rs.getString("ce.name"));
            ce.id(rs.getInt("ce.id"));
            ce.description(rs.getString("ce.description"));
            return ce;
        }catch (SQLException e) {
            return null;
        }
    }
}