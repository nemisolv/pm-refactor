package com.viettel.ems.perfomance.object.ont;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
@RequiredArgsConstructor

public class CounterCat {
    String name;
    Integer id;
    String description ;
    Integer objectLevelId;

    Map<Integer , Counter> counters;
    Map<String, ExtraField> extraFields;
    List<Integer> businessCodes;

    Map<Integer, Integer> counterDataReceived;
    Map<String, Object> counterDataMultiReceived;
    Map<String, Object> extraFieldDataReceived;

    public static CounterCat fromRs(ResultSet rs) {
        CounterCat cc = new CounterCat();
        try {
            cc.name(rs.getString("cc.name"));
            cc.id(rs.getInt("cc.id"));
            cc.description(rs.getString("cc.description"));
            cc.objectLevelId(rs.getInt("cc.object_level_id"));
            cc.counters(new HashMap<>());
            cc.counterDataMultiReceived(new HashMap<>());
            cc.counterDataReceived(new HashMap<>());
            cc.extraFields(new HashMap<>());
            cc.extraFieldDataReceived(new HashMap<>());
            cc.businessCodes(new ArrayList<>());

            return cc;
        }catch(SQLException e) {
            return null;
        }
    }
}
