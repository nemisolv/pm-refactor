package com.viettel.ems.perfomance.repository;

import com.viettel.ems.perfomance.object.ExtraFieldObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
@Slf4j
@RequiredArgsConstructor
public class ExtraFieldRepository {
    private final JdbcTemplate jdbcTemplate;

    public HashMap<Integer, HashMap<String, ExtraFieldObject>> getExtraField() {
        try {
            HashMap<Integer, HashMap<String, ExtraFieldObject>> result = new HashMap<>();
            final String sql = "select ef.id as id, ol.id as object_level_id, ol.name as object_level_name, ef.column_code, ef.column_name," +
                    " ef.column_type, ef.is_visible, ef.is_crud from extra_field ef, object_level ol\n" +
                    " where ef.is_crud = 1 and ef.object_level_id = ol.id\n" +
                    " order by ol.id;";
            jdbcTemplate.query(sql, rs -> {
                ExtraFieldObject extraFieldObject = ExtraFieldObject.fromRs(rs);
                if(result.containsKey(extraFieldObject.getObjectLevelId())) {
                    result.get(extraFieldObject.getObjectLevelId()).put(extraFieldObject.getColumnCode(), extraFieldObject);
                }else {
                    HashMap<String, ExtraFieldObject> extraFieldObjectHashMap = new HashMap<>();
                    extraFieldObjectHashMap.put(extraFieldObject.getColumnCode(), extraFieldObject);
                    result.put(extraFieldObject.getObjectLevelId(), extraFieldObjectHashMap);
                }
            });
            return result;
        }catch (Exception e) {
            log.error("error while retrieving extra field with id: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    public Map<String, Integer> getColumnCodeMapping() {
        try {
            HashMap<String,Integer> result = new HashMap<>();
            final String sql = "select ef.column_code, ol.id as object_level_id from extra_field ef, object_level ol \n"+
                    "where ef.is_crud = 1 and ef.object_level_id = ol.id\n" +
                    "order by ol.id;";
            jdbcTemplate.query(sql, rs -> {
                    result.put(rs.getString("column_code"), rs.getInt("object_level_id"));
            });
            return result;
        }catch (Exception e) {
            log.error("error while retrieving column code mapping, {}", e.getMessage(), e);
            return new HashMap<>();
        }
    }
}
