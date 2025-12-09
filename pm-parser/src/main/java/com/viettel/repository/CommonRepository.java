package com.viettel.repository;

import com.viettel.dal.ExtraFieldObject;
import com.viettel.dal.NEGroupConditionParamObject;
import com.viettel.dal.NeLiteInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Repository
@RequiredArgsConstructor
public class CommonRepository  {
    private final NamedParameterJdbcTemplate namedJdbcTemplate;

    private final JdbcTemplate jdbcTemplate;

    public HashMap<String, ExtraFieldObject> getExtraField(String objectLevelName) {
        try {
            HashMap<String, ExtraFieldObject> result = new HashMap<>();
            jdbcTemplate.query(
                "SELECT ef.id, ef.column_code, ef.column_name, ef.display_name, ef.column_type, ef.is_visible, ef.is_crud, ef.is_key " +
                "FROM extra_field ef, object_level ol " +
                "WHERE ef.object_level_id = ol.id AND ol.name = ? AND is_visible=1",
                ps -> ps.setString(1, objectLevelName),
                rs -> {
                    ExtraFieldObject extraFieldObject = ExtraFieldObject.builder()
                        .id(rs.getInt("id"))
                        .columnCode(rs.getString("column_code").trim().toLowerCase())
                        .columnName(rs.getString("column_name").trim())
                        .displayName(rs.getString("display_name").trim())
                        .columnType(rs.getString("column_type").trim().toLowerCase())
                        .visible(rs.getInt("is_visible"))
                        .crud(rs.getInt("is_crud"))
                        .key(rs.getInt("is_key"))
                        .build();
                    result.put(extraFieldObject.getColumnCode(), extraFieldObject);
                });
            return result;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            return new HashMap<>();
        }
    }

    public HashMap<String, ExtraFieldObject> getExtraField() {
        try {
            HashMap<String, ExtraFieldObject> result = new HashMap<>();
            jdbcTemplate.query(
                "SELECT id, column_code, column_name, display_name, column_type, is_visible, is_crud, is_key " +
                "FROM extra_field WHERE is_visible=1",
                rs -> {
                    ExtraFieldObject extraFieldObject = ExtraFieldObject.builder()
                        .id(rs.getInt("id"))
                        .columnCode(rs.getString("column_code").trim().toLowerCase())
                        .columnName(rs.getString("column_name").trim())
                        .displayName(rs.getString("display_name").trim())
                        .columnType(rs.getString("column_type").trim().toLowerCase())
                        .visible(rs.getInt("is_visible"))
                        .crud(rs.getInt("is_crud"))
                        .key(rs.getInt("is_key"))
                        .build();
                    result.put(extraFieldObject.getColumnCode(), extraFieldObject);
                });
            return result;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            return new HashMap<>();
        }
    }






    public Map<Integer, NeLiteInfo> getListNeNameByIds(
    List<Integer> lstNeId, // ne id list from input (optional)
     List<String> listNEGroupCode, 
     String NEGroupConditionParam, 
     List<NEGroupConditionParamObject> neGroupConditionParamList) {
        try {
          Map<Integer, NeLiteInfo> result = new HashMap<>();
          if(lstNeId !=  null && lstNeId.size() > 0) {
            HashMap<String,Object> hmNeId = new HashMap<>();
            
            hmNeId.put("lstNE", lstNeId);
            String sqlQueryNEInfo = "SELECT id, name FROM ne WHERE id IN (:lstNE)";
            namedJdbcTemplate.query(sqlQueryNEInfo, hmNeId, (rs) -> {
                NeLiteInfo neLiteInfo = NeLiteInfo.builder()
                .neID(rs.getInt("id"))  
                .name(rs.getString("name"))
                .build();
                result.put(neLiteInfo.getNeID(), neLiteInfo);
             
            });
          }else if (neGroupConditionParamList != null && neGroupConditionParamList.size() > 0){
            StringBuilder conditionParam = new StringBuilder();
            boolean isFirstNode = true;
            for(NEGroupConditionParamObject neGroupConditionParamObject : neGroupConditionParamList) {
                if( neGroupConditionParamObject.getCustomConditionList() != null && neGroupConditionParamObject.getCustomConditionList().size() > 0){
                    isFirstNode =  true;
                    for(NEGroupConditionParamObject customCondition : neGroupConditionParamObject.getCustomConditionList()){
                        buildNeGroupConditionSql(customCondition, conditionParam, isFirstNode);
                    }
                }else{
                    buildNeGroupConditionSql(neGroupConditionParamObject, conditionParam, isFirstNode);
                }
                conditionParam.append(")");
                isFirstNode = false;
            }
            if(!conditionParam.toString().isEmpty()){
                conditionParam.append(") AND ");
            }
            conditionParam.append(" is_active = 1");
            String sqlQueryNEInfo = "SELECT id, name FROM ne WHERE (" + conditionParam.substring(3);
            namedJdbcTemplate.query(sqlQueryNEInfo, rs -> {
                NeLiteInfo neLiteInfo = NeLiteInfo.builder()
                .neID(rs.getInt("id"))  
                .name(rs.getString("name"))
                .build();
                result.put(neLiteInfo.getNeID(), neLiteInfo);
             
            });
          }else if (listNEGroupCode != null && listNEGroupCode.size() > 0 && NEGroupConditionParam != null && NEGroupConditionParam.trim().length() > 0){
            HashMap<String, Object> hmNEGroupCode = new HashMap<>();
            hmNEGroupCode.put("lstNeGroup", listNEGroupCode);
            String sqlQueryNEInfo = "SELECT id, name FROM ne WHERE " + NEGroupConditionParam.trim() + " IN (:lstNeGroup) and is_active = 1";
            namedJdbcTemplate.query(sqlQueryNEInfo, hmNEGroupCode, (rs) -> {
                NeLiteInfo neLiteInfo = NeLiteInfo.builder()
                .neID(rs.getInt("id"))  
                .name(rs.getString("name"))
                .build();
                result.put(neLiteInfo.getNeID(), neLiteInfo);
            });
          }
          log.info("Log result:: {}", result);
          return result;
        }catch(Exception e){
            log.error("getListNeNameByIds error: {}", e.getMessage());
        }
        return null;
    }

    private static void buildNeGroupConditionSql(NEGroupConditionParamObject customCondition, StringBuilder conditionParam, boolean isFirstNode) {
    if (customCondition.getOperatorLogical() == null || customCondition.getOperatorLogical().isEmpty()) {
        customCondition.setOperatorLogical("or");
    }
    
    conditionParam.append(customCondition.getOperatorLogical() != null ? customCondition.getOperatorLogical() : "").append(" ");
    
    if (isFirstNode) {
        conditionParam.append("(");
    }
    
    conditionParam.append(customCondition.getParam()).append(" ");
    
    switch (customCondition.getOperator()) {
        case "eq":
            conditionParam.append("=");
            break;
        case "neq":
            conditionParam.append("!=");
            break;
        case "contain":
        case "contains":
            conditionParam.append("like");
            break;
        default:
            conditionParam.append(customCondition.getOperator());
            break;
    }
    
    conditionParam.append(" ");
    
    if (customCondition.getOperator().equals("in")) {
        conditionParam.append("(")
                     .append(customCondition.getValue().stream()
                                              .map(value -> "'" + value + "'")
                                              .collect(Collectors.joining(", ")))
                     .append(") ");
    } else {
        conditionParam.append(customCondition.getValue().stream()
                                                 .map(value -> (customCondition.getOperator().equals("like") ||
                                                               customCondition.getOperator().equals("not like"))
                                                     ? ("%" + value + "%")
                                                     : ("'" + value + "'"))
                                                 .collect(Collectors.joining(", "))).append(" ");
    }
}
}
