package com.viettel.dal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmsFilter {
    private int idx;
    private String operator;
    private Object value;

    @JsonProperty("counter_id")
    @SerializedName("counter_id")
    private long id;

    @JsonProperty("logical_operator")
    @SerializedName("logical_opeator")
    private String logicalOperator;

    @JsonProperty("parameter_list")
    @SerializedName("parameter_list")
    private List<EmsFilter> parameterList;


    @JsonProperty("parameter_name")
    @SerializedName("parameter_name")
    private String parameterName;

    private static Map<String, String> operatorMap = Map.of("gt", ">"
    ,"lt", "<", "eq", "=", "ge", ">=", "le", "<=", "not", "!="
    );

    public String toJSString(String name) {
        if(operator.equals("eq") || operator.equals("=")) {
            return name + " == " + value;
        }
        return name + " " +  (operatorMap.containsKey(operator) ? operatorMap.get(operator) : operator) + " " + value;

    }

    public String toSQLString(HashMap<String, ExtraFieldObject> hmExtraField) {
        String columnName = hmExtraField.containsKey(parameterName) 
        ? hmExtraField.get(parameterName).getColumnName() : parameterName;
        String columnType = hmExtraField.containsKey(parameterName) ?
        hmExtraField.get(parameterName).getColumnType() : "string";
        switch(operator.toLowerCase()) {
            case "in":
            case "inlist":
                return columnName + " IN " + getValue(String.valueOf(value),columnType);
            case "contain":
            case "contains":
                return columnName + " LIKE '%" + value + "%'";
            case "like":
                return columnName + " LIKE '" + value + "'";
            case "not like":
                return columnName + " NOT LIKE '%" + value + "%'";
            case "startwith":
                return columnName + " LIKE '" + value + "%'";
            case "endswith":
                return columnName + " LIKE '%" + value + "'";
            default:
                return columnName + " " + (operatorMap.containsKey(operator)
                ? operatorMap.get(operator) : operator) + " "
                + ("string".equalsIgnoreCase(columnType)
                ? getStringQuotedText(String.valueOf(value)) : value); 
        }
    }

    private String getValue(String columnValue, String type) {
        String [] arrValue = columnValue.split(",");
        StringBuilder sbVal = new StringBuilder("(");
        String COMMA_CHAR = "";
        if("string".equalsIgnoreCase(type)) {
            for(String val : arrValue) {
                sbVal.append(COMMA_CHAR).append(getStringQuotedText(val.trim()));
                COMMA_CHAR = ",";
            }
        }else {
            for(String val: arrValue) {
                sbVal.append(COMMA_CHAR).append(val.trim());
                COMMA_CHAR = ",";
            }
        }

        sbVal.append(")");
        return sbVal.toString();
    }


    private String getStringQuotedText(String s) {
        return (s != null && !s.isEmpty()) ? "'" + s + "'" : null;
    }





}