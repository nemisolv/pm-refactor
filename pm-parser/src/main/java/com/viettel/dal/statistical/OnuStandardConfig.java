
package com.viettel.dal.statistical;


import com.google.gson.reflect.TypeToken;
import com.viettel.util.OtherFilterUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public class OnuStandardConfig {
    private Integer id;
    private String productClass;
    private String type;
    private String configType = "WIFI";

    private List<OnuStandardConfigRule> rules = new ArrayList<>();
    private Map<String, OnuStandardConfigRule> rulesMap = new HashMap<>();

    public static OnuStandardConfig fromResultSet(ResultSet rs) throws SQLException {
        OnuStandardConfig config = new OnuStandardConfig();
        config.setId(rs.getInt("id"));
        config.setProductClass(rs.getString("product_class"));
        config.setType(rs.getString("type"));
        config.setConfigType(rs.getString("config_type"));
        config.fromStringRule(rs.getString("rules"));
        return config;
    }

    public void fromStringRule(String jsonRules) {
        try {
//            this.rules = new ArrayList<>(OtherFilterUtil.gson.fromJson(jsonRules, new TypeToken<List<OnuStandardConfigRule>>() {

//            }.getType()));
//            rules.forEach(rule -> rulesMap.put(rule.getKeyConfig(), rule));
        }catch (Exception e) {
            this.rules = new ArrayList<>();
        }
    }

}