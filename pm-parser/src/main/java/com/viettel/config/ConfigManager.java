package com.viettel.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
@ConfigurationProperties(prefix = "systemsapi")
public class ConfigManager {
    private Map<String, SystemConfig> configs;
    private Map<String, String> taskCrons; // key format: <SYSTEM>.<taskKey>

    @Data
    public static class SystemConfig {
        private Map<String, DataSourceProps> datasources; // multiple DS per system
        private String primaryDatasource; // optional default DS key
        private boolean deploy;
        // private ScheduleProps schedule;

        private Map<String, Object> custom; // custom configuration keys
    }

    @Data
    public static class DataSourceProps {
        private String url;
        private String username;
        private String password;
        private String driverClassName;
        private Integer maximumPoolSize = 10;
        private Long connectionTimeoutMs = 60000L;
    }

    @Data
    public static class ScheduleProps {
        private boolean enabled = true;
    }

    public String resolveCron(SystemType system, String taskKey) {
        if (taskCrons != null && system != null && taskKey != null) {
            String v = taskCrons.get(system.name() + "." + taskKey);
            if (v != null && !v.isEmpty()) return v;
        }
        return null;
    }

    public SystemConfig getConfig(SystemType systemType) {
        if (systemType == null) return null;
        SystemConfig cfg = configs.get(systemType.name());
        if (cfg == null) {
            cfg = configs.get(systemType.name().toLowerCase());
        }
        return cfg;
    }

    public boolean isDeployed(SystemType systemType) {
        SystemConfig config = getConfig(systemType);
        return config != null && config.isDeploy();
    }





    public String resolveDatasourceKey(SystemType system, String desiredKey) {
        SystemConfig cfg = getConfig(system);
        if (cfg == null || cfg.getDatasources() == null || cfg.getDatasources().isEmpty()) return null;
        if (desiredKey != null && !desiredKey.isEmpty() && cfg.getDatasources().containsKey(desiredKey)) {
            return desiredKey;
        }
        if (cfg.getPrimaryDatasource() != null && cfg.getDatasources().containsKey(cfg.getPrimaryDatasource())) {
            return cfg.getPrimaryDatasource();
        }
        if (cfg.getDatasources().size() == 1) {
            return cfg.getDatasources().keySet().iterator().next();
        }
        return null; // ambiguous
    }

    // Quick access methods for custom configuration keys

    /**
     * Get a custom configuration value as String
     */
    public String getCustomValue(SystemType system, String key) {
        SystemConfig cfg = getConfig(system);
        if (cfg == null || cfg.getCustom() == null) return null;

        Object value = cfg.getCustom().get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Get a custom configuration value as Boolean
     */
    public Boolean getCustomBoolean(SystemType system, String key) {
        String value = getCustomValue(system, key);
        if (value == null) return null;
        return Boolean.parseBoolean(value);
    }

    public String getTheCurrentSystemConfigDB() {
        SystemType systemType = TenantContextHolder.getCurrentSystem();
        SystemConfig cfg = getConfig(systemType);
        if (cfg == null || cfg.getCustom() == null) return null;
        String dbType = (String) cfg.getCustom().get("dbType");
        return dbType;
    }

    /**
     * Get a custom configuration value as Integer
     */
    public Integer getCustomInteger(SystemType system, String key) {
        String value = getCustomValue(system, key);
        if (value == null) return null;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Check if a custom key exists for a system
     */
    public boolean hasCustomKey(SystemType system, String key) {
        return getCustomValue(system, key) != null;
    }
}


