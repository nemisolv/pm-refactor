package com.viettel.ems.perfomance.config;

import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@Component
@ConfigurationProperties(prefix = "systems")
public class ConfigManager {
    public  Map<String, SystemConfig> configs;
    private Map<String, String> taskCrons; // key format: <SYSTEM>.<taskKey>

    @Data
    public static class SystemConfig {
        private Map<String, DataSourceProps> datasources; // multiple DS per system
        private String primaryDatasource; // optional default DS key
        private Map<String, Object> custom; // custom configuration keys
        @Getter
        private boolean deploy;
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
    
    public  String getCustomValue(SystemType system, String key) {
        SystemConfig cfg = getConfig(system);
        if (cfg == null || cfg.getCustom() == null) return null;
        
        Object value = cfg.getCustom().get(key);
        return value != null ? value.toString() : null;
    }
    
    public Boolean getCustomBoolean(SystemType system, String key) {
        String value = getCustomValue(system, key);
        if (value == null) return null;
        return Boolean.parseBoolean(value);
    }
    
    public Integer getCustomInteger(SystemType system, String key) {
        String value = getCustomValue(system, key);
        if (value == null) return null;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    public boolean hasCustomKey(SystemType system, String key) {
        return getCustomValue(system, key) != null;
    }

    // Quick access methods for common configuration values
    
    public Set<String> getSystemNames() {
        return configs != null ? configs.keySet() : Collections.emptySet();
    }
    
    public Set<String> getDatasourceKeys(SystemType system) {
        SystemConfig cfg = getConfig(system);
        return (cfg != null && cfg.getDatasources() != null) 
            ? cfg.getDatasources().keySet() 
            : Collections.emptySet();
    }
    
    public String getDatasourceUrl(SystemType system, String datasourceKey) {
        SystemConfig cfg = getConfig(system);
        if (cfg != null && cfg.getDatasources() != null) {
            DataSourceProps ds = cfg.getDatasources().get(datasourceKey);
            return ds != null ? ds.getUrl() : null;
        }
        return null;
    }
    
    public Set<String> getTaskKeys(SystemType system) {
        if (taskCrons == null || system == null) return Collections.emptySet();
        String prefix = system.name() + ".";
        return taskCrons.keySet().stream()
            .filter(key -> key.startsWith(prefix))
            .map(key -> key.substring(prefix.length()))
            .collect(Collectors.toSet());
    }
    
    public boolean isDeployed(SystemType system) {
        SystemConfig cfg = getConfig(system);
        return cfg != null && cfg.deploy;
    }
    
    public String getPrimaryDatasourceKey(SystemType system) {
        SystemConfig cfg = getConfig(system);
        return cfg != null ? cfg.getPrimaryDatasource() : null;
    }
    
    public Map<String, String> getTaskCrons(SystemType system) {
        if (taskCrons == null || system == null) return Collections.emptyMap();
        String prefix = system.name() + ".";
        return taskCrons.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .collect(Collectors.toMap(
                entry -> entry.getKey().substring(prefix.length()),
                Map.Entry::getValue
            ));
    }
}
