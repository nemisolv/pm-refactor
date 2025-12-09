package com.viettel.ems.perfomance.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class DynamicDataSourceConfig {

    private final ConfigManager configManager;

    @Bean(name = "routingDataSource")
    public DataSource routingDataSource() {
        RoutingDataSource routing = new RoutingDataSource();
        Map<Object, Object> targetDataSources = new HashMap<>();
        DataSource defaultDataSource = null;
        if (configManager.getConfigs() != null) {
            for (Map.Entry<String, ConfigManager.SystemConfig> entry : configManager.getConfigs().entrySet()) {
                String systemKey = entry.getKey().toUpperCase();
                if(!configManager.isDeployed(SystemType.fromString(systemKey))) {
                    continue;
                }

                Map<String, ConfigManager.DataSourceProps> dsMap = entry.getValue().getDatasources();
                if (dsMap == null || dsMap.isEmpty()) continue;
                String primaryKey = entry.getValue().getPrimaryDatasource();
                for (Map.Entry<String, ConfigManager.DataSourceProps> dsEntry : dsMap.entrySet()) {
                    String dsKey = dsEntry.getKey();
                    ConfigManager.DataSourceProps ds = dsEntry.getValue();
                    if (ds == null) continue;
                    HikariDataSource dataSource = getHikariDataSource(ds);
                    String routingKey = systemKey + ":" + dsKey;
                    targetDataSources.put(routingKey, dataSource);

                    // Register short key for primary
                    if (primaryKey != null && primaryKey.equals(dsKey)) {
                        targetDataSources.put(systemKey, dataSource);
                        if (defaultDataSource == null) {
                            defaultDataSource = dataSource;
                        }
                    }
                }
            }
        }
        routing.setTargetDataSources(targetDataSources);
        // Set a safe default data source for framework startup (dialect resolution, etc.)
        if (defaultDataSource == null && !targetDataSources.isEmpty()) {
            // pick the first available as a last resort
            defaultDataSource = (DataSource) targetDataSources.values().iterator().next();
        }
        if (defaultDataSource != null) {
            routing.setDefaultTargetDataSource(defaultDataSource);
        }
        routing.afterPropertiesSet();
        return routing;
    }

    private static HikariDataSource getHikariDataSource(ConfigManager.DataSourceProps ds) {
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(ds.getUrl());
        hikari.setUsername(ds.getUsername());
        hikari.setPassword(ds.getPassword());
        hikari.setDriverClassName(ds.getDriverClassName());
        if (ds.getMaximumPoolSize() != null) hikari.setMaximumPoolSize(ds.getMaximumPoolSize());
        if (ds.getConnectionTimeoutMs() != null) hikari.setConnectionTimeout(ds.getConnectionTimeoutMs());
        return new HikariDataSource(hikari);
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


}


