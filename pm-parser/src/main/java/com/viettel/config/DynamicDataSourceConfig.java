package com.viettel.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
        if (configManager.getConfigs() != null) {
            for (Map.Entry<String, ConfigManager.SystemConfig> entry : configManager.getConfigs().entrySet()) {
                if(entry.getValue().isDeploy()) {
                    String systemKey = entry.getKey().toUpperCase();
                    Map<String, ConfigManager.DataSourceProps> dsMap = entry.getValue().getDatasources();
                    if (dsMap == null || dsMap.isEmpty()) continue;
                    for (Map.Entry<String, ConfigManager.DataSourceProps> dsEntry : dsMap.entrySet()) {
                        String dsKey = dsEntry.getKey();
                        ConfigManager.DataSourceProps ds = dsEntry.getValue();
                        if (ds == null) continue;
                        HikariDataSource dataSource = getHikariDataSource(ds);
                        String routingKey = systemKey + ":" + dsKey;
                        targetDataSources.put(routingKey, dataSource);
                    }

                }

            }
        }
        routing.setTargetDataSources(targetDataSources);
        // Set a safe default data source for framework startup (dialect resolution, etc.)
        // Runtime routing still uses context-based keys.
        if (!targetDataSources.isEmpty()) {
            Object first = targetDataSources.values().iterator().next();
            if (first instanceof DataSource) {
                routing.setDefaultTargetDataSource(first);
            }
        }
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


}


