package com.viettel.config;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class RoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        SystemType system = TenantContextHolder.getCurrentSystem();
        String dsKey = TenantContextHolder.getCurrentDatasourceKey();
        if (system == null) return null;
        if (dsKey == null || dsKey.isEmpty()) {
            return system.name();
        }
        return system.name() + ":" + dsKey;
    }
}


