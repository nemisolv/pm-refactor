package com.viettel.ems.perfomance.config;

import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
public class RoutingContextExecutor {

    public void runWith(SystemType systemType, String datasourceKey, Runnable runnable) {
        SystemType prevSystem = TenantContextHolder.getCurrentSystem();
        String prevDs = TenantContextHolder.getCurrentDatasourceKey();
        try {
            if (systemType != null) {
                TenantContextHolder.setCurrentSystem(systemType);
            }
            if (datasourceKey != null) {
                TenantContextHolder.setCurrentDatasourceKey(datasourceKey);
            }
            runnable.run();
        } finally {
            if (prevSystem == null) {
                TenantContextHolder.clear();
            } else {
                TenantContextHolder.setCurrentSystem(prevSystem);
                TenantContextHolder.setCurrentDatasourceKey(prevDs);
            }
        }
    }

    public <T> T callWith(SystemType systemType, String datasourceKey, Supplier<T> supplier) {
        SystemType prevSystem = TenantContextHolder.getCurrentSystem();
        String prevDs = TenantContextHolder.getCurrentDatasourceKey();
        try {
            if (systemType != null) {
                TenantContextHolder.setCurrentSystem(systemType);
            }
            if (datasourceKey != null) {
                TenantContextHolder.setCurrentDatasourceKey(datasourceKey);
            }
            return supplier.get();
        } finally {
            if (prevSystem == null) {
                TenantContextHolder.clear();
            } else {
                TenantContextHolder.setCurrentSystem(prevSystem);
                TenantContextHolder.setCurrentDatasourceKey(prevDs);
            }
        }
    }
}
