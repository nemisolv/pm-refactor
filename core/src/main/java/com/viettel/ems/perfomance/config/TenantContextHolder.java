package com.viettel.ems.perfomance.config;

public final class TenantContextHolder {
    private static final ThreadLocal<SystemType> CURRENT_SYSTEM = new ThreadLocal<>();
    private static final ThreadLocal<String> CURRENT_DATASOURCE_KEY = new ThreadLocal<>();

    private TenantContextHolder() {}

    public static void setCurrentSystem(SystemType systemType) {
        CURRENT_SYSTEM.set(systemType);
    }

    public static SystemType getCurrentSystem() {
        return CURRENT_SYSTEM.get();
    }

    public static void setCurrentDatasourceKey(String datasourceKey) {
        CURRENT_DATASOURCE_KEY.set(datasourceKey);
    }

    public static String getCurrentDatasourceKey() {
        return CURRENT_DATASOURCE_KEY.get();
    }

    public static void clear() {
        CURRENT_SYSTEM.remove();
        CURRENT_DATASOURCE_KEY.remove();
    }
}
