package com.viettel.ems.perfomance.config;

import lombok.Getter;

@Getter
public enum SystemType {
    SYSTEM_5GC("5GC", "5G Core Network"),
    SYSTEM_ONT("ONT", "Optical Network Terminal"),
    SYSTEM_4GA("4GA", "4G Access Network"),
    SYSTEM_5GA("5GA", "5G Access Network");
    
    private final String code;
    private final String description;
    
    SystemType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static SystemType fromString(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return SystemType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

}
