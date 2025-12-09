package com.viettel.config;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

public enum SystemType {
    SYSTEM_5GC("5GC"),
    SYSTEM_5GA("5GA"),
    SYSTEM_4GA("4GA"),
    SYSTEM_ONT("ONT");
    @Getter
    @Setter
    private String code;
    SystemType(String code) {
        this.code = code;
    }

    public static SystemType fromCode(String code) {
        for(SystemType systemType : SystemType.values()) {
            if(systemType.code.equals(code)) {
                return systemType;
            }
        }
        return null;
    }

}


