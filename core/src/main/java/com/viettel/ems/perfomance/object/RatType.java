package com.viettel.ems.perfomance.object;

import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.config.TenantContextHolder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Getter
public enum RatType {
        NR_TDD("NR_TDD"),
        NR_FDD("NR_FDD"),
        LTE_TDD("LTE_TDD"),
        LTE_FDD("LTE_FDD"),
        UNKNOWN("UNKNOWN");
        
        private final String value;

        private static final String DEFAULT_5G_ONLY = "1";

        RatType(final String value) {
            this.value = value;
        }

        private static final Map<String, RatType> ratTypeMap = new HashMap<>();

        static {
            for(RatType ratType : RatType.values()) {
                ratTypeMap.put(ratType.getValue(), ratType);
            }
        }

        public static String fromNodeFunction(String nodeFunctionRaw) {
            SystemType systemType = TenantContextHolder.getCurrentSystem();
            if(nodeFunctionRaw == null || nodeFunctionRaw.isEmpty()) {
                log.warn("NodeFunction is absent, use the defaultl value for {}", systemType);
                if(SystemType.SYSTEM_4GA.equals(systemType)) return LTE_FDD.getValue();
                else if(SystemType.SYSTEM_5GA.equals(systemType)) return NR_FDD.getValue();
                return UNKNOWN.getValue() + ":" + nodeFunctionRaw;
            }
            String normalized = nodeFunctionRaw.trim();
            // 5g use 1 fore NR_FDD by default
            if(normalized.equals(DEFAULT_5G_ONLY)) {
                log.debug("rat_type =1, DEFAULT_5G_ONLY");
                return NR_FDD.getValue();
            }

            // in case the rat_type is NR(LTE)-FDD(TDD), it can be backward compatibility
            String v = normalized.toUpperCase().replace('-', '_');
            try {
                RatType t = ratTypeMap.get(v);
                return t.getValue();
            }catch(IllegalArgumentException e) {
                if(SystemType.SYSTEM_4GA.equals(systemType)) return LTE_FDD.getValue();
                else if(SystemType.SYSTEM_5GA.equals(systemType)) return NR_FDD.getValue();
                return UNKNOWN.getValue() + ":" + nodeFunctionRaw ;

            }


        }
    }