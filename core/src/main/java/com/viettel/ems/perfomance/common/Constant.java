package com.viettel.ems.perfomance.common;

import lombok.Getter;

import java.time.format.DateTimeFormatter;
import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public class Constant {
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final int MAX_RETRY_DB = 3;

    @Getter
    public enum NEType {
        ENODEB(0), GNODEB(1), AMF(2), SMF(3), UPF(4), NRF(5), NSSF(6),
        ONT(7), MSC(8), UDM(9), MME(10), SGWC(11), IMS(12), NEF(13),
        NWDAF(14), AUSF(15), UDR(16), PCF(17), GMLC(18), CHF(19), LMF(20),
        AF(21), DN(22);

        private int value;

        NEType(int value) {
            this.value = value;
        }
    }

    @Getter
    public enum CompressionType {
        CAPNPROTO(0), PROTOBUF(1), CSV(2);

        private final int value;

        CompressionType(int value) {
            this.value = value;
        }
    }

    @Getter

    public enum KpiType {
        system_define(0), user_define(1);

        private final int value;

        KpiType(int value) {
            this.value = value;
        }

        private final static Map<Integer, KpiType> map =
                stream(KpiType.values()).collect(toMap(kpiType -> kpiType.value, kpiType -> kpiType));

        public static KpiType valueOf(int value) {
            return map.get(value);
        }
    }
}
