package com.viettel.ems.perfomance.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
@Data
public class SystemConfig {
    @Value("${spring.pm.pool_size}")
    private int poolSize;

    @Value("${spring.pm.ftp-max-pool-size:10}")
    private int ftpMaxPoolSize;

    @Value("${spring.pm.precounter_queue_size}")
    private int preCounterQueueSize;

    @Value("${spring.pm.threshold_warning_queue_size}")
    private int thresholdWarningQueueSize;

    @Value("${spring.ipha.host}")
    private String IPHA;

    @Value("${spring.ftp.folder_name_done}")
    private String folderNameFTPDone;
    @Value("${spring.ftp.folder_name_clickhouse}")
    private String folderNameFTPClickhouse;

    @Value("${spring.pm.limit-maximum-file}")
    private int limitMaximumFile;

    @Value("${spring.pm.limit-maximum-file-clickhouse}")
    private int limitMaximumFileClickhouse;

       @Value("${spring.pm.queue-ont-envelop-size}")
    private Integer queueMessageSize;

    @Value("${spring.pm.pool-ont-envelop-size}")
    private Integer poolSizeONT;

    @Value("${spring.pm.pool-ont-envelop-size-max:100}")
    private Integer poolSizeONTMax;

    @Value("${spring.pm.pool-ont-envelop-size-queue:100}")
    private Integer poolSizeONTQueue;

    @Value("${spring.pm.pool-ont-envelop-flush-time-delay:10000}")
    private Long poolFlushONTDelay;

    @Value("${spring.pm.max-number-of-file-to-push}")
    private Integer maxNumberOfFileToPush;



    public static String UE_ETHERNET;
      @Value("${spring.pm.ue_ethernet}")
    public void setUeEthernet(String ueEthernet) {
        UE_ETHERNET = ueEthernet;
    }
    public static String UE_WIFI;

    @Value("${spring.pm.ue_wifi}")
    public void setUeWifi(String ueWifi) {
        UE_WIFI = ueWifi;
    }
    public static int[] LanCounterID;
     @Value("${spring.pm.counter_id}")
    public void setLanCounterID(String lanCounterID) {
        LanCounterID = Arrays.stream(lanCounterID.trim().split(","))
                             .mapToInt(Integer::parseInt)
                             .toArray();
    }
    public static String WIFI_CLIENT_CODE;

    @Value("${spring.pm.wifi_client_code}")
    public void setWifiClientCode(String wifiClientCode) {
        WIFI_CLIENT_CODE = wifiClientCode;
    }
    public static String WIFI_24GHZ;
        @Value("${spring.pm.wifi_24ghz}")
    public void setWifi24ghz(String wifi24ghz) {
        WIFI_24GHZ = wifi24ghz;
    }
    public static String WIFI_5GHZ;
     @Value("${spring.pm.wifi_5ghz}")
    public void setWifi5ghz(String wifi5ghz) {
        WIFI_5GHZ = wifi5ghz;
    }


    @Value("${spring.pm.cache-interval}")
    private String intervalListStr;

   
}
