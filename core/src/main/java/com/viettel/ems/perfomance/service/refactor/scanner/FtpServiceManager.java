package com.viettel.ems.perfomance.service.refactor.scanner;

import com.viettel.ems.perfomance.config.SystemConfig;
import com.viettel.ems.perfomance.object.FtpServerObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class FtpServiceManager {
    // Cache: Key là IP_PORT (hoặc ID server), Value là FtpService (chứa Pool)
    private final Map<String, FtpService> serviceMap = new ConcurrentHashMap<>();
    private final SystemConfig systemConfig;

    public FtpService getService(FtpServerObject config) {
        String key = config.getKey(); // Giả sử key là "Host_Port"

        // Pattern "Double-checked locking" hoặc computeIfAbsent để tạo Pool 1 lần duy nhất
        return serviceMap.computeIfAbsent(key, k -> {
            int poolSize = systemConfig.getFtpMaxPoolSize();
            log.info("Creating FTP connection pool for {} with max size: {}", config.getHost(), poolSize);
            return new FtpService(config, poolSize);
        });
    }
    
    // Gọi khi shutdown app để đóng pool
    public void shutdown() {
        serviceMap.values().forEach(FtpService::shutdown);
    }
}