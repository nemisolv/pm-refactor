package com.viettel.ems.perfomance.service.refactor.worker;

import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.object.FtpServerObject;
import com.viettel.ems.perfomance.service.refactor.scanner.FtpService;
import com.viettel.ems.perfomance.service.refactor.scanner.FtpServiceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

//@Component
@RequiredArgsConstructor
@Slf4j
public class FileProcessingTracker {
    // Key: FilePath, Value: Số lượng task còn lại phải hoàn thành (ví dụ: 2 - MySQL & ClickHouse)
    private final ConcurrentHashMap<String, AtomicInteger> trackingMap = new ConcurrentHashMap<>();
    private final FtpServiceManager ftpServiceManager;
    private final ConfigManager configManager;
    private final SystemType systemType;

    // Khởi tạo tracking cho 1 file
    public void initTracking(String directoryPath, String fileName, int totalTasks) {
        String key = generateKey(directoryPath, fileName);
        trackingMap.put(key, new AtomicInteger(totalTasks));
    }
    private String generateKey(String path, String name) {
        return path + "/" + name;
    }

    // Worker gọi hàm này khi xử lý xong
    public void completeTask(String directoryPath, String fileName, FtpServerObject ftpServerConfig, boolean success) {
       String trackingKey =  generateKey(directoryPath,fileName);
        AtomicInteger remaining = trackingMap.get(trackingKey);
        if (remaining == null) return;

        // Giảm counter
        int current = remaining.decrementAndGet();
        
        if (current == 0) {
            // Cả 2 luồng đã xong -> Move file
            // Lưu ý: Cần logic xử lý nếu 1 trong 2 luồng báo success=false (ví dụ move sang Error thay vì Done)
            finalizeFile(directoryPath, fileName, ftpServerConfig, success);
            trackingMap.remove(trackingKey);
        }
    }

    private void finalizeFile(String directoryPath, String fileName, FtpServerObject config, boolean success) {
        FtpService ftp = ftpServiceManager.getService(config);
//        String targetFolder = success ? "Done" : "Error";
        String targetFolder = configManager.getCustomValue(systemType, "ftpDoneFolder"); // -> All to Done
        boolean moved = ftp.moveFileToDone(directoryPath, fileName, targetFolder);
        if(!moved) {
            log.warn(" Can't move  file: {}",directoryPath +"/" + fileName);
        }
    }
}