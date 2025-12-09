package com.viettel.ems.perfomance.service.refactor.scanner;

import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.object.FTPPathObject;
import com.viettel.ems.perfomance.repository.FTPPathRepository;
import com.viettel.ems.perfomance.service.refactor.CycleStatistics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * FileProducer (Producer)
 * Nhiệm vụ: Chạy định kỳ, quét tên file từ FTP Server, lọc theo SystemType
 * và đẩy metadata vào hàng đợi để Worker xử lý.
 */
@Slf4j
@RequiredArgsConstructor
public class FileProducer implements Runnable {
    private final CycleStatistics stats;
    private final FTPPathRepository ftpPathRepository;
    private final FtpServiceManager ftpServiceManager;
    private final SystemType systemType;

    // Hàng đợi dùng chung để giao tiếp với Consumer (Worker)
    private final BlockingQueue<CounterDataObject> outputQueue;



    // --- Cấu hình Cache ---
    private static final long CACHE_TTL_MS = 60 * 60 * 1000L; // 1 giờ
    private List<FTPPathObject> cachedPaths;
    private long lastCacheTime = 0;

    @Override
    public void run() {
        // already check leader in the Caller
        scanAndProduce();
        if(outputQueue.isEmpty()) {
            log.info("{}: No file found!", systemType.name());
        }
    }

    private void scanAndProduce() {
        try {
            List<FTPPathObject> paths = getFtpPathsWithCache();
            if (paths == null || paths.isEmpty()) return;

            for (FTPPathObject pathObj : paths) {
                // true: tiếp tục, false: dừng scan (do queue đầy)
                boolean shouldContinue = processPath(pathObj);

                if (!shouldContinue) {
                    log.warn("[{}] Queue full. Stopping scan cycle early.", systemType);
                    break; // Break khỏi vòng lặp PATHS -> Dừng chu kỳ quét này
                }
            }
        } catch (Exception e) {
            log.error("Critical error in FileProducer scan loop", e);
        }
    }

    /**
     * Trả về false nếu Queue đầy và cần dừng quét.
     */
    private boolean processPath(FTPPathObject pathObj) {
        try {
            FtpService ftpService = ftpServiceManager.getService(pathObj.getFtpServerObject());
            if (ftpService == null) return true;

            String path = pathObj.getPath();
            List<String> fileNames = ftpService.getListFileNames(path);

            if (fileNames == null || fileNames.isEmpty()) {
                log.debug("[{}] Path {} has 0 files. Skipping.", systemType, path);
                return true;
            }
            stats.addScannedCount(fileNames.size());

            int currentRead = 0;
            int totalFiles = fileNames.size();

            for (String fileName : fileNames) {
                // Filter logic
                if (!isTargetFile(fileName)) continue;

                CounterDataObject task = new CounterDataObject();
                task.setFileName(fileName);
                task.setPath(path);
                task.setFtpServerObject(pathObj.getFtpServerObject());

                boolean added = outputQueue.offer(task);

                if (!added) {
                    //  Log rõ ràng tại sao dừng: Queue đầy
                    log.warn("[{}] Queue full (size={}). Stopped scanning {} after adding {}/{} files.",
                            systemType, outputQueue.size(), path, currentRead, totalFiles);
                    return false; // <--- Báo hiệu dừng toàn bộ chu kỳ quét
                }
                stats.incrementQueued(); // Đếm số file đẩy thành công
                currentRead++;
            }

            // Chỉ log INFO nếu có file thực sự được đẩy đi
            if (currentRead > 0) {
                log.info("[{}] Scanned {}: queued {}/{} files.", systemType, path, currentRead, totalFiles);
            } else {
                // Có file trong folder nhưng không khớp systemType (ví dụ 5GA quét trúng folder toàn file LTE)
                log.debug("[{}] Scanned {}: 0/{} files matched target.", systemType, path, totalFiles);
            }

            return true; // Path này xong, tiếp tục path khác

        } catch (Exception e) {
            log.error("[{}] Error scanning path: {}", systemType, pathObj.getPath(), e);
            return true; // Lỗi path này thì vẫn thử path khác
        }
    }

    /**
     * Logic lọc file dựa trên SystemType để tránh xung đột giữa 4GA và 5GA
     */
    private boolean isTargetFile(String fileName) {
        String upperName = fileName.toUpperCase();

        if (SystemType.SYSTEM_5GA.equals(systemType)) {
            // 5GA: Bỏ qua các file có tag _LTE_ (Nghĩa là lấy _NR_ và file thường)
            return !upperName.contains("_LTE_");
        }
        else if (SystemType.SYSTEM_4GA.equals(systemType)) {
            // 4GA: Chỉ lấy các file có tag _LTE_
            return upperName.contains("_LTE_");
        }

        // Các hệ thống khác (ONT, 5GC) lấy tất cả hoặc thêm logic tùy ý
        return true;
    }


    /**
     * Lấy danh sách FTP Path có sử dụng Local Cache 1 giờ.
     */
    private List<FTPPathObject> getFtpPathsWithCache() {
        long now = System.currentTimeMillis();

        // Nếu chưa có cache hoặc cache đã hết hạn (quá 1h) -> Query DB
        if (cachedPaths == null || (now - lastCacheTime > CACHE_TTL_MS)) {
            try {
                log.info("[{}] Cache expired or empty. Reloading FTP Paths from DB...", systemType);
                List<FTPPathObject> newPaths = ftpPathRepository.findAll();

                if (newPaths != null && !newPaths.isEmpty()) {
                    this.cachedPaths = newPaths;
                    this.lastCacheTime = now;
                    log.info("[{}] FTP Path cache updated. Size: {}", systemType, newPaths.size());
                } else {
                    log.warn("[{}] DB returned empty paths. Keeping old cache if available.", systemType);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to reload FTP paths from DB. Using old cache.", systemType, e);
                // Nếu DB lỗi, giữ nguyên cache cũ để hệ thống không chết hẳn
            }
        }
        return cachedPaths != null ? cachedPaths : Collections.emptyList();
    }
}