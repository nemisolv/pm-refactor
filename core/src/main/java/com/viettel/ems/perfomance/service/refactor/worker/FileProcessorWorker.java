package com.viettel.ems.perfomance.service.refactor.worker;

import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.config.TenantContextHolder;
import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.parser.better.BetterParserCounter;
import com.viettel.ems.perfomance.service.refactor.CycleStatistics;
import com.viettel.ems.perfomance.service.refactor.object.ProcessingTask;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import com.viettel.ems.perfomance.service.refactor.scanner.FtpService;
import com.viettel.ems.perfomance.service.refactor.scanner.FtpServiceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RequiredArgsConstructor
public class FileProcessorWorker implements Runnable {

    private final int workerId;
    private final BlockingQueue<CounterDataObject> inputFileQueue;
    private final BlockingQueue<ProcessingTask> dbQueue;
    private final BlockingQueue<ProcessingTask> kafkaClickhouseQueue;
    private final FtpServiceManager ftpServiceManager;
    private final CycleStatistics stats;
    private final AtomicReference<BetterParserCounter> parserRef;
    private final FileProcessingTracker tracker;
    private final ConfigManager configManager;

    // Config fields
    private String folderDone;
    private String sysName;
    private boolean isUsingClickhouse;
    private boolean isUsingMySQL;

    @Override
    public void run() {
        loadSystemConfig(); // 1. Tách config load ra cho gọn

        while (!Thread.currentThread().isInterrupted()) {
            try {
                processTask(inputFileQueue.take());
            } catch (InterruptedException e) {
                log.warn("Worker-{} interrupted, shutting down.", workerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Worker-{} unexpected error loop", workerId, e);
            }
        }
    }

    private void loadSystemConfig() {
        SystemType currentSystem = TenantContextHolder.getCurrentSystem();
        this.sysName = currentSystem.name();
        this.folderDone = configManager.getCustomValue(currentSystem, "ftpDoneFolder");
        this.isUsingClickhouse = Boolean.TRUE.equals(configManager.getCustomBoolean(currentSystem, "isUsingClickhouse"));
        this.isUsingMySQL = Boolean.TRUE.equals(configManager.getCustomBoolean(currentSystem, "isUsingMySQL"));
    }

    private void processTask(CounterDataObject task) {
        String fullPath = task.getPath() + "/" + task.getFileName();
        log.debug("[{}] - Processing file: {}", sysName, fullPath);

        FtpService ftpService = null;
        try {
            ftpService = ftpServiceManager.getService(task.getFtpServerObject());

            // 1. Download
            CounterDataObject data = downloadFile(ftpService, task.getPath(), task.getFileName());
            if (data == null) return; // Đã log và update stats trong hàm download

            // 2. Parse
            Map<String, List<UnifiedRecord>> result = parseData(data);
            if (result == null || result.isEmpty()) {
                handleErrorFile(ftpService, task, "File empty or parse failed");
                return;
            }

            // 3. Init Tracking
            int tasksCount = (isUsingMySQL ? 1 : 0) + (isUsingClickhouse ? 1 : 0);
            String trackingKey = sysName + ":" + fullPath;
            tracker.initTracking(task.getPath(), task.getFileName(), tasksCount);

            // 4. Dispatch to Queues
            ProcessingTask pTask = new ProcessingTask(result, task.getPath(), task.getFileName(), task.getFtpServerObject());

            if (isUsingMySQL) {
                dispatchToQueue(dbQueue, pTask, "DB Queue");
            }
            if (isUsingClickhouse) {
                dispatchToQueue(kafkaClickhouseQueue, pTask, "Kafka Clickhouse Queue");
            }

        } catch (Exception e) {
            log.error("Worker-{} logic error on file {}: {}", workerId, fullPath, e.getMessage());
            stats.incrementFailed();
            if (ftpService != null) {
                // Move file sang Done/Error để tránh loop vô tận
                ftpService.moveFileToDone(task.getPath(), task.getFileName(), folderDone);
            }
        }
    }

    private CounterDataObject downloadFile(FtpService ftpService, String path, String fileName) {
        long start = System.currentTimeMillis();
        CounterDataObject data = ftpService.downloadFile(path, fileName);
        stats.recordDownload(System.currentTimeMillis() - start);

        if (data == null) {
            log.warn("[{}] - file download is null, skip: {}/{}", sysName, path, fileName);
            stats.incrementFailed();
        }
        return data;
    }

    private Map<String, List<UnifiedRecord>> parseData(CounterDataObject data) {
        BetterParserCounter parser = parserRef.get();
        if (parser == null) {
            throw new IllegalStateException("Parser configuration is missing!");
        }

        long start = System.currentTimeMillis();
        Map<String, List<UnifiedRecord>> parsedCounterMap = parser.parseCounter(data); // key: tableName, value
        stats.recordParseTime(System.currentTimeMillis() - start);
        return parsedCounterMap;
    }

    // Hàm chung để đẩy vào queue, xử lý việc đầy queue
    private void dispatchToQueue(BlockingQueue<ProcessingTask> queue, ProcessingTask task, String queueName) throws InterruptedException {
        if (!queue.offer(task, 10, TimeUnit.SECONDS)) {
            log.warn("{} full! Worker-{} blocking to push file: {}", queueName, workerId, task.getFileName());
            queue.put(task);
        }
    }

    private void handleErrorFile(FtpService ftpService, CounterDataObject task, String reason) {
        log.warn("Worker-{} {}: {}/{}", workerId, reason, task.getPath(), task.getFileName());
        ftpService.moveFileToDone(task.getPath(), task.getFileName(), folderDone);
        stats.incrementFailed();
    }
}