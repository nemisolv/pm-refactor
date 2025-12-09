package com.viettel.ems.perfomance.service.refactor.worker.writer;

import com.viettel.ems.perfomance.config.TenantContextHolder;
import com.viettel.ems.perfomance.repository.KafkaMessageRepository;
import com.viettel.ems.perfomance.service.refactor.CycleStatistics;
import com.viettel.ems.perfomance.service.refactor.object.ProcessingTask;
import com.viettel.ems.perfomance.service.refactor.object.UnifiedRecord;
import com.viettel.ems.perfomance.service.refactor.worker.FileProcessingTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class KafkaProducerWorker implements Runnable {

    private final int workerId;
    private final BlockingQueue<ProcessingTask> kafkaQueue;
    private final KafkaMessageRepository kafkaMessageRepository;
    private final FileProcessingTracker tracker;
    private final CycleStatistics stats;

    // Cần systemName để tạo key tracking khớp với FileProcessorWorker
    private  String systemName;

    @Override
    public void run() {
        systemName = TenantContextHolder.getCurrentSystem().name();
        log.info("Kafka-Producer-{} started for system {}", workerId, systemName);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                ProcessingTask task = kafkaQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task != null) {
                    processTask(task);
                }
            } catch (InterruptedException e) {
                log.warn("Kafka-Producer-{} interrupted.", workerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Kafka-Producer-{} loop error", workerId, e);
            }
        }
    }

    private void processTask(ProcessingTask task) {
        String fileFullPath = task.getFilePath() + "/" + task.getFileName();
        long start = System.currentTimeMillis();
        int totalRecordsSent = 0;

        try {
            // 1. Lấy dữ liệu đã được parse và group theo bảng
            // Key: Table Name (Topic), Value: List các bản ghi
            Map<String, List<UnifiedRecord>> dataMap = task.getData();

            if (dataMap == null || dataMap.isEmpty()) {
                log.warn("Kafka-Producer-{} No data for file: {}", workerId, fileFullPath);
                notifyTracker(task, true);
                return;
            }

            List<CompletableFuture<?>> futures = new ArrayList<>();

            // 2. Duyệt qua từng bảng (Topic)
            for (Map.Entry<String, List<UnifiedRecord>> entry : dataMap.entrySet()) {
                String tableName = entry.getKey(); // Tên bảng cũng là tên Topic (hoặc mapping tùy config)
                List<UnifiedRecord> records = entry.getValue();

                // 3. Duyệt từng bản ghi, flatten và gửi
                for (UnifiedRecord record : records) {
                    // Convert sang Flat Map (JSON phẳng cho ClickHouse)
                    Map<String, Object> flatJson = record.toFlatMap();

                    CompletableFuture<?> future = kafkaMessageRepository.sendRecordToKafka(tableName, flatJson);
                    futures.add(future);
                }
                totalRecordsSent += records.size();
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            long duration = System.currentTimeMillis() - start;
             log.debug("Kafka-Producer-{} sent {} records for {} in {}ms", workerId, totalRecordsSent, fileFullPath, duration);

            // 4. Báo cáo thành công
            notifyTracker(task, true);

        } catch (Exception e) {
            log.error("Kafka-Producer-{} failed to send file {}: {}", workerId, fileFullPath, e.getMessage());
            stats.incrementFailed();
            // 5. Báo cáo thất bại
            notifyTracker(task, false);
        }
    }

    private void notifyTracker(ProcessingTask task, boolean success) {
        try {

            tracker.completeTask( task.getFilePath(), task.getFileName(), task.getFtpServerObject(), success);

            if (success) {
                stats.incrementHandleSuccess(); // Có thể tách riêng stat cho Kafka nếu cần
            }
        } catch (Exception e) {
            log.error("Error notifying tracker for file {}", task.getFileName(), e);
        }
    }
}