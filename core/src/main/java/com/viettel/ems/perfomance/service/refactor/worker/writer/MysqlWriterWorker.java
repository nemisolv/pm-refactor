package com.viettel.ems.perfomance.service.refactor.worker.writer;

import com.viettel.ems.perfomance.service.refactor.CycleStatistics;
import com.viettel.ems.perfomance.service.refactor.object.ProcessingTask;
import com.viettel.ems.perfomance.service.refactor.repository.BetterCounterMySqlRepository;
import com.viettel.ems.perfomance.service.refactor.worker.FileProcessingTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class MysqlWriterWorker implements Runnable {

    private final int workerId;
    private final BlockingQueue<ProcessingTask> dbQueue;
    private final BetterCounterMySqlRepository repository;
    private final CycleStatistics stats;
    private final FileProcessingTracker tracker;
    private final String systemName;

    @Override
    public void run() {
        log.info("DB-Writer-{} started (Single Insert Mode).", workerId);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Lấy task (File) từ queue
                ProcessingTask task = dbQueue.poll(100, TimeUnit.MILLISECONDS);

                if (task != null) {
                    processTask(task);
                }

            } catch (InterruptedException e) {
                log.warn("DB-Writer-{} interrupted.", workerId);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("DB-Writer-{} unexpected error", workerId, e);
            }
        }
    }

    private void processTask(ProcessingTask task) {
        long start = System.currentTimeMillis();
        boolean success = true;

        try {
            // Gọi hàm insert từng dòng mới
            repository.insertSequential(task.getData());

            long duration = System.currentTimeMillis() - start;
            // Record số lượng record (ước lượng)
            int totalRecs = task.getData().values().stream().mapToInt(java.util.List::size).sum();
            stats.recordDbInsert(duration, totalRecs);

        } catch (Exception e) {
            log.error("DB-Writer-{} failed to process file {}: {}", workerId, task.getFileName(), e.getMessage());
            success = false;
            stats.incrementFailed();
        }

        // Báo cáo tracker
        notifyTracker(task, success);
    }

    private void notifyTracker(ProcessingTask task, boolean success) {
        try {
            tracker.completeTask( task.getFilePath(), task.getFileName(), task.getFtpServerObject(), success);
        } catch (Exception e) {
            log.error("Error notifying tracker", e);
        }
    }
}