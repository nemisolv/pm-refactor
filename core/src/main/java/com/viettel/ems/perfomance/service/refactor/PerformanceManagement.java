package com.viettel.ems.perfomance.service.refactor;

import com.viettel.ems.perfomance.config.*;
import com.viettel.ems.perfomance.object.CounterDataObject;
import com.viettel.ems.perfomance.parser.better.BetterParserCounter;
import com.viettel.ems.perfomance.repository.FTPPathRepository;
import com.viettel.ems.perfomance.repository.KafkaMessageRepository;
import com.viettel.ems.perfomance.service.refactor.helper.ServiceUpdater;
import com.viettel.ems.perfomance.service.refactor.object.ProcessingTask;
import com.viettel.ems.perfomance.service.refactor.repository.BetterCounterMySqlRepository;
import com.viettel.ems.perfomance.service.refactor.scanner.FileProducer;
import com.viettel.ems.perfomance.service.refactor.scanner.FtpServiceManager;
import com.viettel.ems.perfomance.service.refactor.worker.FileProcessingTracker;
import com.viettel.ems.perfomance.service.refactor.worker.FileProcessorWorker;
import com.viettel.ems.perfomance.service.refactor.worker.writer.KafkaProducerWorker;
import com.viettel.ems.perfomance.service.refactor.worker.writer.MysqlWriterWorker;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
@Scope("prototype")
public class PerformanceManagement implements Runnable {

    // ================= DEPENDENCIES =================
    @Autowired
    private FTPPathRepository ftpPathRepository;
    @Autowired
    private FtpServiceManager ftpServiceManager;
    @Autowired
    private CuratorFramework curatorClient;
    @Autowired
    private ServiceUpdater serviceUpdater;
    @Autowired
    private BetterCounterMySqlRepository betterCounterMySqlRepository;
    @Autowired
    private KafkaMessageRepository kafkaMessageRepository;

    @Autowired
    private SystemConfig systemConfig;
    @Autowired
    private ConfigManager configManager;

    // ================= CONFIG VALUES =================
    private String cronExpressionStr;
    private Long immediateRunDelay;

    // ================= INSTANCE FIELDS =================
    private SystemType systemType;
    private ContextAwareExecutor executorService;
    private CycleStatistics stats;

    // ================= STATE =================
    private LeaderLatch leaderLatch;
    private volatile boolean isLeader = false;

    // Queue 1: File cần xử lý (Scanner -> FileWorker)
    private BlockingQueue<CounterDataObject> queueCounterDataFiles;

    // Queue 2.1: Dữ liệu cần Insert DB nếu có cấu hình isUsingMySQL (FileWorker -> DbWorker)
    private BlockingQueue<ProcessingTask> dbQueue;


    // Queue 2.2: Dữ liệu cần publish sang kafka nếu có cấu hình isUsingClickhouse (FileWorker -> KafkaWorker)
    private BlockingQueue<ProcessingTask> kafkaQueue;


    private boolean isUsingMySQL;
    private boolean isUsingClickhouse;

    @Getter
    @Setter
    private Date lastTimeUpdateCounterInfo;

    private final AtomicReference<BetterParserCounter> parserRef = new AtomicReference<>();

    public void init(SystemType systemType, ContextAwareExecutor executorService) {
        this.systemType = systemType;
        this.executorService = executorService;
        if (systemType == null || executorService == null) {
            throw new IllegalStateException("PerformanceManagement not initialized! Call init() first.");
        }
        int filesHandlingPerCycle = configManager.getCustomInteger(systemType, "filesHandlingPerCycle");
        dbQueue = new ArrayBlockingQueue<>(filesHandlingPerCycle);
        kafkaQueue = new ArrayBlockingQueue<>(filesHandlingPerCycle);
        queueCounterDataFiles = new ArrayBlockingQueue<>(filesHandlingPerCycle);

        isUsingClickhouse = configManager.getCustomBoolean(systemType, "isUsingClickhouse");
        isUsingMySQL = configManager.getCustomBoolean(systemType, "isUsingMySQL");
        cronExpressionStr = configManager.getCustomValue(systemType, "scheduleScan");
        immediateRunDelay = Long.parseLong(configManager.getCustomValue(systemType, "immediateRunDelaySecond"));
        log.info("[{}] - config: " +
                        "\n isUsingClickhouse: {}, isUsingMySQL: {}," +
                        "\n cronExpressionStr: {}, immediateRunDelay: {}" +
                        "\nfilesHandlingPerCycle: {}",
                systemType.name(), isUsingClickhouse, isUsingMySQL,
                cronExpressionStr, immediateRunDelay,
                filesHandlingPerCycle
                );


    }

    @Override
    public void run() {

        log.info("🚀 [{}] Booting up PerformanceManagement...", systemType);

        // 1. Init Stats
        this.stats = new CycleStatistics(systemType);

        // 2. Init File Processing Tracker
        FileProcessingTracker tracker = new FileProcessingTracker(ftpServiceManager, configManager, systemType);

        // 2. Init Parser
        log.info("[{}] Initializing parser...", systemType);
        parserRef.set(serviceUpdater.updateCounterInfo(lastTimeUpdateCounterInfo));
        lastTimeUpdateCounterInfo = new Date();

        // 3. Start Workers (Pipeline)
        // --- A. DB Workers (Tầng 1: Insert DB - mysql) ---

        if (isUsingMySQL) {
            int dbWorkerCount = 4;
            log.info("[{}] Spawning {} DB Writer threads...", systemType, dbWorkerCount);
            for (int i = 0; i < dbWorkerCount; i++) {
                MysqlWriterWorker dbWorker = new MysqlWriterWorker(
                        i, dbQueue, betterCounterMySqlRepository, stats, tracker, systemType.name()
                );
                executorService.submit(dbWorker);
            }
        }

        // --- B. Kafka Workers (Tầng 2: Gửi data sang ClickHouse qua Kafka) ---
        if (isUsingClickhouse) {
            int kafkaWorkerCount = 2; // Số lượng worker cho Kafka
            log.info("[{}] Spawning {} Kafka Producer threads...", systemType, kafkaWorkerCount);
            for (int i = 0; i < kafkaWorkerCount; i++) {
                KafkaProducerWorker kafkaWorker = new KafkaProducerWorker(
                        i, kafkaQueue, kafkaMessageRepository, tracker, stats
                );
                executorService.submit(kafkaWorker);
            }
        }

        // --- C. File Workers (Tầng 1: Download & Parse) ---
        int fileWorkerCount = 20; // Số lượng nhiều để tải file
        log.info("[{}] Spawning {} File Processor threads...", systemType, fileWorkerCount);
        for (int i = 0; i < fileWorkerCount; i++) {
            FileProcessorWorker worker = new FileProcessorWorker(
                    i, queueCounterDataFiles, dbQueue, kafkaQueue, ftpServiceManager, stats, parserRef, tracker, configManager
            );
            executorService.submit(worker);
        }

        // 4. Leader Election
        setupLeaderElection();

        // 5. Init Scanner
        FileProducer fileProducer = new FileProducer(
                stats, ftpPathRepository, ftpServiceManager, systemType, queueCounterDataFiles
        );

        // -----------------------------------------------------------
        // 6. SCHEDULING
        // -----------------------------------------------------------

        // Task chạy ngay (Immediate Run)
        executorService.getScheduledExecutorService().schedule(() -> {
            if (isLeader) {
                log.info("⚡ [{}] TRIGGERING IMMEDIATE FIRST RUN...", systemType);
                runScanCycle(fileProducer);
            } else {
                log.info("💤 [{}] Initial run skipped (Not Leader).", systemType);
            }
        }, immediateRunDelay, TimeUnit.SECONDS);

        // Task chạy định kỳ (Cron)
        log.info("⏰ [{}] Scheduling Cron Task with expression: [{}]", systemType, cronExpressionStr);
        scheduleNextCronRun(fileProducer);

        log.info("✅ [{}] Engine fully started.", systemType);
    }

    private void scheduleNextCronRun(FileProducer fileProducer) {
        try {
            CronExpression cronExpression = CronExpression.parse(cronExpressionStr);
            ZonedDateTime now = ZonedDateTime.now();
            ZonedDateTime nextRun = cronExpression.next(now);

            if (nextRun == null) {
                log.error("[{}] Cron expression {} will never fire again!", systemType, cronExpressionStr);
                return;
            }

            long delayMs = Duration.between(now, nextRun).toMillis();
            if (delayMs < 1000) {
                ZonedDateTime nextNextRun = cronExpression.next(nextRun);
                if (nextNextRun != null) {
                    delayMs = Duration.between(now, nextNextRun).toMillis();
                    nextRun = nextNextRun;
                }
            }

            log.info("[{}] Next Cron Scan scheduled at: {} (in {} ms)", systemType, nextRun, delayMs);

            executorService.getScheduledExecutorService().schedule(() -> {
                try {
                    if (isLeader) {
                        // In báo cáo thống kê của chu kì trước
                        stats.reportAndReset();
                        log.info("⏰ [{}] Executing Cron Scan...", systemType);
                        runScanCycle(fileProducer);

                    }
                } catch (Exception e) {
                    log.error("[{}] Cron execution failed", systemType, e);
                } finally {
                    scheduleNextCronRun(fileProducer);
                }
            }, delayMs, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            log.error("[{}] Failed to schedule next cron run", systemType, e);
        }
    }

    /**
     * Hàm giám sát tiến độ xử lý của Worker
     * Nó sẽ tự gọi lại chính nó mỗi 2 giây cho đến khi xử lý xong hết file
     */


    private void runScanCycle(FileProducer fileProducer) {
        try {
            TenantContextHolder.setCurrentSystem(systemType);
            log.debug("[{}] Updating counter info...", systemType);

            BetterParserCounter newParser = serviceUpdater.updateCounterInfo(lastTimeUpdateCounterInfo);
            lastTimeUpdateCounterInfo = new Date();
            if (newParser != null) {
                parserRef.set(newParser);
            }

            fileProducer.run();

        } catch (Exception e) {
            log.error("[{}] Scan cycle failed", systemType, e);
        } finally {
            TenantContextHolder.clear(); // Clear context để tránh leak sang thread khác nếu pool reuse
        }
    }

    private void setupLeaderElection() {
        try {
            String latchPath = "/pm_processor/leader/" + systemType.getCode();
            String instanceId = localInstanceId();
            leaderLatch = new LeaderLatch(curatorClient, latchPath, instanceId);
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    isLeader = true;
                    log.info("[{}] I AM LEADER NOW ({}). Scanner active.", systemType, instanceId);
                }

                @Override
                public void notLeader() {
                    isLeader = false;
                    log.info("💤 [{}] I AM FOLLOWER ({}). Scanner paused.", systemType, instanceId);
                }
            });
            leaderLatch.start();
        } catch (Exception e) {
            log.error("Failed to start LeaderLatch for {}", systemType, e);
        }
    }

    public void shutdown() {
        try {
            if (leaderLatch != null) leaderLatch.close();
        } catch (IOException e) {
            log.error("Error closing LeaderLatch", e);
        }
    }

    private String localInstanceId() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }
}