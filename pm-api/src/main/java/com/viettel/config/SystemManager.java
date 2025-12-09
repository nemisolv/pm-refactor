package com.viettel.config;

import com.viettel.schedule.ScheduleService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.SchedulerException;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Service
@Slf4j
@RequiredArgsConstructor
public class SystemManager {

    private static final int DEFAULT_NUM_OF_THREADS = 20;

    private final Map<SystemType, ContextAwareExecutor> consumerExecutors = new ConcurrentHashMap<>();
    private final ConfigManager configManager;
    private final RoutingContextExecutor routingContextExecutor;
    private final ScheduleService scheduleService;

    // the Entry point
    @PostConstruct
    public void bootstrap() {
        // Start all systems (5GC, 5GA, 4GA, ONT)
        log.info("✅ Bootstrapping System Manager...");
        for (SystemType systemType : SystemType.values()) {
            if (configManager.isDeployed(systemType)) {
                log.info("deploy: {}", systemType);
                registerSystem(systemType);
            } else {
                log.info("⚠️ System {} is disabled in configuration, skipping startup.", systemType);
            }
        }
        log.info("✅ All systems started successfully");
    }

    public void registerSystem(SystemType systemType) {
        Integer consumerThreads = configManager.getCustomInteger(systemType, "consumerThreads");

        if (consumerThreads != null && consumerThreads < 10) {
            log.warn("The number of configured threads is too small, which can lead to unpredictable");
        }
        if (consumerThreads == null) {
            log.warn("consumerThreads is not set, use default: {}", DEFAULT_NUM_OF_THREADS);
            consumerThreads = DEFAULT_NUM_OF_THREADS; // default
        }

        ExecutorService baseExecutor = Executors.newFixedThreadPool(consumerThreads, new ThreadFactory() {
            private int seq = 0;

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(String.format("SYS-%s-Worker-%d", systemType.getCode(), ++seq));
                return t;
            }
        });
        ContextAwareExecutor consumerExecutor = new ContextAwareExecutor(baseExecutor, systemType,  "PRIMARY");

        consumerExecutors.put(systemType, consumerExecutor);

        consumerExecutor.submit(() -> routingContextExecutor.runWith(systemType,  "PRIMARY", () -> {
            try {
                scheduleService.runOne();
            } catch (SchedulerException e) {
                throw new RuntimeException(e);
            }
        }));

        log.info("✅ System {} started with Consumer ({} threads)", systemType.getCode(), consumerThreads);
    }

    public ContextAwareExecutor getExecutor(SystemType systemType) {
        return consumerExecutors.get(systemType);
    }
}
