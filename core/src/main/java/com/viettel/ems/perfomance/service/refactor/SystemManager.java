package com.viettel.ems.perfomance.service.refactor;

import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.ContextAwareExecutor;
import com.viettel.ems.perfomance.config.SystemType;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class SystemManager {

    private static final int DEFAULT_NUM_OF_THREADS = 20;

    private final ConfigManager configManager;

    // Factory để tạo ra các instance PerformanceManagement mới (Prototype)
    private final ObjectProvider<PerformanceManagement> performanceManagementProvider;

    // Map quản lý các instance đang chạy để có thể shutdown sau này
    private final Map<SystemType, PerformanceManagement> performanceInstances = new ConcurrentHashMap<>();
    private final Map<SystemType, ContextAwareExecutor> consumerExecutors = new ConcurrentHashMap<>();

    @PostConstruct
    public void bootstrap() {
        log.info("=================================================");
        log.info("🚀 BOOTSTRAPPING SYSTEM MANAGER");
        log.info("=================================================");

        for (SystemType systemType : SystemType.values()) {
            if (configManager.isDeployed(systemType)) {
                try {
                    startSystem(systemType);
                } catch (Exception e) {
                    log.error("❌ Failed to start system {}", systemType, e);
                }
            } else {
                log.info("⚠️ System `{}` is disabled in configuration.", systemType);
            }
        }
        log.info("✅ Bootstrapping completed.");
    }

    public void startSystem(SystemType systemType) {
        log.info("🚀 Starting system: {} ({})", systemType.getCode(), systemType.getDescription());

        // 1. Config Thread Pool Size
        Integer consumerThreads = configManager.getCustomInteger(systemType, "consumerThreads");
        if (consumerThreads == null || consumerThreads <= 0) {
            log.warn("consumerThreads not set for {}, using default: {}", systemType, DEFAULT_NUM_OF_THREADS);
            consumerThreads = DEFAULT_NUM_OF_THREADS;
        }

        // 2. Init Executor Service
        ExecutorService baseExecutor = Executors.newFixedThreadPool(consumerThreads);
        ContextAwareExecutor consumerExecutor = new ContextAwareExecutor(baseExecutor, systemType, configManager.getPrimaryDatasourceKey(systemType));

        // 3. Tạo Instance từ Spring Factory (Prototype)
        PerformanceManagement pm = performanceManagementProvider.getObject();

        // 4. Init các thông số riêng biệt cho instance này
        pm.init(systemType, consumerExecutor);

        // 5. Lưu tham chiếu và Chạy
        performanceInstances.put(systemType, pm);
        consumerExecutors.put(systemType, consumerExecutor);

        // Submit task chính vào executor (Async start)
        consumerExecutor.submit(pm);

        log.info("✅ System {} started successfully with {} threads.", systemType.getCode(), consumerThreads);
    }

    @PreDestroy
    public void shutdown() {
        log.info("🛑 Shutting down all systems...");
        // Dừng logic nghiệp vụ (đóng LeaderLatch, etc.)
        performanceInstances.values().forEach(PerformanceManagement::shutdown);

        // Dừng Thread Pool
        consumerExecutors.values().forEach(executor -> {
            try {
                executor.shutdown();
            } catch (Exception e) {
                log.error("Error shutting down executor", e);
            }
        });
        log.info("🛑 Shutdown completed.");
    }
}