//package com.viettel.ems.perfomance.service;
//
//import com.viettel.ems.perfomance.config.ConfigManager;
//import com.viettel.ems.perfomance.config.ContextAwareExecutor;
//import com.viettel.ems.perfomance.config.SystemConfig;
//import com.viettel.ems.perfomance.config.SystemType;
//import com.viettel.ems.perfomance.object.CounterObject;
//import com.viettel.ems.perfomance.repository.*;
//import jakarta.annotation.PostConstruct;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
////@Component
//// legacy
//@RequiredArgsConstructor
//@Slf4j
//public class SystemManager {
//
//    private static final int DEFAULT_NUM_OF_THREADS = 20;
//    private final Map<SystemType, PerformanceManagement> performanceInstances = new ConcurrentHashMap<>();
//    private final Map<SystemType, ContextAwareExecutor> consumerExecutors = new ConcurrentHashMap<>();
//    private final ConfigManager configManager;
//    private final FTPPathRepository ftpPathRepository;
//    private final CounterMySqlRepository counterMySqlRepository;
//    private final ExtraFieldRepository extraFieldRepository;
//    private final NERepository neRepository;
//    private final CounterCounterCatRepository counterCounterCatRepository;
//    private final ParamsCodeRepository paramsCodeRepository;
//    private final NotificationEngineRedisService notificationEngineRedisService;
//    private final KafkaTemplate<String, List<CounterObject>> kafkaTemplate;
//    private final KafkaMessageRepository kafkaMessageRepository;
//    private final SystemConfig systemConfig;
//    private final DataLakeProcess dataLakeProcess;
//
//
//
//    // the Entrypoint
////    @PostConstruct
//    public void bootstrap() {
//        // Start all systems (5GC, 5GA, 4GA, ONT)
//        log.info("🚀 Bootstrapping System Manager...");
//        for (SystemType systemType : SystemType.values()) {
//            if(configManager.isDeployed(systemType)) {
//                startSystem(systemType);
//            } else {
//                log.info("⚠️ System `{}` is disabled in configuration, skipping startup.", systemType);
//            }
//        }
//        log.info("✅ All systems started successfully");
//    }
//
//
//    public void startSystem(SystemType systemType) {
//        log.info("🚀 Starting system: {} ({})", systemType.getCode(), systemType.getDescription());
//
//
//        Integer consumerThreads = configManager.getCustomInteger(systemType, "consumerThreads");
//
//        if(consumerThreads != null && consumerThreads < 10) {
//            log.warn("The number of configured threads is too small, which can lead to unpredictable behavior.");
//        }
//        if (consumerThreads == null)  {
//            log.warn("consumerThreads is not set, use default: {}",DEFAULT_NUM_OF_THREADS );
//            consumerThreads = DEFAULT_NUM_OF_THREADS; // default
//        }
//
//
//        ExecutorService baseExecutor = Executors.newFixedThreadPool(consumerThreads);
//        ContextAwareExecutor consumerExecutor = new ContextAwareExecutor(baseExecutor, systemType, "PRIMARY");
//
//        // Create and start performanceInstance for this system
//        PerformanceManagement performanceInstance = new PerformanceManagement(
//            ftpPathRepository,
//            counterMySqlRepository,
//            neRepository,
//            counterCounterCatRepository,
//            extraFieldRepository,
//            paramsCodeRepository,
//            notificationEngineRedisService,
//            kafkaTemplate,
//            kafkaMessageRepository,
//            systemConfig,
//            dataLakeProcess,
//            systemType,
//            configManager,
//            consumerExecutor
//                );
//
//        performanceInstances.put(systemType, performanceInstance);
//
//        consumerExecutors.put(systemType, consumerExecutor);
//        // start immediately the system
//        consumerExecutor.submit(performanceInstance);
//
//        log.info("✅ System {} started with Consumer ({} threads)", systemType.getCode(), consumerThreads);
//    }
//
//    public PerformanceManagement getPerformanceInstance(SystemType systemType) {
//        return performanceInstances.get(systemType);
//    }
//
//    public ContextAwareExecutor getExecutor(SystemType systemType) {
//        return consumerExecutors.get(systemType);
//    }
//
//}
