package com.viettel.config;

import org.springframework.context.ApplicationContext;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
@Configuration
@EnableScheduling
@RequiredArgsConstructor
public class SystemScheduler implements SchedulingConfigurer {

    private final ConfigManager configManager;
    private final ApplicationContext applicationContext;
    private final SystemManager systemManager;

    @Override
    public void configureTasks(@NonNull ScheduledTaskRegistrar taskRegistrar) {
        log.info("Configuring system scheduler");
        Map<String, ConfigManager.SystemConfig> configs = configManager.getConfigs();
        if (configs == null || configs.isEmpty()) return;

        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(org.springframework.stereotype.Component.class);
        for (Object bean : beans.values()) {
            Method[] methods = bean.getClass().getMethods();
            for (Method method : methods) {
                SystemScheduled anno = method.getAnnotation(SystemScheduled.class);
                if (anno == null) continue;
                String taskKey = anno.key();
                String taskDsKey = anno.datasource();
                for (String systemKey : configs.keySet()) {
                    SystemType systemType = SystemType.valueOf(systemKey);
                    if (!configManager.isDeployed(systemType)) {
                        log.debug("Skip schedule for system `{}` due to disable", systemType.getCode());
                        continue;
                    }
                    String cron = configManager.resolveCron(systemType, taskKey);
                    ConfigManager.SystemConfig sysCfg = configs.get(systemKey);
                    boolean enabled = sysCfg != null && sysCfg.isDeploy();
                    if (!enabled || cron == null || cron.isEmpty()) continue;

                    ScheduledExecutorService sharedExecutor = systemManager.getExecutor(systemType).getScheduledExecutorService();

                    taskRegistrar.setScheduler(sharedExecutor);
                    Runnable task = () -> runAnnotated(bean, method, systemType, taskDsKey);
                    taskRegistrar.addCronTask(task, cron);
                    log.info("Registered task {} for system {} cron {}", taskKey, systemType, cron);
                }
            }
        }
    }

    private void runAnnotated(Object bean, Method method, SystemType systemType, String desiredDsKey) {
        String originalThreadName = Thread.currentThread().getName();
        try {
            // Set thread name to include system info
            Thread.currentThread().setName(String.format("SYS-%s-%s-%s",
                    systemType.getCode(),
                    desiredDsKey != null ? desiredDsKey : "PRIMARY",
                    originalThreadName));

            TenantContextHolder.setCurrentSystem(systemType);
            String dsKey = configManager.resolveDatasourceKey(systemType, desiredDsKey);
            if (dsKey == null || dsKey.isEmpty()) {
                log.warn("Skip task {}.{} for system {} due to ambiguous datasource (set primaryDatasource or annotation datasource)", bean.getClass().getName(), method.getName(), systemType);
                return;
            }
            TenantContextHolder.setCurrentDatasourceKey(dsKey);
            method.invoke(bean);
        } catch (Exception ex) {
            log.error("Error running scheduled task for system {} bean {} method {}", systemType, bean.getClass().getName(), method.getName(), ex);
        } finally {
            // Restore original thread name
            Thread.currentThread().setName(originalThreadName);
            TenantContextHolder.clear();
        }
    }
}
