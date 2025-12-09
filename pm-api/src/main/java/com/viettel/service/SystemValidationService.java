package com.viettel.service;

import com.viettel.config.ConfigManager;
import com.viettel.config.LoadingSystemCodeProperties;
import com.viettel.config.SystemType;
import com.viettel.config.TenantContextHolder;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class SystemValidationService {

    private final LoadingSystemCodeProperties loadingSystemCodeProperties;
    private final SystemValidationRepository systemValidationRepository;
    private final ConfigManager configManager;

    private Map<String, Set<String>> systemNeTypeMapRelations;

    @PostConstruct
    public void init() {
        initializeSystemTypeCodesMapping();
        loadAndCacheSystemData();
    }

    private void initializeSystemTypeCodesMapping() {
        log.info("#### Initializing System Type Code Mapping from configuration ####");
        Map<String, String> mapping = loadingSystemCodeProperties.getMapping();
        if (mapping.isEmpty()) {
            log.warn("No system code mappings found in configuration. Enum code will be used default default values");
            return;
        }

        for (SystemType systemType : SystemType.values()) {
            String enumName = systemType.name();
            String oldCode = systemType.getCode();
            String code = mapping.get(enumName);

            if (code != null && !code.isEmpty() && configManager.isDeployed(systemType)) {
                systemType.setCode(code);
                log.info("replace with configured system code from {} to {}", oldCode, code);
            } else {
                log.warn("-> No code mapping found or system {} is not deploy for {}, using default code: {}", enumName, systemType, oldCode);
            }
        }
        log.info("#### System Type Codes initialization complete. ####");
    }

    @Scheduled(cron = "${pm.system-codes.cron-reload-mapping}")
    private void loadAndCacheSystemData() {
        log.info("Reloading System and NE Type validation data from database...");

        Map<String, Set<String>> mergedMap = new HashMap<>();
        String originalThreadName = Thread.currentThread().getName();

        for (SystemType deployedSystem : SystemType.values()) {
            if (configManager.isDeployed(deployedSystem)) {
                TenantContextHolder.setCurrentSystem(deployedSystem);
                String resolvedDs = configManager.resolveDatasourceKey(deployedSystem, null);
                TenantContextHolder.setCurrentDatasourceKey(resolvedDs);

                try {
                    List<SystemNeDto> relations = systemValidationRepository.findAllSystemNeTypeRelations();
                    relations.forEach(dto -> {
                        String key = dto.getSystemName().toUpperCase();
                        mergedMap.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet())
                                .add(dto.getNeTypeName().toUpperCase());
                    });
                    log.info("Found {} relations for system {}", relations.size(), deployedSystem);
                } catch (Exception e) {
                    log.error("Failed to load data for system: {}, Error: {}", deployedSystem, e.getMessage());
                }
            }
        }

        this.systemNeTypeMapRelations = new ConcurrentHashMap<>(mergedMap);
        log.info("Finished loading System and NE Type validation data from database..., total systems with data: {}", systemNeTypeMapRelations.size());

        TenantContextHolder.clear();
        Thread.currentThread().setName(originalThreadName);
    }

    public boolean isValid(String systemType, String neType) {
        if (systemType == null || neType == null) {
            return false;
        }

        Set<String> validNeTypes = systemNeTypeMapRelations.get(systemType.toUpperCase());
        return validNeTypes != null && validNeTypes.contains(neType);
    }
}
