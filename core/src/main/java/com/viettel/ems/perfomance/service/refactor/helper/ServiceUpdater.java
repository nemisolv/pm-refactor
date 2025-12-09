package com.viettel.ems.perfomance.service.refactor.helper;

import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.SystemConfig;
import com.viettel.ems.perfomance.config.TenantContextHolder;
import com.viettel.ems.perfomance.object.*;
import com.viettel.ems.perfomance.parser.better.BetterParserCounter;
import com.viettel.ems.perfomance.repository.CounterCounterCatRepository;
import com.viettel.ems.perfomance.repository.ExtraFieldRepository;
import com.viettel.ems.perfomance.repository.NERepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ServiceUpdater {
    private final CounterCounterCatRepository counterCounterCatRepository;
    private final ExtraFieldRepository extraFieldRepository;
    private final ConfigManager configManager;
    private final SystemConfig systemConfig;
    private final NERepository nERepository;
    private BetterParserCounter parserCounterData;
    private Map<Integer, CounterCounterCatObject> counterCounterCatMap;
    private HashMap<Integer, HashMap<String, ExtraFieldObject>> extraFieldMap;
//    private HashMap<Integer, CounterCatObject> counterCatMap;
    private final HashMap<String, NEObject> activeNeMap = new HashMap<>();
    private HashMap<String, CounterConfigObject> hmCounterConfig;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    @Value("${spring.pm.is-preprocessing-data}")
    private boolean isProcessingData;


    private List<String> cacheIntervalList;


    @PostConstruct
    public void init() {
//        counterCatMap = new HashMap<>();
        cacheIntervalList = Arrays.stream(systemConfig.getIntervalListStr().split(",")).map(String::trim).collect(Collectors.toList());
    }


    private void getCounterInfo() {
        String sysName = TenantContextHolder.getCurrentSystem().name();
        List<CounterCounterCatObject> lstGroupCounter = counterCounterCatRepository.findAll();
        HashMap<Integer, CounterCounterCatObject> tmpHmCounterCounterCat = new HashMap<>();
        if(lstGroupCounter != null && !lstGroupCounter.isEmpty()) {
            lstGroupCounter.forEach(item -> tmpHmCounterCounterCat.put(item.getCounterId(), item));
            counterCounterCatMap = tmpHmCounterCounterCat;
        }
        // get list extra field
        extraFieldMap = extraFieldRepository.getExtraField();
        // oran
        hmCounterConfig= counterCounterCatRepository.findCounterOran();
        log.info("[{}] groupCounter, size = {}", sysName, counterCounterCatMap.size());
        log.info("[{}] hmExtraField, size= {}", sysName,extraFieldMap == null ? 0 : extraFieldMap.size());
        log.info("[{}] hmCounterConfig, size = {}", sysName,hmCounterConfig == null ? 0 : hmCounterConfig.size());
    }

    public BetterParserCounter updateCounterInfo(Date lastTimeUpdateCounterInfo) {
        try {
            String sysName = TenantContextHolder.getCurrentSystem().name();

            updateActiveNe();
            getCounterInfo();
            // initialized parsercounterdata
            parserCounterData = new BetterParserCounter(activeNeMap, counterCounterCatMap, extraFieldMap, hmCounterConfig);


            boolean isAutoAddingColumn = Optional.ofNullable(configManager.getCustomBoolean(TenantContextHolder.getCurrentSystem(), "isAutoAddingColumn")).orElse(Boolean.FALSE);
            if(!isAutoAddingColumn) return parserCounterData;
            List<CounterCatObject> lstCounterCatObjectUpdate = counterCounterCatRepository.getUpdatedCounterCat(lastTimeUpdateCounterInfo != null ? dateFormat.format(lastTimeUpdateCounterInfo) : null);
            List<CounterCounterCatObject> lstCounterCounterCatObjectUpdate = counterCounterCatRepository.getUpdatedCounterCounterCat(lastTimeUpdateCounterInfo != null ? dateFormat.format(lastTimeUpdateCounterInfo) : null);
            log.info("[{}] {} new updated counter cat: {}",sysName, lstCounterCatObjectUpdate.size(), lstCounterCatObjectUpdate.stream().map(counterCatObject -> String.valueOf(counterCatObject.getCode())).collect(Collectors.joining(",")));
            log.info("[{}] {} new updated Counter: {}",sysName, lstCounterCounterCatObjectUpdate.size(), lstCounterCounterCatObjectUpdate.stream().map(counterCounterCatObject -> String.valueOf(counterCounterCatObject.getCounterId())).collect(Collectors.joining(",")));
            for(CounterCatObject counterCatObject : lstCounterCatObjectUpdate) {
                try {
                    if(!counterCatObject.isSubCat()) {
                        counterCounterCatRepository.addCounterCatTableToDb(counterCatObject.getCode(),
                                extraFieldMap.get(counterCatObject.getObjectLevelId()),
                                counterCounterCatMap.values().stream().filter(
                                        counterCounterCatObject -> counterCounterCatObject.getCounterCatId() == counterCatObject.getId()
                                ).map(CounterCounterCatObject::getCounterId).collect(Collectors.toList()),
                                counterCatObject.isSubCat()
                        );
//                        counterCatMap.put(counterCatObject.getId(), counterCatObject);
                    }
                } catch (Exception e) {
                    log.error("Error while updateing counter cat with id: {}",counterCatObject.getId(), e);
                }

                if(isProcessingData) {
                    for(String interval : cacheIntervalList) {
                        if(!counterCatObject.isSubCat()) {
                            counterCounterCatRepository.addCounterCatTableToDb(String.format("%s_%s", counterCatObject.getCode(), interval),
                                    extraFieldMap.get(counterCatObject.getObjectLevelId()),
                                    counterCounterCatMap.values().stream().filter(
                                            counterCounterCatObject -> counterCounterCatObject.getCounterId() == counterCatObject.getId()
                                    ).map(CounterCounterCatObject::getCounterId).collect(Collectors.toList()), counterCatObject.isSubCat());
                        }else {
                            for(Constant.KpiType kpiType : Constant.KpiType.values()) {
                                counterCounterCatRepository.addCounterCatTableToDb(String.format("%s_%s_%s", counterCatObject.getCode(), kpiType, interval),
                                        extraFieldMap.get(counterCatObject.getObjectLevelId()),
                                        counterCounterCatMap.values().stream().filter(
                                                        counterCounterCatObject -> counterCounterCatObject.getSubCatId() == counterCatObject.getId()
                                                                && counterCounterCatObject.getKpiType() == kpiType.getValue()
                                                ).map(CounterCounterCatObject::getCounterId)
                                                .collect(Collectors.toList()), counterCatObject.isSubCat());
                            }
                        }
                    }
                }
            }
            counterCounterCatRepository.addCounterToTable(lstCounterCounterCatObjectUpdate, counterCounterCatMap, extraFieldMap, "");
            if(isProcessingData) {
                for(String interval : cacheIntervalList) {
                    counterCounterCatRepository.addCounterToTable(lstCounterCounterCatObjectUpdate, counterCounterCatMap, extraFieldMap, interval);
                }
            }
        }catch(Exception e) {
            log.error("Error while updating counter: {}",e.getMessage(), e);

        }

        return parserCounterData;
    }


    private void updateActiveNe() {
        List<NEObject> lstNEObject = nERepository.findAllNeActive();
        if(lstNEObject != null && !lstNEObject.isEmpty()) {
            HashMap<String, NEObject> lstNeActTmp = new HashMap<>();
            lstNEObject.forEach(item -> lstNeActTmp.put(item.getName(), item));
            activeNeMap.keySet().retainAll(lstNeActTmp.keySet());
            activeNeMap.putAll(lstNeActTmp);
        }
    }
}
