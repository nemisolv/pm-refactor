package com.viettel.ems.perfomance.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.viettel.dal.statistical.OnuStandardConfig;
import com.viettel.ems.perfomance.common.Constant;
import com.viettel.ems.perfomance.common.ErrorCode;
import com.viettel.ems.perfomance.config.ConfigManager;
import com.viettel.ems.perfomance.config.SystemType;
import com.viettel.ems.perfomance.object.CounterObject;
import com.viettel.ems.perfomance.object.LiteExtraFieldObject;
import com.viettel.ems.perfomance.object.clickhouse.CounterValueObject;
import com.viettel.ems.perfomance.object.clickhouse.NewFormatCounterObject;
//import com.viettel.ems.perfomance.service.PerformanceManagement;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.*;

@Repository
@RequiredArgsConstructor
@Slf4j
public class KafkaMessageRepository {

    private final ConfigManager configManager;;
    private final KafkaTemplate<String, List<CounterObject>> kafkaTemplate;
    private final KafkaTemplate<String, NewFormatCounterObject> kafkaTemplateNewFormat;
    private final KafkaTemplate<String, JsonNode> kafkaTemplateClickhouse;
    private final Map<String, OnuStandardConfig> configs = new HashMap<>();
    private ExecutorService executorService;
    private final JdbcTemplate jdbcTemplate;



    @PostConstruct
    public void init() {
        initLoadData();
        executorService = new ThreadPoolExecutor(10, 20, 10000, TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<>(100),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Scheduled(cron = "0 0/5 * * * ?")
    public void initLoadData() {
        // nếu không deploy ONT -> bỏ qua
        if(!configManager.isDeployed(SystemType.SYSTEM_ONT)) return;
        Map<String, OnuStandardConfig> configsTmp = new HashMap<>();
       jdbcTemplate.query("SELECT * FROM tbl_onu_standard_config",
               rs -> {
               while(rs.next()) {
                   OnuStandardConfig config = OnuStandardConfig.fromResultSet(rs);
                   configsTmp.put(config.getProductClass() + "_" + config.getType(), config);
               }
               return null;
               });
        configs.keySet().retainAll(configsTmp
                .keySet());
        configs.putAll(configsTmp);
    }



    public ErrorCode sendToClickhouse(NewFormatCounterObject counterObject, Map<String, List<Map<String, Object>>> dataPushClickHouses) {
        Set<String> putTable = new HashSet<>();
        Integer totalTable = 0;
        ErrorCode result = ErrorCode.NO_ERROR;
        try {
            if(counterObject == null || counterObject.getHmGroupCounterValues().isEmpty()) {
                return ErrorCode.ERROR_UNKNOWN;
            }
            totalTable = counterObject.getHmGroupCounterValues().size();
            String recordTime = counterObject.getTime().format(Constant.dtf);
            for(var groupCounterValueEntry : counterObject.getHmGroupCounterValues().entrySet()) {
               for(CounterValueObject counterValueObject : groupCounterValueEntry.getValue().getHmCounterValueObjects().values()) {
                   HashMap<String, Object> row = new HashMap<>();
                   row.put("record_time", recordTime);
                   row.put("ne_id", counterObject.getNeId());
                   row.put("duration", counterObject.getDuration());
                   for(LiteExtraFieldObject extraField: counterValueObject.getHmExtraFields().values()) {
                       row.put(extraField.getColumnCode(), extraField.getColumnValue());
                   }
                   for(int i = 0; i< groupCounterValueEntry.getValue().getLstCounterId().size(); i++) {
                       int counterId = groupCounterValueEntry.getValue().getLstCounterId().get(i);
                       row.put("c_"+ counterId, counterValueObject.getLstCounterValues().get(i));
                   }
                   result = sendMessageToKafka(groupCounterValueEntry.getKey(), row);
                   if("g_wifi".equalsIgnoreCase(groupCounterValueEntry.getKey()) && counterObject.isCheckONUConfig()) {
                       log.debug("Process Compare Value");
//                       executorService.execute(new CompareConfig(counterObject.getPr(), row));
                   }
                   log.debug("Data sent... result= {}, record_time = {}", result, recordTime);
                   if(result != ErrorCode.NO_ERROR) {
                       return result;
                   }
                   if(!dataPushClickHouses.containsKey(groupCounterValueEntry.getKey())) {
                       dataPushClickHouses.put(groupCounterValueEntry.getKey(), new ArrayList<>());
                   }
                   dataPushClickHouses.get(groupCounterValueEntry.getKey()).add(row);
               }
               putTable.add(groupCounterValueEntry.getKey());
            }
            return result;
        }catch (Exception ex) {
            log.error(ex.getMessage());
            return ErrorCode.ERROR_UNKNOWN;
        }finally {
//            if(counterObject != null) {
//                LoggerFactory.getLogger(PerformanceManagement.class).info("{} - Push to ClickHouse Kafka {}/{} table: {}",
//                        counterObject.getFileName(), totalTable, putTable.size(), String.join(",", putTable));
//            }
        }


    }

    public boolean sendMessageToKafka(String topic, NewFormatCounterObject counterObject) {
        try {
            log.debug("Send Message  = {} to kafka via topic = {}", counterObject.toString(), topic);
            kafkaTemplateNewFormat.send(topic, counterObject);
            return true;
        }catch (Exception ex) {
            log.error(ex.getMessage());
            return false;
        }
    }

    public ErrorCode sendMessageToKafka(String topic, Map<String,Object> data) {
        log.debug("Send Message  = {} to kafka via topic = {}", new ObjectMapper().convertValue(data, JsonNode.class), topic);
        ErrorCode result = ErrorCode.NO_ERROR;
        try {
            kafkaTemplateClickhouse.send(topic, new ObjectMapper().convertValue(data, JsonNode.class));
        }catch (Exception ex) {
            log.error(ex.getMessage());
            return ErrorCode.ERROR_UNKNOWN;
        }
        return result;
    }

    public CompletableFuture<?> sendRecordToKafka(String topic, Map<String,Object> data) {
        log.debug("Send Message  = {} to kafka via topic = {}", new ObjectMapper().convertValue(data, JsonNode.class), topic);
       return  kafkaTemplateClickhouse.send(topic, new ObjectMapper().convertValue(data, JsonNode.class));
    }


    // ont don't need for now
    @AllArgsConstructor
    public class CompareConfig implements Runnable {
        private String pr;
        private HashMap<String,String> row;

        @Override
        public void run() {
//            Map<String, String> values = new HashMap<>();
//            Map<String, Object> sendData = new HashMap<>();
//            values.put("Authentication Mode",
//                    getBeaConType(pr, String.value))
        }
    }

}
