//package com.viettel.ems.perfomance.service;
//
//
//import com.viettel.ems.perfomance.common.Constant;
//import com.viettel.ems.perfomance.common.ErrorCode;
//import com.viettel.ems.perfomance.config.*;
//import com.viettel.ems.perfomance.object.*;
//import com.viettel.ems.perfomance.repository.*;
//import com.viettel.ems.perfomance.object.clickhouse.NewFormatCounterObject;
//import com.viettel.ems.perfomance.parser.ParseCounterDataONT;
//import com.viettel.ems.perfomance.parser.ParserCounterData;
//import com.viettel.ems.perfomance.performance.Performance;
//import lombok.AllArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//
//import java.net.InetAddress;
//import java.text.SimpleDateFormat;
//import java.time.Duration;
//import java.time.Instant;
//import java.util.*;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.Callable;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.stream.Collectors;
//// legacy
//@Slf4j
//public class PerformanceManagement implements  Runnable {
//    private final long DELTA_MISMATCH_TIME = 20 * 60000L;  // in seconds - 20 minutes
//    public static final String PERFORMANCE_CONSUMER_TOPIC_ID = "PERFORMANCE_CONSUMER_TOPIC_ID";
//    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//    private  ArrayBlockingQueue<CounterInfoObject> queueCounterInfo;
//    private  ArrayBlockingQueue<CounterDataObject> queueCounterData;
//    private  ArrayBlockingQueue<CounterDataObject> queueCounterDataDone;
//    private  ArrayBlockingQueue<CounterDataObject> queueCounterDataClickhouse;
//    private  ArrayBlockingQueue<CounterDataObject> queueCounterDataDoneClickhouse;
//    private ArrayBlockingQueue<ArrayList<CounterObject>> queuePreProcessingData;
//
//    private final FTPPathRepository ftpPathRepository;
//    private final CounterMySqlRepository counterMySqlRepository;
//    private final NERepository neRepository;
//    private final KafkaMessageRepository kafkaMessageRepository;
//    private final CounterCounterCatRepository counterCounterCatRepository;
//    private final ExtraFieldRepository extraFieldRepository;
//    private final ParamsCodeRepository paramsCodeRepository;
//
//    private final NotificationEngineRedisService notificationEngineRedisService;
//    private final KafkaTemplate<String, List<CounterObject>> kafkaTemplate;
//
//
//    private Hashtable<String, FtpServer> htFTPServerCron;
//    private Hashtable<String, FtpServer> htFTPServerRename;
//    private Hashtable<String, FtpServer> htFTPServerCronClickhouse;
//    private Hashtable<String, FtpServer> htFTPServerRenameClickhouse;
//    private Hashtable<String, FtpServer> htFTPServerKafka;
//    private HashMap<String, CounterConfigObject> hmCounterConfig;
//
//    private final HashMap<String, NEObject> activeNeMap = new HashMap<>();
//    private HashMap<Integer, CounterCatObject> counterCatMap;
//    private Map<Integer, CounterCounterCatObject> counterCounterCatMap;
//    private HashMap<Integer, HashMap<String, ExtraFieldObject>> extraFieldMap;
//    // private Map<String, Integer> columnCodeExtraFieldMap;
//
//    @Value("${spring.kafka.producer.topics}")
//    private String producerTopics;
//    @Value("${spring.kafka.producer.new-format-topic}")
//    private String producerNewFormatTopics;
//    @Value("${spring.pm.is-preprocessing-data}")
//    private boolean isProcessingData;
//
//    private List<String> cacheIntervalList;
//    private final SystemConfig systemConfig;
//    private final DataLakeProcess dataLakeProcess;
//    private final ContextAwareExecutor executorService;
//    private final SystemType systemType;
//    private final ConfigManager configManager;
//
//    private List<FTPPathObject> lstFTPPath;
//    private int limitMaximumFile;
//    private int limitMaximumFileClickhouse;
//    private String localInstanceKey;
//    private Date lastTimeUpdateCounterInfo;
//    private volatile ParserCounterData parserCounterData;
//
//    // Statistics tracking fields
//    private final AtomicInteger totalFilesRead = new AtomicInteger(0);
//    private final AtomicInteger totalFilesParsedSuccess = new AtomicInteger(0);
//    private final AtomicInteger totalFilesParsedFailed = new AtomicInteger(0);
//    private final AtomicInteger totalFilesMovedToDone = new AtomicInteger(0);
//    private final AtomicInteger totalCountersInsertedMySQL = new AtomicInteger(0);
//    private final AtomicInteger totalCountersInsertedClickhouse = new AtomicInteger(0);
//    private final AtomicInteger totalMessagesSentKafka = new AtomicInteger(0);
//    private final AtomicLong totalParsingTimeMs = new AtomicLong(0);
//    private final AtomicLong totalDbInsertTimeMs = new AtomicLong(0);
//    private Instant cronStartTime;
//
//    public PerformanceManagement(FTPPathRepository ftpPathRepository,
//                                 CounterMySqlRepository counterMySqlRepository,
//                                 NERepository neRepository,
//                                 CounterCounterCatRepository counterCounterCatRepository,
//                                 ExtraFieldRepository extraFieldRepository,
//                                 ParamsCodeRepository paramsCodeRepository,
//                                 NotificationEngineRedisService notificationEngineRedisService,
//                                 KafkaTemplate<String, List<CounterObject>> kafkaTemplate,
//                                 KafkaMessageRepository kafkaMessageRepository,
//                                 SystemConfig systemConfig,
//                                 DataLakeProcess dataLakeProcess,
//                                 SystemType systemType,
//                                 ConfigManager configManager,
//                                 ContextAwareExecutor contextAwareExecutor
//                                    ) {
//    this.executorService = contextAwareExecutor;
//    this.ftpPathRepository = ftpPathRepository;
//    this.counterMySqlRepository = counterMySqlRepository;
//    this.neRepository = neRepository;
//    this.counterCounterCatRepository = counterCounterCatRepository;
//    this.extraFieldRepository =extraFieldRepository;
//    this.paramsCodeRepository =paramsCodeRepository;
//    this.notificationEngineRedisService = notificationEngineRedisService;
//    this.kafkaTemplate = kafkaTemplate;
//    this.kafkaMessageRepository = kafkaMessageRepository;
//    this.dataLakeProcess = dataLakeProcess;
//    this.systemConfig = systemConfig;
//    this.systemType = systemType;
//    this.configManager = configManager;
//    }
//
//    public void initialize() {
//        try {
//            htFTPServerCron = new Hashtable<>();
//            htFTPServerRename = new Hashtable<>();
//            htFTPServerCronClickhouse = new Hashtable<>();
//            htFTPServerRenameClickhouse = new Hashtable<>();
//            htFTPServerKafka = new Hashtable<>();
//            queueCounterInfo = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//            queueCounterDataDone = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//            queueCounterDataClickhouse = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//            queueCounterDataDoneClickhouse = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//            queueCounterData = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//            queueCounterDataDone = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//            queuePreProcessingData = new ArrayBlockingQueue<>(systemConfig.getPreCounterQueueSize());
//
//            limitMaximumFile = systemConfig.getLimitMaximumFile();
//            limitMaximumFileClickhouse = systemConfig.getLimitMaximumFileClickhouse();
//            cacheIntervalList = Arrays.stream(systemConfig.getIntervalListStr().split(",")).map(String::trim).collect(Collectors.toList());
//            log.info("Begin process get FTPath, NeActive, GroupCounter...");
//            loadConfig();
//            lastTimeUpdateCounterInfo = new Date();
//        }catch (Exception e) {
//            log.error(e.getMessage());
//        }finally {
//            if(SystemType.SYSTEM_ONT.equals(systemType)){
//                startParseDataFromFileONT();
//            }
//        }
//    }
//
//    public void loadConfig() {
//        try {
//            lstFTPPath = ftpPathRepository.findAll();
//            if(lstFTPPath == null || lstFTPPath.isEmpty()) {
//                log.info("Getting list path done, size={}", 0);
//            }
//            log.info("FTPInfo, size= {}", lstFTPPath.size());
//
//
//            lstFTPPath.forEach(ftpInfo -> {
//                FtpServerObject ftpServerObject = ftpInfo.getFtpServerObject();
//                String ftpServerKey = ftpServerObject.getKey() ;
//
//                if(!htFTPServerCron.containsKey(ftpServerKey)) {
//                    htFTPServerCron.put(ftpServerKey, new FtpServer(ftpServerObject));
//                    htFTPServerRename.put(ftpServerKey, new FtpServer(ftpServerObject));
//                    htFTPServerKafka.put(ftpServerKey, new FtpServer(ftpServerObject));
//                    htFTPServerCronClickhouse.put(ftpServerKey, new FtpServer(ftpServerObject));
//                    htFTPServerRenameClickhouse.put(ftpServerKey, new FtpServer(ftpServerObject));
//                }
//            });
//
//             // get list NE active
//             updateActiveNe();
//
//
//            if(activeNeMap == null || activeNeMap.isEmpty()) {
//                log.warn("Getting lst NE done, size={}", 0);
//            }
//            log.info("NeAct, size={}", activeNeMap.size());
//
//            getInstanceKey();
//
//
//            // for dev ======================================================================
//            paramsCodeRepository.updatePMCoreProcessKey(localInstanceKey);
//            //  ======================================================================
//            // get list GroupCounter
//            counterCatMap = new HashMap<>();
//            counterCounterCatMap = new HashMap<>();
//            updateCounterInfo();
//            if(counterCounterCatMap == null || counterCounterCatMap.isEmpty()) {
//                log.info("Getting list path done, size={}", 0);
//            }
//
//        }catch (Exception e) {
//            log.error(e.getMessage(), e);
//        }
//    }
//
//    private void updateActiveNe() {
//        List<NEObject> lstNEObject = neRepository.findAllNeActive();
//        if(lstNEObject != null && !lstNEObject.isEmpty()) {
//            HashMap<String, NEObject> lstNeActTmp = new HashMap<>();
//            lstNEObject.forEach(item -> lstNeActTmp.put(item.getName(), item));
//            activeNeMap.keySet().retainAll(lstNeActTmp.keySet());
//            activeNeMap.putAll(lstNeActTmp);
//        }
//    }
//
//    private void getInstanceKey() {
//        long processId = ProcessHandle.current().pid();
//        try {
//            localInstanceKey = InetAddress.getLocalHost().getHostAddress() + "_" + processId;
//        }catch(Exception ex) {
//            localInstanceKey = "unknown" + "_" + processId;
//            log.error("getInstanceKey() ", ex);
//        }
//        log.info("Instance key: {}", localInstanceKey);
//    }
//
//    @Override
//    public void run() {
//        try {
//            initialize();
//            // start the processFTPFile
//            log.info(" === Start ProcessFTPFileDone... ===");
//            executorService.submit(new ProcessingCounterDataDone());
//            log.info(" === Start ProcessingCounterDataDoneClickhouse... ===");
//            executorService.submit(new ProcessingCounterDataDoneClickhouse());
//            log.info(" === ProcessingCounterDataClickhouse... ===");
//            executorService.submit(new ProcessingCounterDataClickhouse());
//            log.info(" === ProcessingCounterInfo ===");
//            executorService.submit(new ProcessingCounterInfo());
//            log.info("===");
//            executorService.submit(new PreProcessingData());
//            log.info("===");
//            executorService.submit(new ProcessingCounterInfo());
//            executorService.submit(new ProcessingCounterData());
//            log.info("=== Call remainingPreCounterFileHandlerClickhouse ===");
//            remainingPreCounterFileHandlerClickhouse();
//            log.info("=== Call remainingPreCounterFileHandler ===");
//            remainingPreCounterFileHandler();
//        } catch (Exception ex) {
//            log.error("Error while init ConsumerHandler {}", ex.getMessage());
//            log.info("failed, check other.log for more detailed");
//        }
//    }
//
//    public void voteInstanceProcessing() {
//        try {
//        paramsCodeRepository.updatePMCoreProcessKey(localInstanceKey);
//        log.info("updated instance processor");
//        }catch(Exception e) {
//            log.error("voteInstanceProcessing error", e);
//        }
//    }
//
//    public  synchronized  void remainingPreCounterFileHandler() {
//
//        boolean isUsingMySQL = configManager.getCustomBoolean(systemType,"isUsingMySQL");
//        try {
//            if (!isUsingMySQL) {
//                log.info("{} is not using MySQL! Ignore scanning file", systemType);
//                return;
//            }
//            String instanceKeyDB = paramsCodeRepository.getPMCoreProcessKey(localInstanceKey);
//            if(instanceKeyDB == null || !localInstanceKey.equalsIgnoreCase(instanceKeyDB)) {
//                log.info("standby instance, Don't read remainingPreCounterFileHandler");
//                return;
//            }
//            logStats();
//            // Reset statistics for new cron run
//            cronStartTime = Instant.now();
//            totalFilesRead.set(0);
//            totalFilesParsedSuccess.set(0);
//            totalFilesParsedFailed.set(0);
//            totalFilesMovedToDone.set(0);
//            totalCountersInsertedMySQL.set(0);
//            totalCountersInsertedClickhouse.set(0);
//            totalMessagesSentKafka.set(0);
//            totalParsingTimeMs.set(0);
//            totalDbInsertTimeMs.set(0);
//
//            updateActiveNe();
//
//            if (activeNeMap == null || activeNeMap.isEmpty()) {
//                log.warn("No NE found! size=0");
//                return;
//            }
//
//            if (lstFTPPath == null || lstFTPPath.isEmpty()) {
//                log.warn("No FTPath found! Skip");
//                return;
//            }
//
//            AtomicInteger totalFileRead = new AtomicInteger(0);
//            lstFTPPath.forEach(ftpPathObject -> {
//                if (htFTPServerCron.containsKey(ftpPathObject.getFtpServerObject().getKey())) {
//                    log.info("Listing FTP: serverKey={} path={} currentlyRead={}", ftpPathObject.getFtpServerObject().getKey(), ftpPathObject.getPath(), totalFileRead.get());
//                    if (totalFileRead.get() >= limitMaximumFile) {
//                        return;
//                    }
//
//                      htFTPServerCron.get(ftpPathObject.getFtpServerObject().getKey()).getListFile(ftpPathObject.getPath(), limitMaximumFile, totalFileRead
//                              , preCounter -> {
//                                  try {
//                                        if (preCounter != null) {
//                                        totalFilesRead.incrementAndGet();
//                                        log.info("put file to queuePreCounter:  size={}", queueCounterData.size());
//                                        queueCounterData.put(preCounter);
//                                      }
//                                  } catch (InterruptedException ex) {
//                                    log.error("remainingPreCounterFileHandler() ", ex);
//                                      Thread.currentThread().interrupt();
//                                  }
//                              });
//                }
//            });
//
//
//        } catch (Exception ex) {
//            log.error("Error while fetching file from FTP: {}", ex.getMessage());
//        }
//    }
//
//
//    public synchronized void remainingPreCounterFileHandlerClickhouse() {
//        // get config from thread
//        boolean isUsingClickhouse = configManager.getCustomBoolean(systemType,"isUsingClickhouse");
//        try {
//            if (!isUsingClickhouse) {
//                log.info("{} is not using Clickhouse! Skip scanning file in the {} folder", systemType, systemConfig.getFolderNameFTPClickhouse());
//                return;
//            }
//            String instanceKeyDB = paramsCodeRepository.getPMCoreProcessKey(localInstanceKey);
//            if(!Objects.equals(Optional.ofNullable(instanceKeyDB).orElse("").toLowerCase(),
//            Optional.ofNullable(localInstanceKey).orElse("-_-").toLowerCase())) {
//                log.info("standby instance, Don't read remainingPreCounterFileHandlerClickhouse");
//                return;
//            }
//            updateActiveNe();
//
//            if (activeNeMap == null || activeNeMap.isEmpty()) {
//                log.warn("No NE found! size=0");
//                return;
//            }
//
//            if (lstFTPPath == null || lstFTPPath.isEmpty()) {
//                log.warn("No FTPath found! Skip");
//                return;
//            }
//            Instant startTime = Instant.now();
//
//            AtomicInteger totalFileRead = new AtomicInteger(0);
//            lstFTPPath.forEach(ftpPathObject -> {
//                if (htFTPServerCronClickhouse.containsKey(ftpPathObject.getFtpServerObject().getKey())) {
//
//                    if (totalFileRead.get() >= limitMaximumFileClickhouse) {
//                        return;
//                    }
//                    try {
//                        Integer subPos = ftpPathObject.getPath().lastIndexOf("/" + systemConfig.getFolderNameFTPClickhouse());
//                        String path = ftpPathObject.getPath().substring(0,subPos);
//                        htFTPServerCronClickhouse.get(ftpPathObject.getFtpServerObject().getKey()).getListFileClickHouse(path, limitMaximumFileClickhouse, totalFileRead
//                                , preCounter -> {
//                                    try {
//                                        if (preCounter != null) {
//                                            log.info("put file {} to queueCounterDataClickhouse, size={} queueCounterDataClickhouse.remainingCapacity(): {}",preCounter.getFileName(), queueCounterDataClickhouse.size(), queueCounterDataClickhouse.remainingCapacity());
//                                            queueCounterDataClickhouse.put(preCounter);
//                                        }
//                                    } catch (InterruptedException ex) {
//                                        Thread.currentThread().interrupt();
//                                    }
//                                });
//
//                    }catch (Exception ex) {
//                        log.error("Error while fetching file from FTP: {}", ex.getMessage());
//                    }
//
//                }
//            });
//
//            Instant endTime = Instant.now();
//            log.info("Duration time={}", Duration.between(startTime, endTime).toMillis());
//
//        } catch (Exception ex) {
//            log.error("Error while fetching file from FTP clickhouse: {}", ex.getMessage());
//        }
//    }
//
//
//
//
//    class ProcessingCounterData implements Runnable {
//        @Override
//        public void run() {
//            do {
//                try {
//                    CounterDataObject preCounter = queueCounterData.take();
//                    log.info("4. Parsing counter {}, queue size: {}", preCounter, queueCounterData.size());
//                    executorService.submit(new ParsingCounterData(preCounter));
//                } catch (Exception ex) {
//                    log.error("Error while processing counter data: {}", ex.getMessage());
//                    Thread.currentThread().interrupt();
//                }
//            } while (true);
//        }
//    }
//
//    class ProcessingCounterDataDone implements Runnable {
//        @Override
//        public void run() {
//            do {
//                try {
//                    CounterDataObject preCounter = queueCounterDataDone.take();
////                    log.info("queueCounterDataDone, queue size: {}", queueCounterDataDone.size());
//                    Performance performance = Performance.of("MoveFileFTP", "PMC", preCounter.toString());
//                    String ftpServerKey = preCounter.getFtpServerObject().getKey();
//                    if (htFTPServerRename.containsKey(ftpServerKey)) {
//                        String path = preCounter.getPath();
//                        String fileName = preCounter.getFileName();
//                        htFTPServerRename.get(ftpServerKey).renameFile(
//                                String.format("%s/%s", path, fileName),
//                                String.format("%s/%s/%s", path, systemConfig.getFolderNameFTPDone(), fileName)
//                        );
//                        totalFilesMovedToDone.incrementAndGet();
//                    }
//                    performance.log(ErrorCode.NO_ERROR.getDescription());
//                } catch (InterruptedException ex) {
//                    Thread.currentThread().interrupt();
//                    log.error("Error while processing counter data: {}", ex.getMessage());
//                } catch (Exception ex) {
//                    log.error("Error while processing counter data done: {}", ex.getMessage());
//                }
//            } while (true);
//        }
//    }
//
//    class ProcessingCounterInfo implements Runnable {
//        @Override
//        public void run() {
//            do {
//                try {
//                    CounterInfoObject fileCounterInfo = queueCounterInfo.take();
//                    log.info("queueCounterInfo size: {}", queueCounterInfo.size());
//                    String ftpServerKey = fileCounterInfo.getFtpServerObject().getKey();
//                    if (htFTPServerKafka.containsKey(ftpServerKey)) {
//                        String path = fileCounterInfo.getPath();
//                        String fileName = fileCounterInfo.getFileName();
//                        CounterDataObject counterDataObject = htFTPServerKafka
//                                .get(ftpServerKey).getFile(path, fileName);
//                        if (counterDataObject != null) {
//                            queueCounterData.put(counterDataObject);
//                        } else {
//                            log.warn("{}/{} not found", path, fileName);
//                        }
//                    }
//
//
//                } catch (InterruptedException ex) {
//                    Thread.currentThread().interrupt();
//                    log.error("Error while processing counter info: {}", ex.getMessage());
//                } catch (Exception ex) {
//                    log.error("Error while processing counter info: {}", ex.getMessage());
//                }
//            } while (true);
//        }
//    }
//
//
//    @AllArgsConstructor
//    class PreProcessingData implements Runnable {
//        @Override
//        public void run() {
//            do {
//
//                try {
//                    ArrayList<CounterObject> lstCounterObj = queuePreProcessingData.take();
//                    log.info("<========= take item from queuePreProcessingData, size={}",queuePreProcessingData.size());
//                    if(isProcessingData) {
//                        sendMessageToKafka(lstCounterObj);
//                    }
//                    try {
//                        Thread.sleep(5);
//                    }catch (InterruptedException ex) {
//                        log.error("Error while sleeping thread: {}", ex.getMessage());
//                        Thread.currentThread().interrupt();
//                    }
//                }catch(InterruptedException e) {
//                    log.error("run()", e);
//                    Thread.currentThread().interrupt();
//                }catch(Exception e) {
//                    log.error("run()", e);
//                }
//            }while(true);
//        }
//
//        private void sendMessageToKafka(ArrayList<CounterObject> msg) {
//            try {
//                log.debug("send messsgae = {} to kafka via topic = {}", msg.toString(), producerTopics );
//                kafkaTemplate.send(producerTopics, msg);
//            }catch(Exception e ) {
//                log.error(e.getMessage(), e);
//            }
//        }
//
//    }
//
//    @AllArgsConstructor
//    class ParsingCounterData implements Callable<Integer> {
//        private final CounterDataObject preCounter;
//
//        @Override
//        public Integer call() {
//            Performance performance = Performance.of("ParsingCounterData", "PMC", preCounter.toString());
//            try {
//                Instant parseStart = Instant.now();
//                List<CounterObject> lstCounter = performance.trace(() ->  parserCounterData.parseCounter(preCounter), "{}","ParsingCounterData");
//                long parseTime = Duration.between(parseStart, Instant.now()).toMillis();
//                totalParsingTimeMs.addAndGet(parseTime);
//
//                if (lstCounter == null || lstCounter.isEmpty()) {
//                    log.warn("No counter found in file {}", preCounter.getFileName());
//                    performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, "lstCounter is null or empty"));
//                    totalFilesParsedFailed.incrementAndGet();
//                    queueCounterDataDone.put(preCounter);
//                    return 0;
//                }
//
//                totalFilesParsedSuccess.incrementAndGet();
//
//                // insert to mysql
//                Instant dbStart = Instant.now();
//                ErrorCode resCode = performance.trace(() -> counterMySqlRepository.addCounter(lstCounter), "{}", "StoreDatabase");
//                long dbTime = Duration.between(dbStart, Instant.now()).toMillis();
//                totalDbInsertTimeMs.addAndGet(dbTime);
//
//                if (resCode == ErrorCode.NO_ERROR || resCode == ErrorCode.ERROR_DUPLICATE_RECORD) {
//                    totalCountersInsertedMySQL.addAndGet(lstCounter.size());
//                    boolean resultKafka = sendMessageToKafka(lstCounter);
//                    if (resultKafka) {
//                        totalMessagesSentKafka.incrementAndGet();
//                        queueCounterDataDone.put(preCounter);
//                    }
//                }
////                sendNeIdToRedis(lstCounter);
//            } catch (InterruptedException ex) {
//                log.error("Error while parsing counter data: {}", ex.getMessage());
//                performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, ex.getMessage()));
//                totalFilesParsedFailed.incrementAndGet();
//                Thread.currentThread().interrupt();
//                return 1;
//            } catch (Exception ex) {
//                log.error("Error while parsing counter data: {}", ex.getMessage());
//                                performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, ex.getMessage()));
//                totalFilesParsedFailed.incrementAndGet();
//                return 1;
//            }
//            performance.log(String.format(ErrorCode.NO_ERROR.getDescription()));
//            return 0;
//        }
//        private boolean sendMessageToKafka(List<CounterObject> msg) {
//            try {
//                if(isProcessingData) {
//                    log.debug("Send message = {} to kafka via topic = {}", msg.toString(), producerTopics);
//                    kafkaTemplate.send(producerTopics, msg);
//                }
//                return true;
//            }catch (Exception e) {
//                log.error(e.getMessage(), e);
//                return false;
//            }
//        }
//
//        private void sendNeIdToRedis(ArrayList<CounterObject> lstCounter) {
//            CounterObject object = lstCounter.get(0);
//            notificationEngineRedisService.addNe(object.getNeId());
//        }
//    }
//
//    class ProcessingCounterDataClickhouse implements Runnable {
//
//         @Override
//         public void run() {
//             do {
//                 try {
//                     CounterDataObject preCounter = queueCounterDataClickhouse.take();
//                     executorService.submit(new ParsingCounterDataClickhouse(preCounter));
//
//                 } catch (InterruptedException ex) {
//                     // Nếu luồng bị ngắt từ bên ngoài, dừng vòng lặp
//                     Thread.currentThread().interrupt();
//                     log.warn("ClickHouse consumer thread was interrupted and is shutting down.");
//                 } catch (Exception ex) {
//                     // Ghi lại lỗi nhưng không để luồng chết, tiếp tục xử lý các file khác
//                     log.error("Error occurred in ClickHouse consumer thread while processing a file", ex);
//                 }
//             } while (true);
//
//         }
//     }
//
//
//
//    @AllArgsConstructor
//    class ParsingCounterDataClickhouse implements Callable<Integer> {
//        private final CounterDataObject preCounter;
//
//        @Override
//        public Integer call() {
//            Performance performance = Performance.of("ParsingCounterDataClickHouse", "PMC", preCounter.toString());
//            Integer result = doParsingCounterDataClickhouseCall(performance, preCounter, null);
//            if(result != -1) return result;
//            performance.log(ErrorCode.NO_ERROR.getDescription());
//            return 0;
//        }
//    }
//
//
//    class ProcessingCounterDataDoneClickhouse implements Runnable {
//
//        @Override
//        public void run() {
//            do {
//                try {
//                    CounterDataObject preCounter = queueCounterDataDoneClickhouse.take();
//                    log.info(" queueCounterDataDoneClickhouse queue size: {}", preCounter, queueCounterDataDoneClickhouse.size());
//                    Performance performance = Performance.of("MoveFileFTP", "PMC", preCounter.toString());
//                    String ftpServerKey = preCounter.getFtpServerObject().getKey();
//                    if (htFTPServerRenameClickhouse.containsKey(ftpServerKey)) {
//                        String path = preCounter.getPath();
//                        String fileName = preCounter.getFileName();
//                        htFTPServerRenameClickhouse.get(ftpServerKey).renameFile(
//                                String.format("%s/%s", path, fileName),
//                                String.format("%s/%s/%s", path, systemConfig.getFolderNameFTPClickhouse(), fileName)
//                        );
//                    }
//                    performance.log(ErrorCode.NO_ERROR.getDescription());
//                } catch (InterruptedException ex) {
//                    log.error("Error while processing counter data: {}", ex.getMessage());
//                    Thread.currentThread().interrupt();
//                } catch (Exception ex) {
//                    log.error("Error while processing counter data done: {}", ex.getMessage());
//                }
//            } while (true);
//        }
//    }
//
//
//
//
//     public Integer doParsingCounterDataClickhouseCall(Performance performance, CounterDataObject preCounterObject, ProcessDataONT inputObject ) {
//        try {
//            Map<String, List<Map<String, Object>>> dataPushClickHouses = new HashMap<>();
//            List<NewFormatCounterObject> counterObjectList =
//                    inputObject != null ?
//                            performance.trace(() -> parserCounterData.parseCounterClickHouse(inputObject), "{}", "[ONT - KPI], parsing data") :
//                            performance.trace(() -> parserCounterData.parseCounterClickHouse(preCounterObject), "{}", "ParsingCoutnerData");
//
//            String fileName = inputObject != null ? inputObject.getFileName() : preCounterObject.getFileName();
//            if(counterObjectList == null) {
//                log.info("{}: Oops! counterObjectList is null", fileName);
//                performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, "counterObject is null"));
//                if(inputObject == null) {
//                    queueCounterDataDoneClickhouse.put(preCounterObject);
//                }else {
//                    pushDataQueueONTCounterDone(inputObject, true, dataPushClickHouses);
//                }
//                return 0;
//            }
//
//            for(var counterObject : counterObjectList) {
//                counterObject.setCheckONUConfig(inputObject != null );
//                counterObject.setFileName(fileName);
//
//                ErrorCode resCode = performance.trace(() -> kafkaMessageRepository.sendToClickhouse(counterObject, dataPushClickHouses), "{}", "GenerateClickHouseJson");
//                boolean isProcessCounterObject = Objects.equals(null, inputObject);
//                if(resCode == ErrorCode.NO_ERROR) {
//                    // send to kafka
//                    boolean resultKafka = true;
//                    if(isProcessingData)
//                        resultKafka = kafkaMessageRepository.sendMessageToKafka(producerNewFormatTopics, counterObject);
//                    // move file to Done folder
//                    if(isProcessCounterObject) {
//                        if(resultKafka) {
//                            queueCounterDataDoneClickhouse.put(preCounterObject);
//                        }
//                    }else {
//                        pushDataQueueONTCounterDone(inputObject, resultKafka, dataPushClickHouses);
//                    }
//                }else {
//                    if(!isProcessCounterObject) {
//                        pushDataQueueONTCounterDone(inputObject, false, dataPushClickHouses);
//                    }
//                }
//            }
//        } catch (Exception ex) {
//            log.error("Error while parsing counter data clickhouse: {}", ex.getMessage());
//            performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, ex.getMessage()));
//            return 1;
//        }
//        return -1;
//     }
//
//
//     @KafkaListener(topics = "${spring.kafka.consumer.topics}", groupId = "${spring.kafka.consumer.group-id}",
//     id = PERFORMANCE_CONSUMER_TOPIC_ID, autoStartup = "false"
//     )
//     public void listen(CounterInfoObject message) {
//        Performance performance = Performance.of("ReceiveMessageFromKafka", "PMC", message.toString());
//        try {
//            log.info("[core]Received message in group - group-id: {}", message);
//            queueCounterInfo.put(message);
//            performance.log(ErrorCode.NO_ERROR.getDescription());
//        }catch(InterruptedException e) {
//            log.error(e.getMessage(), e);
//            performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, e.getMessage()));
//            Thread.currentThread().interrupt();
//        }catch(Exception e) {
//            log.error(e.getMessage(), e);
//            performance.log(String.format("%s: %s", ErrorCode.ERROR_UNKNOWN, e.getMessage()));
//        }
//     }
//
//
//
//
//
//    private void pushDataQueueONTCounterDone(ProcessDataONT inputObject, boolean result, Map<String, List<Map<String, Object>>> dataPushClickHouses) throws InterruptedException{
//        inputObject.setSuccess(result);
//        if(result) {
//            inputObject.setDataPushClickHouses(dataPushClickHouses);
//            dataLakeProcess.processAppendFile(inputObject);
//        }else {
//            ParseCounterDataONT.queueDataONTProcessCounterDone.put(inputObject);
//        }
//    }
//
//
//    private void startParseDataFromFileONT() {
//        new Thread(() -> {
//            while(!ParseCounterDataONT.postConstructCalled) {
//                try {
//                    Thread.sleep(2000);
//                }catch(InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                    throw new RuntimeException(e);
//                }
//
//                log.warn("WAITING FOR ONT POST CONSTRUCTION");
//            }
//            log.info("ONT POST constructioni called");
//            while(true) {
//                try {
//                    ProcessDataONT inObject =ParseCounterDataONT.queueDataONTProcessCounter.take();
//                    log.info("?. parsing data afile ONT counter {}, queueDataONTProcessCounterDone size={}", inObject.getFileName(), ParseCounterDataONT.queueDataONTProcessCounter.size());
//                    executorService.submit(new ProcessingCounterDataFileONTFirstTime(inObject));
//                }catch(Exception e) {
//                    log.error(e.getMessage(), e);
//                }
//            }
//        }).start();
//    }
//
//
//    class ProcessingCounterDataFileONTFirstTime implements Runnable {
//        ProcessDataONT data;
//        public ProcessingCounterDataFileONTFirstTime(ProcessDataONT data) {
//            this.data = data;
//        }
//
//        @Override
//        public void run() {
//            Performance performance = Performance.of("ProcessingCounterDataFileONTFirstTime", "PMCF", data.getFileName() );
//            Integer result = doParsingCounterDataClickhouseCall(performance, null,data );
//            performance.log(ErrorCode.NO_ERROR.getDescription());
//        }
//    }
//
//
//    private void getCounterInfo() {
//        List<CounterCounterCatObject> lstGroupCounter = counterCounterCatRepository.findAll();
//        HashMap<Integer, CounterCounterCatObject> tmpHmCounterCounterCat = new HashMap<>();
//        if(lstGroupCounter != null && !lstGroupCounter.isEmpty()) {
//            lstGroupCounter.forEach(item -> tmpHmCounterCounterCat.put(item.getCounterId(), item));
//            counterCounterCatMap = tmpHmCounterCounterCat;
//        }
//        log.info("groupcounter, size = {}", counterCounterCatMap.size());
//        // get list extra field
//        extraFieldMap = extraFieldRepository.getExtraField();
////        columnCodeExtraFieldMap = extraFieldRepository.getColumnCodeMapping();
//        log.info("hmExtraField, size={}", extraFieldMap == null ? 0 : extraFieldMap.size());
//        // oran
//        hmCounterConfig= counterCounterCatRepository.findCounterOran();
//        log.info("hmCounterConfig, size = {}", hmCounterConfig == null ? 0 : hmCounterConfig.size());
//    }
//
//    public void updateCounterInfo() {
//        try {
//            getCounterInfo();
//            // initialized parsecounterdata
//            parserCounterData = new ParserCounterData(activeNeMap, counterCounterCatMap, extraFieldMap, hmCounterConfig);
//
//            String instanceKeyDB = paramsCodeRepository.getPMCoreProcessKey(localInstanceKey);
//            if(!Optional.ofNullable(localInstanceKey).orElse("-_-").equalsIgnoreCase(
//                Optional.ofNullable(instanceKeyDB).orElse("")
//            )) {
//                log.info("standby instance, NOt creating or updating database table");
//                return;
//            }
//
//            boolean isAutoAddingColumn = configManager.getCustomBoolean(TenantContextHolder.getCurrentSystem(), "isAutoAddingColumn");
//            if(!isAutoAddingColumn) return;
//            List<CounterCatObject> lstCounterCatObjectUpdate = counterCounterCatRepository.getUpdatedCounterCat(lastTimeUpdateCounterInfo != null ? dateFormat.format(lastTimeUpdateCounterInfo) : null);
//            List<CounterCounterCatObject> lstCounterCounterCatObjectupdate = counterCounterCatRepository.getUpdatedCounterCounterCat(lastTimeUpdateCounterInfo != null ? dateFormat.format(lastTimeUpdateCounterInfo) : null);
//            log.info("{} new updated counter cat: {}", lstCounterCatObjectUpdate.size(), lstCounterCatObjectUpdate.stream().map(counterCatObject -> String.valueOf(counterCatObject.getCode())).collect(Collectors.joining(",")));
//            log.info("{} new updated Counter: {}", lstCounterCounterCatObjectupdate.size(), lstCounterCounterCatObjectupdate.stream().map(counterCounterCatObject -> String.valueOf(counterCounterCatObject.getCounterId())).collect(Collectors.joining(",")));
//            Date current = new Date();
//            for(CounterCatObject counterCatObject : lstCounterCatObjectUpdate) {
//                try {
//                    if(!counterCatObject.isSubCat()) {
//                        counterCounterCatRepository.addCounterCatTableToDb(counterCatObject.getCode(),
//                                extraFieldMap.get(counterCatObject.getObjectLevelId()),
//                        counterCounterCatMap.values().stream().filter(
//                                counterCounterCatObject -> counterCounterCatObject.getCounterCatId() == counterCatObject.getId()
//                        ).map(CounterCounterCatObject::getCounterId).collect(Collectors.toList()),
//                                counterCatObject.isSubCat()
//                        );
//                        counterCatMap.put(counterCatObject.getId(), counterCatObject);
//                    }
//                } catch (Exception e) {
//                    log.error("Error while updateing counter cat with id: {}",counterCatObject.getId(), e);
//                }
//
//                if(isProcessingData) {
//                    for(String interval : cacheIntervalList) {
//                        if(!counterCatObject.isSubCat()) {
//                            counterCounterCatRepository.addCounterCatTableToDb(String.format("%s_%s", counterCatObject.getCode(), interval),
//                                    extraFieldMap.get(counterCatObject.getObjectLevelId()),
//                                    counterCounterCatMap.values().stream().filter(
//                                            counterCounterCatObject -> counterCounterCatObject.getCounterId() == counterCatObject.getId()
//                                    ).map(CounterCounterCatObject::getCounterId).collect(Collectors.toList()), counterCatObject.isSubCat());
//                        }else {
//                            for(Constant.KpiType kpiType : Constant.KpiType.values()) {
//                                counterCounterCatRepository.addCounterCatTableToDb(String.format("%s_%s_%s", counterCatObject.getCode(), kpiType, interval),
//                                        extraFieldMap.get(counterCatObject.getObjectLevelId()),
//                                        counterCounterCatMap.values().stream().filter(
//                                                counterCounterCatObject -> counterCounterCatObject.getSubCatId() == counterCatObject.getId()
//                                                && counterCounterCatObject.getKpiType() == kpiType.getValue()
//                                        ).map(CounterCounterCatObject::getCounterId)
//                                                .collect(Collectors.toList()), counterCatObject.isSubCat());
//                            }
//                        }
//                    }
//                }
//            }
//            counterCounterCatRepository.addCounterToTable(lstCounterCounterCatObjectupdate, counterCounterCatMap, extraFieldMap, "");
//            if(isProcessingData) {
//                for(String interval : cacheIntervalList) {
//                    counterCounterCatRepository.addCounterToTable(lstCounterCounterCatObjectupdate, counterCounterCatMap, extraFieldMap, interval);
//                }
//            }
//            lastTimeUpdateCounterInfo = current;
//        }catch(Exception e) {
//            log.error("Error while updating counter: {}",e.getMessage(), e);
//
//        }
//    }
//
//
//
//    public void logStats() {
//        if (cronStartTime == null) {
//            log.info("=== CRON STATISTICS: No cron run started yet ===");
//            return;
//        }
//
//        long totalDurationMs = Duration.between(cronStartTime, Instant.now()).toMillis();
//        double totalDurationSec = totalDurationMs / 1000.0;
//
//        int filesRead = totalFilesRead.get();
//        int filesParsedSuccess = totalFilesParsedSuccess.get();
//        int filesParsedFailed = totalFilesParsedFailed.get();
//        int filesMovedDone = totalFilesMovedToDone.get();
//        int countersMySQL = totalCountersInsertedMySQL.get();
//        int countersClickhouse = totalCountersInsertedClickhouse.get();
//        int kafkaMessages = totalMessagesSentKafka.get();
//        long avgParseTimeMs = filesRead > 0 ? totalParsingTimeMs.get() / filesRead : 0;
//        long avgDbInsertTimeMs = filesParsedSuccess > 0 ? totalDbInsertTimeMs.get() / filesParsedSuccess : 0;
//
//        int queueCounterDataSize = queueCounterData != null ? queueCounterData.size() : 0;
//        int queueCounterDataDoneSize = queueCounterDataDone != null ? queueCounterDataDone.size() : 0;
//        int queueCounterInfoSize = queueCounterInfo != null ? queueCounterInfo.size() : 0;
//        int queueCounterDataClickhouseSize = queueCounterDataClickhouse != null ? queueCounterDataClickhouse.size() : 0;
//        int queueCounterDataDoneClickhouseSize = queueCounterDataDoneClickhouse != null ? queueCounterDataDoneClickhouse.size() : 0;
//
//        log.info("============================================================");
//        log.info("               CRON JOB STATISTICS({})                     ", TenantContextHolder.getCurrentSystem().getCode());
//        log.info("============================================================");
//        log.info("Execution Time:");
//        log.info("  - Start Time          : {}", cronStartTime);
//        log.info("  - Current Time        : {}", Instant.now());
//        log.info(String.format("  - Total Duration      : %.2fs (%dms)", totalDurationSec, totalDurationMs));
//        log.info("------------------------------------------------------------");
//        log.info("File Processing:");
//        log.info("  - Files Read          : {}", filesRead);
//        log.info("  - Parsed Success      : {}", filesParsedSuccess);
//        log.info("  - Parsed Failed       : {}", filesParsedFailed);
//        log.info("  - Moved to Done       : {}", filesMovedDone);
//        log.info(String.format("  - Success Rate        : %.1f%%", filesRead > 0 ? (filesParsedSuccess * 100.0 / filesRead) : 0.0));        log.info("------------------------------------------------------------");
//        log.info("Counter Insertion:");
//        log.info("  - MySQL Counters      : {}", countersMySQL);
//        log.info("  - Clickhouse Counters : {}", countersClickhouse);
//        log.info("  - Kafka Messages Sent : {}", kafkaMessages);
//        log.info("------------------------------------------------------------");
//        log.info("Performance Metrics:");
//        log.info("  - Avg Parse Time      : {}ms", avgParseTimeMs);
//        log.info("  - Avg DB Insert Time  : {}ms", avgDbInsertTimeMs);
//        log.info(String.format("  - Throughput          : %.2f files/sec", totalDurationSec > 0 ? filesRead / totalDurationSec : 0.0));        log.info("------------------------------------------------------------");
//        log.info("Queue Status:");
//        log.info("  - queueCounterData               : {} items", queueCounterDataSize);
//        log.info("  - queueCounterDataDone           : {} items", queueCounterDataDoneSize);
//        log.info("  - queueCounterInfo               : {} items", queueCounterInfoSize);
//        log.info("  - queueCounterDataClickhouse     : {} items", queueCounterDataClickhouseSize);
//        log.info("  - queueCounterDataDoneClickhouse : {} items", queueCounterDataDoneClickhouseSize);
//        log.info("============================================================");
//    }
//
//}
