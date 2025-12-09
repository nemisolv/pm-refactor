//package com.viettel.ems.perfomance.tools;
//
//import org.apache.commons.net.ftp.FTPClient;
//import org.apache.commons.net.ftp.FTPFile;
//import org.apache.commons.net.ftp.FTPReply;
//import org.capnproto.MessageBuilder;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.time.temporal.ChronoUnit;
//import java.util.List;
//import java.util.Locale;
//import java.util.Random;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class HighPerformanceGenerator {
//
//    // ===================================================================================
//    // ===                           BẢNG ĐIỀU KHIỂN CẤU HÌNH                           ===
//    // ===================================================================================
//
//    // --- Cấu hình FTP Server ---
//    private static final String FTP_HOST = "localhost";
//    private static final int FTP_PORT = 21;
//    private static final String FTP_USER = "ftpuser";
//    private static final String FTP_PASS = "nam456";
//
//    // --- Cấu hình Đường dẫn & Khu vực ---
//    private static final String BASE_FTP_PATH = "/Access/5G";
//    // Danh sách folder vùng miền
//    private static final List<String> REGIONS = List.of("HNM", "DNG", "HCM", "CTO", "HPH", "DNA", "Others", "Default", "ND");
//
//    // --- Cấu hình Số lượng & Thời gian ---
//
//    private static final int TOTAL_FILES_PER_CYCLE = 10000;
//    private static final int SPREAD_DURATION_SECONDS = 60;
//    private static final int INTERVAL_UPLOAD = 2;
//
//    // --- Cấu hình Số bản ghi (Records) ---
//    private static final int RECORDS_PER_FILE_MULTI = 10;
//    private static final int RECORDS_PER_FILE_SINGLE = 1;
//
//    // --- Cấu hình Tỷ lệ sinh dữ liệu (Tổng = 1.0) ---
//    private static final double RATE_5G_ONLY = 0.4;
//    private static final double RATE_5G_MIX = 0.3;
//
//    // --- Cấu hình Hiệu Năng ---
//    private static final int PARALLEL_THREADS = 50;
//    private static final int MAX_RETRIES = 3;
//
//    // --- Cấu hình Dọn dẹp ---
//    private static final boolean WIPE_DATA_ON_STARTUP = false; // <--- [NEW] true = Xóa sạch file cũ trước khi chạy
//    private static final int CLEANUP_INTERVAL_MINUTES = 1;    // Chu kỳ dọn dẹp folder Done
//
//    // Danh sách trạm (NE) khớp với DB
//    private static final String[] neList = {
//            "gNodeB0284", "gNodeB2164", "gNodeB1141", "gNodeB9982", "gNodeB5512", "gNodeB1001"
//    };
//
//    // Danh sách KPI ID bắt buộc
//    private static final int[] COUNTER_IDS = {68, 69, 1049, 1050, 2261, 2329};
//
//    // ===================================================================================
//
//    private static final ThreadLocal<FTPClient> ftpClientThreadLocal = new ThreadLocal<>();
//    private static final ConcurrentHashMap<String, Boolean> createdPaths = new ConcurrentHashMap<>();
//
//    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter
//            .ofPattern("yyyyMMdd_HHmmss_SSS")
//            .withLocale(Locale.US)
//            .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
//
//    public static void main(String[] args) throws InterruptedException {
//        // --- 1. KHỞI ĐỘNG WORKER POOL CHÍNH ---
//        ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);
//
//        // --- [NEW] 1.5. THỰC HIỆN XÓA SẠCH DỮ LIỆU NẾU ĐƯỢC CẤU HÌNH ---
//        if (WIPE_DATA_ON_STARTUP) {
//            performStartupCleanup();
//        }
//
//        // --- 2. KHỞI ĐỘNG THREAD DỌN DẸP (CLEANER) ---
//        ScheduledExecutorService cleanerExecutor = Executors.newSingleThreadScheduledExecutor();
//        startCleanerTask(cleanerExecutor);
//
//        System.out.println("🚀 SERVICE STARTED.");
//        System.out.println("   - Mode: Generator + Auto Cleanup (Every " + CLEANUP_INTERVAL_MINUTES + " mins)");
//
//        // --- 3. VÒNG LẶP CHÍNH (GENERATOR) ---
//        while (!Thread.currentThread().isInterrupted()) {
//            long millisToSleep = calculateSleepTimeForNextSchedule(INTERVAL_UPLOAD);
//            System.out.printf("💤 Đang ngủ %d giây chờ đến mốc thời gian tiếp theo...\n", millisToSleep / 1000);
//            Thread.sleep(millisToSleep);
//
//            System.out.println("\n⏰ BẮT ĐẦU CHU KỲ ĐẨY FILE: " + LocalDateTime.now());
//            long cycleStartTime = System.currentTimeMillis();
//            AtomicInteger filesSentInCycle = new AtomicInteger(0);
//
//            int filesPerSecond = (int) Math.ceil((double) TOTAL_FILES_PER_CYCLE / SPREAD_DURATION_SECONDS);
//
//            for (int sec = 0; sec < SPREAD_DURATION_SECONDS; sec++) {
//                long startSecondTime = System.currentTimeMillis();
//
//                for (int i = 0; i < filesPerSecond; i++) {
//                    if (filesSentInCycle.get() >= TOTAL_FILES_PER_CYCLE) break;
//
//                    executor.submit(() -> {
//                        try {
//                            processSingleFileUpload();
//                            filesSentInCycle.incrementAndGet();
//                        } catch (Exception e) {
//                            // Silent catch to keep running
//                        }
//                    });
//                }
//
//                long elapsed = System.currentTimeMillis() - startSecondTime;
//                if (elapsed < 1000) {
//                    Thread.sleep(1000 - elapsed);
//                }
//            }
//
//            System.out.printf("✅ KẾT THÚC CHU KỲ. Đã đẩy %d files. Thời gian: %.2f giây.\n",
//                    filesSentInCycle.get(), (System.currentTimeMillis() - cycleStartTime) / 1000.0);
//            createdPaths.clear();
//        }
//
//        cleanerExecutor.shutdown();
//        executor.shutdown();
//    }
//
//    /**
//     * [NEW] Hàm xóa sạch dữ liệu khi khởi động
//     */
//    private static void performStartupCleanup() {
//        System.out.println("\n⚠️ [STARTUP] Đang thực hiện xóa sạch dữ liệu cũ trên FTP...");
//        FTPClient ftpClient = new FTPClient();
//        try {
//            ftpClient.connect(FTP_HOST, FTP_PORT);
//            ftpClient.login(FTP_USER, FTP_PASS);
//            ftpClient.enterLocalPassiveMode();
//            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
//
//            int totalDeleted = 0;
//
//            for (String region : REGIONS) {
//                // 1. Xóa trong folder chính (ví dụ /Access/5G/HNM)
//                String mainPath = BASE_FTP_PATH + "/" + region;
//                totalDeleted += deleteFilesInDirectory(ftpClient, mainPath);
//
//                // 2. Xóa trong folder Done (ví dụ /Access/5G/HNM/Done)
//                String donePath = mainPath + "/Done";
//                totalDeleted += deleteFilesInDirectory(ftpClient, donePath);
//            }
//
//            System.out.printf("✅ [STARTUP] Đã xóa sạch %d files cũ.\n\n", totalDeleted);
//            ftpClient.logout();
//            ftpClient.disconnect();
//
//        } catch (Exception e) {
//            System.err.println("❌ [STARTUP] Lỗi khi xóa dữ liệu cũ: " + e.getMessage());
//            try { ftpClient.disconnect(); } catch (IOException ex) {}
//        }
//    }
//
//    private static int deleteFilesInDirectory(FTPClient ftpClient, String path) throws IOException {
//        int count = 0;
//        if (ftpClient.changeWorkingDirectory(path)) {
//            FTPFile[] files = ftpClient.listFiles();
//            if (files != null) {
//                for (FTPFile file : files) {
//                    if (file.isFile()) {
//                        ftpClient.deleteFile(file.getName());
//                        count++;
//                    }
//                }
//            }
//            ftpClient.changeWorkingDirectory("/"); // Reset về root
//        }
//        return count;
//    }
//
//    /**
//     * Hàm khởi động tác vụ dọn dẹp định kỳ (chỉ dọn folder Done)
//     */
//    private static void startCleanerTask(ScheduledExecutorService cleanerExecutor) {
//        cleanerExecutor.scheduleAtFixedRate(() -> {
//            System.out.println("\n🧹 [CLEANER] Bắt đầu dọn dẹp các thư mục 'Done'...");
//            FTPClient ftpClient = new FTPClient();
//            try {
//                ftpClient.connect(FTP_HOST, FTP_PORT);
//                ftpClient.login(FTP_USER, FTP_PASS);
//                ftpClient.enterLocalPassiveMode();
//                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
//
//                int totalDeleted = 0;
//                for (String region : REGIONS) {
//                    String donePath = BASE_FTP_PATH + "/" + region + "/Done";
//                    totalDeleted += deleteFilesInDirectory(ftpClient, donePath);
//                }
//                System.out.printf("🧹 [CLEANER] Hoàn tất. Đã xóa %d files trong thư mục Done.\n", totalDeleted);
//
//                ftpClient.logout();
//                ftpClient.disconnect();
//
//            } catch (Exception e) {
//                System.err.println("❌ [CLEANER] Lỗi dọn dẹp: " + e.getMessage());
//                try { ftpClient.disconnect(); } catch (IOException ex) {}
//            }
//        }, CLEANUP_INTERVAL_MINUTES, CLEANUP_INTERVAL_MINUTES, TimeUnit.MINUTES);
//    }
//
//    // ... (Các hàm processSingleFileUpload, createCapnprotoData, uploadWithRetry, getFtpClient, ensureDirectoryExists, calculateSleepTimeForNextSchedule GIỮ NGUYÊN) ...
//
//    private static void processSingleFileUpload() throws Exception {
//        Random rand = new Random();
//        String region = REGIONS.get(rand.nextInt(REGIONS.size()));
//        String ftpPath = BASE_FTP_PATH + "/" + region;
//        String neName = neList[rand.nextInt(neList.length)];
//        String timestampStr = TIME_FORMATTER.format(Instant.now());
//
//        double luck = rand.nextDouble();
//        String fileName;
//        String nodeFunction;
//        int recordCount;
//
//        if (luck < RATE_5G_ONLY) {
//            fileName = String.format("GNODEB_%s_%s_%d.capnproto", neName, timestampStr, rand.nextInt(99999));
//            nodeFunction = "1";
//            recordCount = RECORDS_PER_FILE_SINGLE;
//        } else if (luck < (RATE_5G_ONLY + RATE_5G_MIX)) {
//            fileName = String.format("GNODEB_%s_NR_%s_%d.capnproto", neName, timestampStr, rand.nextInt(99999));
//            nodeFunction = rand.nextBoolean() ? "NR_FDD" : "NR_TDD";
//            recordCount = RECORDS_PER_FILE_MULTI;
//        } else {
//            fileName = String.format("GNODEB_%s_LTE_%s_%d.capnproto", neName, timestampStr, rand.nextInt(99999));
//            nodeFunction = rand.nextBoolean() ? "LTE_FDD" : "LTE_TDD";
//            recordCount = RECORDS_PER_FILE_MULTI;
//        }
//
//        byte[] fileData = createCapnprotoData(neName, nodeFunction, recordCount);
//        uploadWithRetry(ftpPath, fileName, fileData);
//    }
//
//    private static byte[] createCapnprotoData(String neName, String nodeFunction, int recordCount) {
//        MessageBuilder message = new MessageBuilder();
//        var root = message.initRoot(CounterSchema.CounterDataCollection.factory);
//        var dataList = root.initData(recordCount);
//        Random random = new Random();
//
//        long randomOffset = random.nextInt(3600000);
//        long baseTime = System.currentTimeMillis() - randomOffset;
//
//        for (int i = 0; i < recordCount; i++) {
//            var counterData = dataList.get(i);
//            counterData.setTime(baseTime - (i * 1000L));
//            counterData.setDuration(900);
//
//            long cellId = random.nextInt(1, 4);
//            String cellName = neName + cellId;
//            String location = String.format("ManagedElement=%s,NodeFunction=%s,CellName=%s,CellId=%d",
//                    neName, nodeFunction, cellName, cellId);
//
//            counterData.setLocation(location);
//            counterData.setCell(cellId);
//            counterData.setService(1);
//
//            int[] tempIds = COUNTER_IDS.clone();
//            int numKpisToGen = random.nextInt(COUNTER_IDS.length) + 1;
//
//            for (int k = tempIds.length - 1; k > 0; k--) {
//                int index = random.nextInt(k + 1);
//                int a = tempIds[index];
//                tempIds[index] = tempIds[k];
//                tempIds[k] = a;
//            }
//
//            var values = counterData.initData(numKpisToGen);
//            for (int j = 0; j < numKpisToGen; j++) {
//                values.get(j).setId(tempIds[j]);
//                values.get(j).setValue(random.nextInt(50000));
//            }
//        }
//
//        root.setUnit(neName);
//        root.setType(0);
//
//        try {
//            java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
//            java.nio.channels.WritableByteChannel channel = java.nio.channels.Channels.newChannel(outputStream);
//            org.capnproto.Serialize.write(channel, message);
//            return outputStream.toByteArray();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private static void uploadWithRetry(String ftpPath, String fileName, byte[] fileData) throws Exception {
//        createdPaths.computeIfAbsent(ftpPath, path -> {
//            try {
//                FTPClient client = getFtpClient();
//                ensureDirectoryExists(client, path);
//                ensureDirectoryExists(client, path.endsWith("/") ? (path + "Done") : (path + "/Done"));
//                return true;
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
//
//        String remoteFilePath = ftpPath.endsWith("/") ? (ftpPath + fileName) : (ftpPath + "/" + fileName);
//
//        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
//            try (InputStream inputStream = new ByteArrayInputStream(fileData)) {
//                FTPClient ftpClient = getFtpClient();
//                ftpClient.changeWorkingDirectory("/");
//                if (ftpClient.storeFile(remoteFilePath, inputStream)) return;
//                throw new IOException("FTP store failed");
//            } catch (Exception e) {
//                ftpClientThreadLocal.remove();
//                if (attempt == MAX_RETRIES) throw e;
//                Thread.sleep(100);
//            }
//        }
//    }
//
//    private static FTPClient getFtpClient() throws IOException {
//        FTPClient ftpClient = ftpClientThreadLocal.get();
//        if (ftpClient == null || !ftpClient.isConnected() || !ftpClient.sendNoOp()) {
//            if (ftpClient != null) try {
//                ftpClient.disconnect();
//            } catch (IOException ignored) {
//            }
//            ftpClient = new FTPClient();
//            ftpClient.connect(FTP_HOST, FTP_PORT);
//            if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) throw new IOException("Connection refused");
//            if (!ftpClient.login(FTP_USER, FTP_PASS)) throw new IOException("Login failed");
//            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
//            ftpClient.enterLocalPassiveMode();
//            ftpClient.setBufferSize(1024 * 1024);
//            ftpClientThreadLocal.set(ftpClient);
//        }
//        return ftpClient;
//    }
//
//    private static void ensureDirectoryExists(FTPClient ftpClient, String dirPath) throws IOException {
//        String[] pathElements = dirPath.split("/");
//        if (pathElements.length == 0) return;
//        if (dirPath.startsWith("/")) ftpClient.changeWorkingDirectory("/");
//        for (String singleDir : pathElements) {
//            if (!singleDir.isEmpty()) {
//                if (!ftpClient.changeWorkingDirectory(singleDir)) {
//                    if (!ftpClient.makeDirectory(singleDir) && !ftpClient.changeWorkingDirectory(singleDir)) {
//                        if (!ftpClient.changeWorkingDirectory(singleDir)) {
//                        }
//                    } else {
//                        ftpClient.changeWorkingDirectory(singleDir);
//                    }
//                }
//            }
//        }
//    }
//
//    private static long calculateSleepTimeForNextSchedule(int intervalMinutes) {
//        LocalDateTime now = LocalDateTime.now();
//        int currentMinute = now.getMinute();
//        int remainder = currentMinute % intervalMinutes;
//        int minutesToAdd = intervalMinutes - remainder;
//        LocalDateTime nextTarget = now.plusMinutes(minutesToAdd).withSecond(0).withNano(0);
//        if (!nextTarget.isAfter(now)) nextTarget = nextTarget.plusMinutes(intervalMinutes);
//        return ChronoUnit.MILLIS.between(now, nextTarget);
//    }
//}