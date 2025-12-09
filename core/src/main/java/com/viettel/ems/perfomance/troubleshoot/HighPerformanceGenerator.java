package com.viettel.ems.perfomance.troubleshoot;

import com.viettel.ems.perfomance.tools.CounterSchema;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.capnproto.MessageBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HighPerformanceGenerator {

    // ===================================================================================
    // ===                           BẢNG ĐIỀU KHIỂN CẤU HÌNH                           ===
    // ===================================================================================

    // --- Cấu hình Database Chung ---
    private static final String DB_USER = "root";
    private static final String DB_PASS = "nam123"; 

    // --- Cấu hình FTP Server ---
    private static final String FTP_HOST = "localhost";
    private static final int FTP_PORT = 21;
    private static final String FTP_USER = "ftpuser";
    private static final String FTP_PASS = "nam456";

    // --- Cấu hình Chung ---
    private static final List<String> REGIONS = List.of("Others");
    private static final int TOTAL_FILES_PER_CYCLE = 10000;
    private static final int SPREAD_DURATION_SECONDS = 60;
    private static final int INTERVAL_UPLOAD_IN_MINUTES = 1;
    private static final int PARALLEL_THREADS = 50;
    private static final boolean WIPE_DATA_ON_STARTUP = true;
    private static final int CLEANUP_INTERVAL_MINUTES = 1;
    private static final int MAX_RETRIES = 3;

    // ===================================================================================
    
    // [NEW] DEFINITION OF A TECHNOLOGY PROFILE (The key to flexibility)
    static class TechProfile {
        String name;           // e.g., "5G", "4G", "CORE"
        String dbUrl;          // Connection string for this specific tech
        String ftpSubFolder;   // e.g., "/Access/5G" or "/Access/4G"
        String neTableName;    // Table to read NEs from
        String counterTableName;// Table to read Counters from
        
        // Data loaded from DB
        List<String> neList = new ArrayList<>();
        List<Integer> counterList = new ArrayList<>();

        public TechProfile(String name, String dbUrl, String ftpSubFolder, String neTableName, String counterTableName) {
            this.name = name;
            this.dbUrl = dbUrl;
            this.ftpSubFolder = ftpSubFolder;
            this.neTableName = neTableName;
            this.counterTableName = counterTableName;
        }
    }

    // List of active profiles (5G, 4G, etc.)
    private static final List<TechProfile> ACTIVE_PROFILES = new ArrayList<>();

    private static final ThreadLocal<FTPClient> ftpClientThreadLocal = new ThreadLocal<>();
    private static final ConcurrentHashMap<String, Boolean> createdPaths = new ConcurrentHashMap<>();
    // Track generated keys to prevent duplicates: Key = "NE_NAME#TIMESTAMP_EPOCH"
    private static final ConcurrentHashMap<String, Boolean> generatedKeys = new ConcurrentHashMap<>();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter
            .ofPattern("yyyyMMdd_HHmmss_SSS")
            .withLocale(Locale.US)
            .withZone(ZoneId.of("Asia/Ho_Chi_Minh"));

    public static void main(String[] args) throws InterruptedException {
        // --- 1. DEFINE & LOAD PROFILES ---
        // You can easily add CORE here later!
        ACTIVE_PROFILES.add(new TechProfile("5G", 
                "jdbc:mysql://localhost:3306/pm_access?useSSL=false&allowPublicKeyRetrieval=true", 
                "/Access/5G", "ne", "counter"));
        
        ACTIVE_PROFILES.add(new TechProfile("4G", 
                "jdbc:mysql://localhost:3306/pm_access_4g?useSSL=false&allowPublicKeyRetrieval=true", 
                "/Access/4G"  // 4g also store in this folder
                , "ne", "counter"));

        // [FUTURE] ACTIVE_PROFILES.add(new TechProfile("CORE", ...));

        loadMetadataFromDB();

        // --- 2. START SERVICES ---
        ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);

        if (WIPE_DATA_ON_STARTUP) {
            performStartupCleanup();
        }

        ScheduledExecutorService cleanerExecutor = Executors.newSingleThreadScheduledExecutor();
        startCleanerTask(cleanerExecutor);

        System.out.println("🚀 SERVICE STARTED.");
        System.out.println("   - Active Profiles: " + ACTIVE_PROFILES.size());
        for(TechProfile p : ACTIVE_PROFILES) {
            System.out.println("     + " + p.name + ": " + p.neList.size() + " NEs, " + p.counterList.size() + " Counters.");
        }

        // --- 3. MAIN GENERATOR LOOP ---
        while (!Thread.currentThread().isInterrupted()) {
            long millisToSleep = calculateSleepTimeForNextSchedule(INTERVAL_UPLOAD_IN_MINUTES);
            System.out.printf("💤 Sleeping %d seconds...\n", millisToSleep / 1000);
            Thread.sleep(millisToSleep);

            System.out.println("\n⏰ START CYCLE: " + LocalDateTime.now());
            long cycleStartTime = System.currentTimeMillis();
            AtomicInteger filesSentInCycle = new AtomicInteger(0);

            int filesPerSecond = (int) Math.ceil((double) TOTAL_FILES_PER_CYCLE / SPREAD_DURATION_SECONDS);

            for (int sec = 0; sec < SPREAD_DURATION_SECONDS; sec++) {
                long startSecondTime = System.currentTimeMillis();

                for (int i = 0; i < filesPerSecond; i++) {
                    if (filesSentInCycle.get() >= TOTAL_FILES_PER_CYCLE) break;

                    executor.submit(() -> {
                        try {
                            processSingleFileUpload();
                            filesSentInCycle.incrementAndGet();
                        } catch (Exception e) {
                            // Silent catch
                        }
                    });
                }

                long elapsed = System.currentTimeMillis() - startSecondTime;
                if (elapsed < 1000) {
                    Thread.sleep(1000 - elapsed);
                }
            }

            System.out.printf("✅ END CYCLE. Sent %d files. Time: %.2f s.\n",
                    filesSentInCycle.get(), (System.currentTimeMillis() - cycleStartTime) / 1000.0);
            createdPaths.clear();
        }

        cleanerExecutor.shutdown();
        executor.shutdown();
    }

    private static void loadMetadataFromDB() {
        System.out.println("⏳ Loading Metadata from Databases...");
        
        for (TechProfile profile : ACTIVE_PROFILES) {
            try (Connection conn = DriverManager.getConnection(profile.dbUrl, DB_USER, DB_PASS)) {
                
                // Load NEs
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT name FROM " + profile.neTableName)) {
                    while (rs.next()) {
                        profile.neList.add(rs.getString("name"));
                    }
                }

                // Load Counters
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT id FROM " + profile.counterTableName)) {
                    while (rs.next()) {
                        profile.counterList.add(rs.getInt("id"));
                    }
                }
                
                if (profile.neList.isEmpty() || profile.counterList.isEmpty()) {
                    System.err.println("⚠️ WARNING: Profile " + profile.name + " has empty NEs or Counters!");
                }

            } catch (SQLException e) {
                System.err.println("❌ Database Error for " + profile.name + ": " + e.getMessage());
            }
        }
    }

    private static void processSingleFileUpload() throws Exception {
        Random rand = new Random();

        TechProfile profile = ACTIVE_PROFILES.get(rand.nextInt(ACTIVE_PROFILES.size()));
        if (profile.neList.isEmpty() || profile.counterList.isEmpty()) return;

        String neName = profile.neList.get(rand.nextInt(profile.neList.size()));
        String region = REGIONS.get(rand.nextInt(REGIONS.size()));
        String ftpPath = profile.ftpSubFolder + "/" + region;

        // [FIX 1] GENERATE RANDOM TIME BETWEEN YEAR 2000 AND 2025
        long minTime = java.sql.Timestamp.valueOf("2000-01-01 00:00:00").getTime();
        long maxTime = java.sql.Timestamp.valueOf("2025-12-30 23:59:59").getTime();
        long randomTimeRange = maxTime - minTime;
        long randomTime = minTime + (long)(rand.nextDouble() * randomTimeRange);

        // Snap to 15-minute interval (900 seconds)
        long period900 = 900 * 1000L;
        long selectedBaseTime = (randomTime / period900) * period900;

        String timestampStr = TIME_FORMATTER.format(Instant.ofEpochMilli(selectedBaseTime));

        String fileName;
        if (profile.name.equals("4G")) {
            fileName = String.format("GNODEB_%s_LTE_%s_%d.capnproto", neName, timestampStr, rand.nextInt(99999));
        } else if(profile.name.equals("5G")) {
            fileName = String.format("GNODEB_%s_NR_%s_%d.capnproto", neName, timestampStr, rand.nextInt(99999));
        }else {
            fileName = "5gcore"; // later
        }

        // Generate data (e.g., 10 records per file)
        byte[] fileData = createCapnprotoData(neName, profile, 1, selectedBaseTime);
        uploadWithRetry(ftpPath, fileName, fileData);
    }

    private static byte[] createCapnprotoData(String neName, TechProfile profile, int recordCount, long baseTime) {
        MessageBuilder message = new MessageBuilder();
        var root = message.initRoot(CounterSchema.CounterDataCollection.factory);
        var dataList = root.initData(recordCount);
        Random random = new Random();

        // Prepare unique counters list
        List<Integer> availableIds = new ArrayList<>(profile.counterList);
        if (availableIds.isEmpty()) return new byte[0];
        Collections.shuffle(availableIds, random);
        int numKpisToGen = random.nextInt(Math.min(10, availableIds.size())) + 1;
        List<Integer> selectedUniqueIds = availableIds.subList(0, numKpisToGen);

        for (int i = 0; i < recordCount; i++) {
            var counterData = dataList.get(i);

            // This ensures NO DUPLICATE (ne_id + time) inside this single file.
            long recordTime = baseTime - (i * 900 * 1000L);

            counterData.setTime(recordTime);
            counterData.setDuration(900);

            long cellId = random.nextInt(1, 4);
            String cellName = neName + "_" + cellId;
            String ratType = null;
            if ("4G".equals(profile.name)) {
                ratType = random.nextBoolean() ? "LTE_FDD" : "LTE_TDD";
            } else if ("5G".equals(profile.name)) {
                ratType = random.nextBoolean() ? "NR_FDD" : "NR_TDD";
            }

            String location = String.format("ManagedElement=%s,CellName=%s,CellId=%d%s",
                    neName,
                    cellName,
                    cellId,
                    (ratType != null ? ",rat_type=" + ratType : "")
            );
            counterData.setLocation(location);
            counterData.setCell(cellId);
            counterData.setService(1);

            var values = counterData.initData(numKpisToGen);
            for (int j = 0; j < numKpisToGen; j++) {
                values.get(j).setId(selectedUniqueIds.get(j));
                values.get(j).setValue(random.nextInt(50000));
            }
        }

        root.setUnit(neName);
        root.setType(0);

        try {
            java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
            java.nio.channels.WritableByteChannel channel = java.nio.channels.Channels.newChannel(outputStream);
            org.capnproto.Serialize.write(channel, message);
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void performStartupCleanup() {
        System.out.println("\n⚠️ [STARTUP] Performing cleanup...");
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(FTP_HOST, FTP_PORT);
            ftpClient.login(FTP_USER, FTP_PASS);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);

            int totalDeleted = 0;

            // [UPDATED] Clean up ALL active profile folders
            for (TechProfile profile : ACTIVE_PROFILES) {
                for (String region : REGIONS) {
                    String mainPath = profile.ftpSubFolder + "/" + region;
                    totalDeleted += deleteFilesInDirectory(ftpClient, mainPath);
                    totalDeleted += deleteFilesInDirectory(ftpClient, mainPath + "/Done");
                }
            }

            System.out.printf("✅ [STARTUP] Cleaned %d files.\n\n", totalDeleted);
            ftpClient.logout();
            ftpClient.disconnect();
        } catch (Exception e) {
            System.err.println("❌ [STARTUP] Error: " + e.getMessage());
        }
    }

    private static int deleteFilesInDirectory(FTPClient ftpClient, String path) throws IOException {
        int count = 0;
        if (ftpClient.changeWorkingDirectory(path)) {
            FTPFile[] files = ftpClient.listFiles();
            if (files != null) {
                for (FTPFile file : files) {
                    if (file.isFile()) {
                        ftpClient.deleteFile(file.getName());
                        count++;
                    }
                }
            }
            ftpClient.changeWorkingDirectory("/"); 
        }
        return count;
    }

    private static void startCleanerTask(ScheduledExecutorService cleanerExecutor) {
        cleanerExecutor.scheduleAtFixedRate(() -> {
            System.out.println("\n🧹 [CLEANER] Cleaning 'Done' folders...");
            FTPClient ftpClient = new FTPClient();
            try {
                ftpClient.connect(FTP_HOST, FTP_PORT);
                ftpClient.login(FTP_USER, FTP_PASS);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);

                int totalDeleted = 0;
                // [UPDATED] Iterate through all profiles
                for (TechProfile profile : ACTIVE_PROFILES) {
                    for (String region : REGIONS) {
                        String donePath = profile.ftpSubFolder + "/" + region + "/Done";
                        totalDeleted += deleteFilesInDirectory(ftpClient, donePath);
                    }
                }
                System.out.printf("🧹 [CLEANER] Finished. Deleted %d files.\n", totalDeleted);
                ftpClient.logout();
                ftpClient.disconnect();
            } catch (Exception e) {
                System.err.println("❌ [CLEANER] Error: " + e.getMessage());
            }
        }, CLEANUP_INTERVAL_MINUTES, CLEANUP_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    private static void uploadWithRetry(String ftpPath, String fileName, byte[] fileData) throws Exception {
        createdPaths.computeIfAbsent(ftpPath, path -> {
            try {
                FTPClient client = getFtpClient();
                ensureDirectoryExists(client, path);
                ensureDirectoryExists(client, path.endsWith("/") ? (path + "Done") : (path + "/Done"));
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        String remoteFilePath = ftpPath.endsWith("/") ? (ftpPath + fileName) : (ftpPath + "/" + fileName);

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (InputStream inputStream = new ByteArrayInputStream(fileData)) {
                FTPClient ftpClient = getFtpClient();
                ftpClient.changeWorkingDirectory("/");
                if (ftpClient.storeFile(remoteFilePath, inputStream)) return;
                throw new IOException("FTP store failed");
            } catch (Exception e) {
                ftpClientThreadLocal.remove();
                if (attempt == MAX_RETRIES) throw e;
                Thread.sleep(100);
            }
        }
    }

    private static FTPClient getFtpClient() throws IOException {
        FTPClient ftpClient = ftpClientThreadLocal.get();
        if (ftpClient == null || !ftpClient.isConnected() || !ftpClient.sendNoOp()) {
            if (ftpClient != null) try { ftpClient.disconnect(); } catch (IOException ignored) {}
            ftpClient = new FTPClient();
            ftpClient.connect(FTP_HOST, FTP_PORT);
            if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) throw new IOException("Connection refused");
            if (!ftpClient.login(FTP_USER, FTP_PASS)) throw new IOException("Login failed");
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setBufferSize(1024 * 1024);
            ftpClientThreadLocal.set(ftpClient);
        }
        return ftpClient;
    }

    private static void ensureDirectoryExists(FTPClient ftpClient, String dirPath) throws IOException {
        String[] pathElements = dirPath.split("/");
        if (pathElements.length == 0) return;
        if (dirPath.startsWith("/")) ftpClient.changeWorkingDirectory("/");
        for (String singleDir : pathElements) {
            if (!singleDir.isEmpty()) {
                if (!ftpClient.changeWorkingDirectory(singleDir)) {
                    if (!ftpClient.makeDirectory(singleDir) && !ftpClient.changeWorkingDirectory(singleDir)) {
                        if (!ftpClient.changeWorkingDirectory(singleDir)) {
                            // Directory creation check
                        }
                    } else {
                        ftpClient.changeWorkingDirectory(singleDir);
                    }
                }
            }
        }
    }

    private static long calculateSleepTimeForNextSchedule(int intervalMinutes) {
        LocalDateTime now = LocalDateTime.now();
        int currentMinute = now.getMinute();
        int remainder = currentMinute % intervalMinutes;
        int minutesToAdd = intervalMinutes - remainder;
        LocalDateTime nextTarget = now.plusMinutes(minutesToAdd).withSecond(0).withNano(0);
        if (!nextTarget.isAfter(now)) nextTarget = nextTarget.plusMinutes(intervalMinutes);
        return ChronoUnit.MILLIS.between(now, nextTarget);
    }
}