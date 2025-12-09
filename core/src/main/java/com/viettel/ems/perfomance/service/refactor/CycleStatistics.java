package com.viettel.ems.perfomance.service.refactor;

import com.viettel.ems.perfomance.config.SystemType;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class CycleStatistics {

    private final SystemType systemName;
    private Instant startTime;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    // --- Metrics ---
    private final AtomicInteger filesScanned = new AtomicInteger(0);
    private final AtomicInteger filesQueued = new AtomicInteger(0);
    private final AtomicInteger filesProcessedSuccess = new AtomicInteger(0);
    private final AtomicInteger filesFailed = new AtomicInteger(0);
    private final AtomicInteger filesDownloadedCount = new AtomicInteger(0);
    private final AtomicInteger filesParsedCount = new AtomicInteger(0);

    // Timers
    private final LongAdder totalDownloadTimeMs = new LongAdder();
    private final LongAdder totalParseTimeMs = new LongAdder();
    private final LongAdder totalDbTimeMs = new LongAdder();

    // DB metrics
    private final AtomicInteger dbBatchesInserted = new AtomicInteger(0);
    private final LongAdder totalRecordsInserted = new LongAdder();

    public CycleStatistics(SystemType systemName) {
        this.systemName = systemName;
        this.startTime = Instant.now();
    }

    // --- Update Methods ---
    public void addScannedCount(int count) { filesScanned.addAndGet(count); }
    public void incrementQueued() { filesQueued.incrementAndGet(); }
    public void incrementHandleSuccess() { filesProcessedSuccess.incrementAndGet(); }
    public void incrementFailed() { filesFailed.incrementAndGet(); }

    public void recordDownload(long ms) {
        totalDownloadTimeMs.add(ms);
        filesDownloadedCount.incrementAndGet();
    }

    public void recordParseTime(long ms) {
        totalParseTimeMs.add(ms);
        filesParsedCount.incrementAndGet();
    }

    public void recordDbInsert(long ms, int recordCount) {
        totalDbTimeMs.add(ms);
        dbBatchesInserted.incrementAndGet();
        totalRecordsInserted.add(recordCount);
    }

    public void reportAndReset() {
        Instant now = Instant.now();
        long durationSec = Duration.between(startTime, now).toSeconds();
        if (durationSec == 0) durationSec = 1;

        // Snapshot
        int scanned = filesScanned.get();
        int queued = filesQueued.get();
        int success = filesProcessedSuccess.get();
        int failed = filesFailed.get();

        if (scanned == 0 && success == 0 && failed == 0) {
            reset();
            return;
        }

        // Calculate Averages
        double avgDownload = filesDownloadedCount.get() > 0
                ? totalDownloadTimeMs.sum() / (double) filesDownloadedCount.get() : 0;

        double avgParse = filesParsedCount.get() > 0
                ? totalParseTimeMs.sum() / (double) filesParsedCount.get() : 0;

        int batches = dbBatchesInserted.get();
        double avgDbBatchTime = batches > 0 ? totalDbTimeMs.sum() / (double) batches : 0;
        long totalRecords = totalRecordsInserted.sum();

        // Build Table String
        StringBuilder sb = new StringBuilder();
        String border = "+-------------------------------------------------------------+";

        sb.append("\n").append(border);
        // Header
        sb.append(String.format("\n| %-25s | %-31s |", "System: " + systemName.name(), "Time: " + TIME_FORMATTER.format(now)));
        sb.append(String.format("\n| %-25s | %-31s |", "Duration: " + durationSec + "s", ""));
        sb.append("\n").append(border);

        // Flow Stats
        sb.append(String.format("\n| %-59s |", "=== Flow Stats ==="));
        sb.append("\n").append(border);
        sb.append(String.format("\n| %-25s | %15s files %-9s |", "Files Scanned", String.format("%,d", scanned), "(total)"));
        sb.append(String.format("\n| %-25s | %15s files %-9s |", "Files Queued", String.format("%,d", queued), "(queued)"));
        sb.append(String.format("\n| %-25s | %15s files %-9s |", "Files Success", String.format("%,d", success), ""));
        sb.append(String.format("\n| %-25s | %15s files %-9s |", "Files Failed", String.format("%,d", failed), ""));
        sb.append("\n").append(border);

        // DB & Performance Stats
        sb.append(String.format("\n| %-59s |", "=== Performance (Avg Latency) ==="));
        sb.append("\n").append(border);
        sb.append(String.format("\n| %-25s | %18.2f ms          |", "Avg Download", avgDownload));
        sb.append(String.format("\n| %-25s | %18.2f ms          |", "Avg Parse", avgParse));
        sb.append(String.format("\n| %-25s | %18.2f ms          |", "Avg DB Batch Insert", avgDbBatchTime));
        sb.append(String.format("\n| %-25s | %15s records       |", "Total Records", String.format("%,d", totalRecords)));
        sb.append(String.format("\n| %-25s | %15s batches       |", "Total Batches", String.format("%,d", batches)));
        sb.append("\n").append(border);

        log.info(sb.toString());

        reset();
    }

    void reset() {
        startTime = Instant.now();
        filesScanned.set(0);
        filesQueued.set(0);
        filesProcessedSuccess.set(0);
        filesFailed.set(0);
        filesDownloadedCount.set(0);
        filesParsedCount.set(0);
        totalDownloadTimeMs.reset();
        totalParseTimeMs.reset();
        totalDbTimeMs.reset();
        dbBatchesInserted.set(0);
        totalRecordsInserted.reset();
    }
}