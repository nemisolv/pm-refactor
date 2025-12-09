# Performance Management System - Complete Flow Implementation

## Overview
This document describes the complete implementation of the Performance Management system for processing counter data from FTP servers, with support for both MySQL and ClickHouse persistence.

## Architecture Components

### 1. SystemManager
- **Purpose**: Bootstrap and manage multiple system instances (4GA, 5GA, ONT, 5GC)
- **Key Features**:
  - Creates dedicated thread pools for each system
  - Manages prototype-scoped PerformanceManagement instances
  - Handles graceful shutdown

### 2. PerformanceManagement
- **Purpose**: Main orchestrator for a single system type
- **Key Features**:
  - Leader election using ZooKeeper
  - Cron-based scheduling with immediate run capability
  - Manages worker pools for different processing stages
  - Tracks statistics and metrics

### 3. Processing Pipeline

#### Stage 1: File Scanning (FileProducer)
- Scans FTP directories for counter files
- Filters files based on system type (4GA vs 5GA)
- Uses cached FTP path configuration (1-hour TTL)
- Pushes file metadata to processing queue

#### Stage 2: File Processing (FileProcessorWorker)
- Downloads files from FTP using connection pooling
- Parses counter data using system-specific parsers
- Creates tracking tasks for file completion
- Pushes parsed data to database and Kafka queues

#### Stage 3A: Database Persistence (MysqlWriterWorker)
- Batch inserts counter data into MySQL
- Auto-creates missing columns dynamically
- Handles file movement to "Done" folder
- Integrates with FileProcessingTracker for completion tracking

#### Stage 3B: Kafka Production (KafkaProducerWorker)
- Converts counter data to ClickHouse format
- Sends data to Kafka for ClickHouse ingestion
- Integrates with FileProcessingTracker for completion tracking

### 4. File Processing Tracker
- **Purpose**: Coordinates file movement across multiple persistence mechanisms
- **Key Features**:
  - Tracks completion of both MySQL and Kafka operations
  - Ensures files are only moved after all operations complete
  - Handles error scenarios appropriately

### 5. Statistics and Monitoring (CycleStatistics)
- Tracks processing metrics including:
  - Files scanned, queued, processed, failed
  - Download, parse, and database operation times
  - Throughput and latency measurements
- Generates ASCII table reports

## Key Implementation Details

### Thread Pool Configuration
- **File Processors**: 20 threads (I/O bound operations)
- **Database Writers**: 4 threads (batch operations)
- **Kafka Producers**: 2 threads (when ClickHouse enabled)

### Queue Configuration
- **File Queue**: Configurable size based on system limits
- **Database Queue**: 5000 capacity
- **Kafka Queue**: 5000 capacity

### Error Handling Strategy
1. **Download Failures**: Files remain in FTP for retry
2. **Parse Failures**: Files moved to "Done" to prevent infinite loops
3. **Database Failures**: Files not moved, automatic retry on next scan
4. **Kafka Failures**: Logged but doesn't block database operations

### Backpressure Mechanisms
- Queue blocking when full
- Database writer throttling when Kafka queue is full
- Automatic retry with exponential backoff for FTP operations

### Configuration Options
- `batchInsertCounter`: Enable/disable batch database operations
- `isUsingClickhouse`: Enable ClickHouse integration via Kafka
- `isAutoAddingColumn`: Dynamic column creation in MySQL
- `consumerThreads`: Thread pool size per system

## Flow Sequence

1. **System Startup**:
   - SystemManager creates PerformanceManagement instances
   - Each instance sets up worker pools and queues
   - Leader election determines scanning responsibility

2. **Scheduled Execution**:
   - Immediate run after configurable delay
   - Cron-based recurring execution
   - Statistics reporting between cycles

3. **File Processing**:
   - Scanner discovers files and queues metadata
   - File processors download and parse files
   - Data distributed to persistence workers
   - File tracking ensures completion

4. **Completion**:
   - Files moved to "Done" folder after all operations
   - Statistics generated and reported
   - Next cycle scheduled

## Integration Points

### FTP Service Manager
- Connection pooling with health checks
- Automatic failover and reconnection
- Optimized file listing and download operations

### Database Repository
- Batch insert optimization
- Dynamic schema management
- Connection verification and retry logic

### Kafka Integration
- Asynchronous data forwarding
- Format conversion for ClickHouse
- Error handling and monitoring

## Performance Optimizations

1. **Connection Pooling**: FTP and database connections reused
2. **Batch Operations**: Database inserts grouped for efficiency
3. **Async Processing**: File movement handled on separate threads
4. **Memory Management**: Immediate cleanup of downloaded data
5. **Caching**: FTP paths and parser configuration cached

## Monitoring and Observability

- Detailed logging at each processing stage
- Metrics collection for performance analysis
- ASCII table reports for cycle statistics
- Error tracking and alerting capabilities

## Configuration Examples

```yaml
spring:
  pm:
    cron:
      schedule-scan: "50 */5 * * * *"  # Every 5 minutes at second 50
    immediate-run-delay: 10  # Seconds after startup
    is-preprocessing-data: true
    is-auto-adding-column: true

zookeeper:
  connectionString: "localhost:2181"

# System-specific configurations
4ga:
  consumerThreads: 20
  batchInsertCounter: true
  isUsingClickhouse: true

5ga:
  consumerThreads: 15
  batchInsertCounter: true
  isUsingClickhouse: false
```

## Deployment Considerations

1. **Resource Allocation**: Sufficient memory for file processing buffers
2. **Network Bandwidth**: FTP download capacity for concurrent operations
3. **Database Capacity**: Connection pool sizing and batch optimization
4. **Kafka Throughput**: Producer configuration for high-volume data
5. **Monitoring**: Log aggregation and metrics collection

## Troubleshooting

### Common Issues
1. **Queue Full**: Increase queue sizes or reduce worker counts
2. **FTP Timeouts**: Adjust connection and read timeouts
3. **Database Deadlocks**: Review batch sizes and transaction scopes
4. **Memory Leaks**: Monitor buffer cleanup and connection pooling

### Debugging Tools
- Enable DEBUG logging for detailed flow tracing
- Monitor queue sizes and processing rates
- Check FTP connection pool statistics
- Review database batch insert performance

This implementation provides a robust, scalable solution for processing performance counter data with multiple persistence mechanisms and comprehensive monitoring capabilities.