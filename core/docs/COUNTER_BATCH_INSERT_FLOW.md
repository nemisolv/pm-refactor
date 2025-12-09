# Counter Batch Insert Flow

## Tổng quan

Hệ thống insert performance counters vào MySQL với 2 tối ưu chính:
1. **Batch Insert**: Gộp nhiều counters vào 1 câu INSERT (giảm DB round-trips)
2. **Multi-threading**: Insert song song các bảng khác nhau (tăng throughput)

---

## Flow chi tiết

### Input
```java
List<CounterObject> counters = [
  Counter(table=pm_cell, time=10:00, neId=123, cell=1, counterId=1, value=100),
  Counter(table=pm_cell, time=10:00, neId=123, cell=1, counterId=2, value=200),
  Counter(table=pm_cell, time=10:00, neId=123, cell=1, counterId=10, value=300),
  Counter(table=pm_cell, time=10:15, neId=124, cell=2, counterId=1, value=150),
  Counter(table=pm_node, time=10:00, neId=456, counterId=5, value=500),
  Counter(table=pm_port, time=10:00, neId=789, counterId=7, value=700)
]
```

---

### PHASE 1: Group by Table (groupCode)

**Method**: `addCounter()`

**Logic**:
```java
Map<String, List<CounterObject>> countersByTable = lstCounter.stream()
    .collect(Collectors.groupingBy(CounterObject::getGroupCode));
```

**Kết quả**:
```
pm_cell: [4 counters]
pm_node: [1 counter]
pm_port: [1 counter]
```

**Decision**:
- Nếu `countersByTable.size() > 1` → PARALLEL INSERT (3 bảng → 3 threads)
- Nếu `countersByTable.size() == 1` → SEQUENTIAL INSERT (1 bảng → 1 thread)

---

### PHASE 2A: Parallel Execution (Multiple Tables)

**Method**: `executeParallelInsert()`

**Thread Pool**:
```java
threadPoolSize = min(số_bảng, số_CPU_cores)
// Ví dụ: min(3, 8) = 3 threads
```

**Parallel Execution**:
```
Thread-1 (Connection-1): pm_cell  → INSERT INTO pm_cell ...
Thread-2 (Connection-2): pm_node  → INSERT INTO pm_node ...  } PARALLEL
Thread-3 (Connection-3): pm_port  → INSERT INTO pm_port ...
```

**Key Point**: Mỗi thread lấy **DEDICATED CONNECTION** từ HikariCP pool
- `jdbcTemplate.execute(ConnectionCallback)` → Đảm bảo connection riêng
- MySQL execute parallel khi các INSERT từ connections khác nhau
- HikariCP pool size: 10-15 connections (đủ cho parallel threads)

---

### PHASE 2B: Sequential Execution (Single Table)

**Method**: `executeSequentialInsert()`

**Logic**: Không tạo thread pool, execute trực tiếp
```
Main Thread: pm_cell → INSERT INTO pm_cell ...
```

---

### PHASE 3: Build SQL (Per Table)

**Method**: `buildMultiRowInsertSQL(tableName, counters)`

**Input**: Counters của 1 bảng (ví dụ pm_cell)
```java
[
  Counter(time=10:00, neId=123, cell=1, counterId=1, value=100),
  Counter(time=10:00, neId=123, cell=1, counterId=2, value=200),
  Counter(time=10:00, neId=123, cell=1, counterId=10, value=300),
  Counter(time=10:15, neId=124, cell=2, counterId=1, value=150)
]
```

#### Step 3.1: Group by RowKey

**RowKey** = (time, duration, neId, extraField)

**Logic**:
```java
Map<RowKey, List<CounterObject>> countersByRow = counters.stream()
    .collect(Collectors.groupingBy(counter ->
        new RowKey(time, duration, neId, sExtraField, extraField)
    ));
```

**Kết quả**:
```
RowKey(time=10:00, neId=123, cell=1): [counterId=1, 2, 10]
RowKey(time=10:15, neId=124, cell=2): [counterId=1]
```

**WHY?**
- Các counters có cùng timestamp/metadata → cùng 1 row trong DB
- Mỗi counter là 1 column (c_1, c_2, c_10, ...)

#### Step 3.2: Collect All Counter IDs

**Logic**:
```java
Set<Integer> allCounterIds = counters.stream()
    .map(CounterObject::getCounterId)
    .collect(Collectors.toSet());
```

**Kết quả**: `{1, 2, 10}`

**WHY?**
- Cần tạo columns cho TẤT CẢ counters xuất hiện trong bất kỳ row nào
- Row không có counter → value = NULL

#### Step 3.3: Build Column List

```sql
INSERT INTO pm_cell (record_time, duration, ne_id, cell, c_1, c_2, c_10)
```

**Structure**:
1. Fixed columns: `record_time, duration, ne_id`
2. Extra fields: `cell` (từ extraField)
3. Counter columns: `c_1, c_2, c_10` (sorted)

#### Step 3.4: Build VALUES Rows

**Row 1**: RowKey(time=10:00, neId=123, cell=1)
- Có counters: c_1=100, c_2=200, c_10=300
- VALUES: `('10:00:00', 900, 123, 1, 100, 200, 300)`

**Row 2**: RowKey(time=10:15, neId=124, cell=2)
- Có counters: c_1=150
- Thiếu: c_2, c_10 → NULL
- VALUES: `('10:15:00', 900, 124, 2, 150, NULL, NULL)`

#### Final SQL

```sql
INSERT INTO pm_cell (record_time, duration, ne_id, cell, c_1, c_2, c_10) VALUES
  ('10:00:00', 900, 123, 1, 100, 200, 300),
  ('10:15:00', 900, 124, 2, 150, NULL, NULL);
```

---

### PHASE 4: Execute SQL

**Method**: `executeQueryWithDedicatedConnection()`

#### Connection Handling

```java
jdbcTemplate.execute((Connection conn) -> {
    try (Statement stmt = conn.createStatement()) {
        stmt.execute(sqlInsert);
    }
    return null;
});
```

**KEY**: `ConnectionCallback` đảm bảo:
1. Lấy **dedicated connection** từ HikariCP pool
2. Connection thuộc về thread này trong suốt quá trình execute
3. Sau khi execute xong, connection trả về pool

**Flow**:
```
Thread-1: Get connection-1 from pool → Execute SQL → Return connection-1 to pool
Thread-2: Get connection-2 from pool → Execute SQL → Return connection-2 to pool
Thread-3: Get connection-3 from pool → Execute SQL → Return connection-3 to pool
```

#### Error Handling

1. **Unknown column**:
   - Parse SQL để tìm missing columns (c_xxx)
   - Execute `ALTER TABLE ADD COLUMN c_xxx bigint(15)`
   - Retry INSERT (max: `Constant.MAX_RETRY_DB`)

2. **Duplicate entry**: Return `ErrorCode.ERROR_DUPLICATE_RECORD`

3. **Other errors**: Return `ErrorCode.ERROR_UNKNOWN`

---

## Performance Comparison

### Before Optimization

**Scenario**: 10 counters, 1 bảng (pm_cell)

```sql
INSERT INTO pm_cell (record_time, duration, ne_id, c_1) VALUES ('10:00:00', 900, 123, 100);
INSERT INTO pm_cell (record_time, duration, ne_id, c_2) VALUES ('10:00:00', 900, 123, 200);
INSERT INTO pm_cell (record_time, duration, ne_id, c_3) VALUES ('10:00:00', 900, 123, 300);
... (7 more INSERT statements)
```

- **10 DB round-trips**
- **10 network calls**
- **Slow**: Mỗi INSERT phải chờ ACK từ DB

### After Optimization

```sql
INSERT INTO pm_cell (record_time, duration, ne_id, c_1, c_2, c_3, ..., c_10) VALUES
  ('10:00:00', 900, 123, 100, 200, 300, ..., 1000);
```

- **1 DB round-trip**
- **1 network call**
- **Fast**: Batch insert, MySQL optimize internally

**Performance gain**: ~10x faster

---

## Multi-threading Benefit

### Scenario: 3 bảng, mỗi bảng 100 counters

#### Without Multi-threading (Sequential)
```
Time 0s:  INSERT pm_cell (2s)
Time 2s:  INSERT pm_node (2s)
Time 4s:  INSERT pm_port (2s)
Total: 6s
```

#### With Multi-threading (Parallel)
```
Time 0s:  INSERT pm_cell (2s)  ┐
          INSERT pm_node (2s)  ├─ PARALLEL
          INSERT pm_port (2s)  ┘
Total: 2s
```

**Performance gain**: 3x faster (với 3 bảng)

---

## Configuration

### HikariCP Pool Size
```yaml
datasources:
  primary:
    maximumPoolSize: 15
```

**Recommended**:
- Pool size ≥ Số threads parallel (thường là số CPU cores)
- Default: 10-15 connections (đủ cho hầu hết trường hợp)

### Thread Pool Size
```java
threadPoolSize = min(num_tables, num_cpu_cores)
```

**Example**:
- Server 8 cores, 5 bảng → 5 threads
- Server 8 cores, 20 bảng → 8 threads (avoid thread overhead)

---

## Important Notes

### NULL vs 0
```java
Long value = counterValues.get(counterId);
rowValue.append(", ").append(value != null ? value : "NULL");
```

- `value = 0` → Insert `0` (valid counter value)
- `value = null` → Insert `NULL` (counter không tồn tại cho row này)

### RowKey Grouping

**KHÔNG group RowKey** = Insert mỗi counter thành 1 row riêng (SAI!)
```sql
-- SAI: 3 rows cho 3 counters cùng timestamp
INSERT INTO pm_cell (record_time, ne_id, c_1) VALUES ('10:00', 123, 100);
INSERT INTO pm_cell (record_time, ne_id, c_2) VALUES ('10:00', 123, 200);
INSERT INTO pm_cell (record_time, ne_id, c_10) VALUES ('10:00', 123, 300);
```

**CÓ group RowKey** = Insert 1 row với nhiều counters (ĐÚNG!)
```sql
-- ĐÚNG: 1 row cho 3 counters cùng timestamp
INSERT INTO pm_cell (record_time, ne_id, c_1, c_2, c_10)
VALUES ('10:00', 123, 100, 200, 300);
```

---

## Summary

| Phase | Method | Purpose | Output |
|-------|--------|---------|--------|
| 1 | `addCounter()` | Group by table | Map<table, counters> |
| 2A | `executeParallelInsert()` | Multi-threading | N parallel tasks |
| 2B | `executeSequentialInsert()` | Single-thread | 1 sequential task |
| 3 | `buildMultiRowInsertSQL()` | Build SQL | Multi-row INSERT statement |
| 4 | `executeQueryWithDedicatedConnection()` | Execute SQL | ErrorCode |

**Key Optimizations**:
1. ✅ Batch insert → Giảm DB round-trips từ N xuống 1
2. ✅ Multi-threading → Parallel insert nhiều bảng
3. ✅ Dedicated connections → True parallel execution ở MySQL level
4. ✅ Dynamic schema → Auto-create missing counter columns
