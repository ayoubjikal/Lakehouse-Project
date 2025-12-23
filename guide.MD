# Complete Real-Time Data Lake Workflow 🔄

## Overview: How Data Flows Through Your System

```
Data Simulator → FastAPI → MinIO (Raw) → Kafka → Spark → MinIO (Processed) → Iceberg → Trino
```

Let's follow a single file through the entire journey!

---

## Stage 1: Data Simulator Sends Files 📤

### What Happens:
The simulator continuously sends JSON files to the FastAPI endpoint every 0.01 seconds (~100 files/sec).

### Watch it Happen:
```bash
docker-compose logs -f data-simulator
```

### You Should See:
```
Loaded 5 files from /app/data/sessions
Loaded 2 files from /app/data/employees
Loaded 2 files from /app/data/clients
Sent file sessions_1.json for session - Status: 200
Sent file employees_4.json for employee - Status: 200
Sent file clients_2.json for client - Status: 200
Sent file sessions_2.json for session - Status: 200
...repeating continuously...
```

### Key Point:
- Files are sent in a **continuous loop** (same files over and over)
- Each successful upload returns **Status: 200**
- The simulator cycles through all files in each folder

---

## Stage 2: FastAPI Receives & Uploads to MinIO 📥

### What Happens:
1. FastAPI receives the JSON file
2. Uploads it to MinIO's `uploads` bucket
3. Publishes a message to Kafka with the file location

### Watch it Happen:
```bash
docker-compose logs -f webservice
```

### You Should See:
```
INFO: 172.x.x.x:xxxxx - "POST /upload/file/sessions_1.json HTTP/1.1" 200 OK
```

### Verify in MinIO Console:
1. Open http://localhost:9001
2. Login: `admin` / `password`
3. Click on **"Buckets"** → **"uploads"**
4. You should see folders:
   - `sessions/` → Contains `sessions_1.json`, `sessions_2.json`, etc.
   - `employee_files/` → Contains employee JSON files
   - `client_files/` → Contains client JSON files

### What You'll Notice in MinIO:
❗ **IMPORTANT:** The raw JSON files in the `uploads` bucket will **NOT** continuously grow in number. The simulator sends the **same files repeatedly**, so you'll always see the **same ~9 files** (5 sessions + 2 employees + 2 clients).

**This is NORMAL!** The files are being overwritten with the same content each time.

---

## Stage 3: Kafka Receives Messages 📨

### What Happens:
FastAPI publishes a pointer message to Kafka for each uploaded file:
```json
{
  "file_name": "sessions_1.json",
  "file_path": "uploads/sessions/sessions_1.json"
}
```

### Watch it Happen:
```bash
# See messages in session_topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic session_topic \
  --from-beginning \
  --max-messages 5
```

### You Should See:
```json
{"file_name":"sessions_1.json","file_path":"uploads/sessions/sessions_1.json"}
{"file_name":"sessions_2.json","file_path":"uploads/sessions/sessions_2.json"}
{"file_name":"sessions_3.json","file_path":"uploads/sessions/sessions_3.json"}
...
```

Press `Ctrl+C` to stop.

### Key Point:
- Kafka **accumulates ALL messages** (not just unique files)
- If the same file is sent 100 times, Kafka will have 100 messages
- This is why data keeps growing downstream!

---

## Stage 4: Spark Reads from Kafka & Processes 🔥

### What Happens:
1. Spark reads messages from Kafka in micro-batches (every 0.1 seconds)
2. Downloads the JSON file from MinIO using the path in the message
3. Validates the data (removes nulls, checks status values)
4. Deduplicates by UUID
5. Merges into Iceberg tables

### Watch it Happen:
```bash
docker-compose logs -f spark | grep -E "Processing batch|Successfully processed"
```

### You Should See:
```
📦 Processing batch 0 for sessions
💾 Writing 1 records to temporary table...
🔄 Merging into sessions...
✅ Successfully processed 1 records for sessions
========================
📦 Processing batch 1 for sessions
💾 Writing 1 records to temporary table...
🔄 Merging into sessions...
✅ Successfully processed 1 records for sessions
...
```

### Key Point:
- Even though the **same files** are processed repeatedly
- **Spark deduplicates by UUID** during the merge
- So the same UUID won't create multiple records
- BUT: Each processing cycle still writes to Iceberg (creating snapshots)

---

## Stage 5: Data Written to MinIO Warehouse (Parquet) 💾

### What Happens:
Spark writes processed data as **Parquet files** to the `warehouse` bucket in MinIO.

### Verify in MinIO Console:
1. Open http://localhost:9001
2. Navigate to **"warehouse"** bucket
3. Browse to: `warehouse/default.db/sessions/data/`

### What You Should See:
```
warehouse/
└── default.db/
    ├── sessions/
    │   ├── data/
    │   │   ├── status=approved/
    │   │   │   ├── 00000-0-xxx.parquet  (100 KB)
    │   │   │   ├── 00001-1-xxx.parquet  (95 KB)
    │   │   │   └── 00002-2-xxx.parquet  (88 KB)
    │   │   └── status=resubmission/
    │   │       ├── 00000-0-xxx.parquet  (105 KB)
    │   │       └── 00001-1-xxx.parquet  (92 KB)
    │   └── metadata/
    │       ├── v1.metadata.json
    │       ├── v2.metadata.json
    │       ├── snap-xxxx.avro
    │       └── ...
    ├── employees/
    │   ├── data/
    │   │   └── ...parquet files...
    │   └── metadata/
    └── clients/
        ├── data/
        │   └── ...parquet files...
        └── metadata/
```

### 🔍 IMPORTANT - What's Actually Growing:

#### ✅ **YES - These ARE increasing:**
1. **Parquet file COUNT** in `warehouse/default.db/*/data/`
   - New files are created with each batch
   - Files accumulate over time
   - Eventually compaction merges small files into larger ones

2. **Metadata files** in `warehouse/default.db/*/metadata/`
   - Each Iceberg "snapshot" creates metadata
   - These track table versions

3. **Total bucket size**
   - You'll see the warehouse bucket size grow in MinIO

#### ❌ **NO - These are NOT increasing:**
1. **Raw JSON files in `uploads` bucket**
   - Same ~9 files, just being overwritten
   - File count stays constant

---

## Stage 6: Query Data with Trino 🔍

### What Happens:
Trino reads Iceberg metadata and queries the Parquet files in MinIO.

### Watch it Happen:
```bash
docker exec -it trino trino
```

Then run:
```sql
-- Check record count (run this multiple times)
SELECT COUNT(*) FROM iceberg_datalake.default.sessions;
```

### What You Should See:

**First query (after ~30 seconds):**
```
 _col0 
-------
    5  
```

**Second query (after ~60 seconds):**
```
 _col0 
-------
    5  
```

**Wait... the count isn't increasing?** 🤔

---

## 🎯 Why Record Count Might NOT Increase (And That's OK!)

### The Key Understanding:

The simulator sends the **same files with the same UUIDs** repeatedly:
- `sessions_1.json` → UUID: `8ba4520a-4ce5-43ee-aa0a-561d453d6539`
- `sessions_2.json` → UUID: `8b443ebe-3ad4-43fa-b668-c1d452a3ce1e`
- etc.

When Spark merges data:
```sql
MERGE INTO sessions AS target
USING new_data AS source
ON target.uuid = source.uuid  -- 🔑 This prevents duplicates!
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

**Result:**
- First time: 5 unique sessions are **INSERTED** → Count = 5
- Second time: Same 5 UUIDs are **UPDATED** (not inserted again) → Count = 5
- Every subsequent time: **UPDATES** → Count = 5

### So What IS Growing?

1. **Iceberg snapshots** (table versions)
2. **Parquet files** (until compaction merges them)
3. **Metadata files**

Check snapshots:
```sql
SELECT COUNT(*) FROM iceberg_datalake.default."sessions$snapshots";
```

This WILL increase over time!

---

## 🧪 Experiment: Make the Count Actually Grow

To see records actually increasing, you need to send **NEW UUIDs**. Here's how:

### Option 1: Create New Test Files

```bash
# Create a new session with unique UUID
cat > data/sessions/sessions_new.json << 'EOF'
{
    "uuid": "new-uuid-$(date +%s)",
    "status": "approved",
    "client_uuid": "test-client-uuid",
    "specialist_uuid": "test-specialist-uuid"
}
EOF

# Upload it
curl -X POST \
  -F "file=@data/sessions/sessions_new.json" \
  http://localhost:8000/upload/file/sessions_new.json

# Wait 5 seconds and query
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg_datalake.default.sessions"
```

Now the count should increase!

### Option 2: Modify Simulator to Generate Random UUIDs

Edit `simulator/simulator.py` to generate unique UUIDs on the fly (more advanced).

---

## 📊 Complete Verification Workflow

### Step 1: Check Raw Files (Stays Constant)
```bash
# MinIO Console → uploads bucket
# Should see ~9 files (same files)
docker exec mc mc ls minio/uploads/sessions/
```

### Step 2: Check Kafka Messages (Keeps Growing)
```bash
# Count messages in topic
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic session_topic \
  --time -1
```

Output shows offset (total messages):
```
session_topic:0:450  ← 450 messages processed!
```

### Step 3: Check Spark Processing (Continuous)
```bash
docker-compose logs spark | grep "Successfully processed" | wc -l
```

Shows how many batches Spark has processed.

### Step 4: Check Parquet Files (Grows Then Compacts)
```bash
# Count parquet files in warehouse
docker exec mc mc ls --recursive minio/warehouse/default.db/sessions/data/ | grep ".parquet" | wc -l
```

This number grows until compaction happens.

### Step 5: Check Snapshots (Always Growing)
```sql
SELECT COUNT(*) as snapshot_count 
FROM iceberg_datalake.default."sessions$snapshots";
```

This will steadily increase!

### Step 6: Check File Sizes (Growing)
```bash
# Check warehouse bucket size
docker exec mc mc du minio/warehouse/
```

Total size increases over time.

---

## 🎬 Live Demo: Watch Everything Flow

Open **4 terminal windows**:

### Terminal 1: Watch Simulator Send
```bash
docker-compose logs -f data-simulator | grep "Status: 200"
```

### Terminal 2: Watch Spark Process
```bash
docker-compose logs -f spark | grep "✅"
```

### Terminal 3: Watch Parquet Files Grow
```bash
watch -n 5 'docker exec mc mc ls --recursive minio/warehouse/default.db/sessions/data/ | grep ".parquet" | wc -l'
```

### Terminal 4: Watch Snapshots Increase
```bash
watch -n 5 'docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg_datalake.default.\"sessions\$snapshots\""'
```

---

## 📈 What You'll Actually See Over Time

### After 1 Minute:
- Simulator: Sent ~6,000 files
- Kafka: ~6,000 messages
- Spark: Processed ~600 batches
- Parquet files: ~50 files
- Unique records: 5 sessions, 2 employees, 2 clients
- Snapshots: ~600

### After 5 Minutes:
- Simulator: Sent ~30,000 files
- Kafka: ~30,000 messages
- Spark: Processed ~3,000 batches
- Parquet files: ~200 files (compaction started)
- Unique records: Still 5, 2, 2 (same UUIDs!)
- Snapshots: ~3,000
- Warehouse size: ~50 MB

### After 30 Minutes:
- Compaction has merged many small files
- Warehouse size: ~100 MB
- Everything still running smoothly
- Record count: STILL the same (because same UUIDs!)

---

## ✅ Success Criteria

Your pipeline is working perfectly if:

1. ✅ Simulator continuously shows "Status: 200"
2. ✅ Parquet files appear in MinIO warehouse bucket
3. ✅ Snapshot count in Iceberg keeps increasing
4. ✅ Trino queries return data instantly
5. ✅ No errors in Spark logs
6. ✅ Warehouse bucket size grows over time
7. ✅ Record count is 5+2+2=9 (with default test data)

**The record count staying constant is CORRECT BEHAVIOR** because you're processing the same UUIDs repeatedly!

---

## 🎯 Summary: What's Growing vs What's Not

| Component | What Happens | Expected Behavior |
|-----------|--------------|-------------------|
| **uploads/ bucket** | Same 9 JSON files | ❌ Does NOT grow (overwritten) |
| **Kafka messages** | Accumulates all messages | ✅ Keeps growing |
| **Parquet files** | New files created per batch | ✅ Grows (then compacts) |
| **Metadata files** | New snapshot each merge | ✅ Keeps growing |
| **Unique records** | Deduped by UUID | ❌ Stays at 9 (same UUIDs) |
| **Snapshots** | Each write creates snapshot | ✅ Keeps growing |
| **Warehouse size** | Total data stored | ✅ Keeps growing |

---

## 🚀 Next Steps

Want to see records actually grow? Create a script that generates unique UUIDs:

```bash
# Generate and upload files with unique UUIDs
for i in {1..100}; do
  UUID=$(uuidgen)
  cat > /tmp/session_$i.json << EOF
{
    "uuid": "$UUID",
    "status": "approved",
    "client_uuid": "client-$RANDOM",
    "specialist_uuid": "specialist-$RANDOM"
}
EOF
  
  curl -X POST \
    -F "file=@/tmp/session_$i.json" \
    http://localhost:8000/upload/file/session_$i.json
    
  sleep 0.1
done
```

Now watch the record count grow in real-time! 🎉