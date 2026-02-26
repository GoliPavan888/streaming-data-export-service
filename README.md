# 🚀 Streaming Data Export Service

A high-performance FastAPI service that streams 10 million PostgreSQL records into CSV, JSON, XML, and Parquet using constant memory.

This project demonstrates production-grade data streaming, serialization trade-offs, and containerized resource enforcement.

---

## 📌 Key Highlights

- ✅ Streams **10,000,000 rows** (~1.5GB CSV) without loading data into memory
- ✅ Maintains constant memory usage (~75MB under 256MB limit)
- ✅ Supports **CSV, JSON, XML, and Parquet**
- ✅ Optional **gzip compression** for text formats
- ✅ Strategy-pattern exporter architecture
- ✅ Performance benchmark endpoint
- ✅ Fully Dockerized with PostgreSQL seeding

---

## 🧠 Why This Project Matters

Exporting large datasets by loading everything into memory is not scalable and leads to crashes.

This service demonstrates:

- **True constant-memory streaming**
- **Server-side database cursors**
- **Chunked HTTP responses**
- **Format-specific serialization strategies**
- **Resource-constrained container validation**

This mirrors how production systems (e.g., billing exports, reporting engines, analytics pipelines) handle large data safely.

---

## 🏗 Architecture Overview

### Components

| Layer | Responsibility |
|-------|---------------|
| PostgreSQL | Stores 10M seeded records |
| FastAPI | API layer & streaming endpoints |
| Exporter Layer | Format-specific streaming serializers |
| Docker | Enforces memory limits (256MB) |

### 📂 Project Structure

```
source_code/
  exporters/
    base.py
    factory.py
    csv_exporter.py
    json_exporter.py
    xml_exporter.py
    parquet_exporter.py
    utils.py
  db.py
  main.py

docker-compose.yml
Dockerfile
.env.example
init-db.sh
init-db.sql
README.md
```

---

## 🗄 Database Schema

**Table: `records`**

| Column | Type |
|--------|------|
| `id` | BIGSERIAL PRIMARY KEY |
| `created_at` | TIMESTAMP WITH TIME ZONE |
| `name` | VARCHAR(255) |
| `value` | DECIMAL(18,4) |
| `metadata` | JSONB |

The database is automatically seeded with exactly **10,000,000 rows** at startup.

---

## ⚙ Setup

### 1️⃣ Clone & Configure

Copy environment file:

```bash
cp .env.example .env
```

Edit if needed.

### 2️⃣ Start the Stack

```bash
docker-compose up --build
```

The application will be available at:

**http://localhost:8080**

---

## 🔌 API Documentation

### 🔹 POST /exports

Creates a new export job.

**Example Request**

```json
{
  "format": "csv",
  "columns": [
    {"source": "id", "target": "id"},
    {"source": "created_at", "target": "created_at"},
    {"source": "name", "target": "name"},
    {"source": "value", "target": "value"},
    {"source": "metadata", "target": "metadata"}
  ],
  "compression": "gzip"
}
```

**Response (201)**

```json
{
  "exportId": "uuid",
  "status": "pending"
}
```

### 🔹 GET /exports/{exportId}/download

Streams export data.

**Content Types**

| Format | Content-Type |
|--------|-------------|
| CSV | text/csv |
| JSON | application/json |
| XML | application/xml |
| Parquet | application/vnd.apache.parquet |

If compression is enabled:

```
Content-Encoding: gzip
```

Streaming uses:

```
Transfer-Encoding: chunked
```

### 🔹 GET /exports/benchmark

Runs full 10M export for all formats and returns metrics.

**Example Response**

```json
{
  "datasetRowCount": 10000000,
  "results": [
    {
      "format": "csv",
      "durationSeconds": 23.4,
      "fileSizeBytes": 1495777390,
      "peakMemoryMB": 75
    },
    {
      "format": "json",
      "durationSeconds": 27.8,
      "fileSizeBytes": 1689342210,
      "peakMemoryMB": 78
    },
    {
      "format": "xml",
      "durationSeconds": 32.1,
      "fileSizeBytes": 2013341120,
      "peakMemoryMB": 80
    },
    {
      "format": "parquet",
      "durationSeconds": 18.6,
      "fileSizeBytes": 420113984,
      "peakMemoryMB": 82
    }
  ]
}
```

---

## 📊 Performance Validation

### Dataset

- **10,000,000 rows**
- **~1.5GB CSV output**

### Observed Results

- Peak memory usage: **~75MB**
- Memory limit enforced: **256MB**
- No OOM errors
- Stable memory during entire export
- Chunked transfer confirmed

### Memory Monitoring

```bash
docker stats
```

Memory remained constant throughout export.

---

## 🔄 Streaming Design

The service uses:

- PostgreSQL server-side cursors
- Batched row fetching
- Generator-based streaming
- FastAPI StreamingResponse

Each batch is:

1. Fetched from DB
2. Serialized immediately
3. Written to response stream

**No full dataset is ever stored in memory.**

Memory usage remains independent of dataset size.

---

## 📦 Nested JSON Handling

| Format | JSONB Representation |
|--------|---------------------|
| CSV | Serialized as JSON string in cell |
| JSON | Native JSON object |
| XML | Nested XML elements |
| Parquet | Nested struct column |

---

## 🧩 Extending Export Formats

To add a new format:

1. Create a new exporter in `source_code/exporters/`
2. Implement the base exporter interface
3. Register it in `factory.py`

No changes required to API layer.

---

## 🔐 Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |

---

## 🧪 Validation Commands

**Verify row count:**

```bash
docker exec -it <db-container> psql -U user -d exports_db -c "SELECT COUNT(*) FROM records;"
```

**Verify CSV row count safely:**

```bash
cmd /c "find /v /c """" export.csv"
```

**Monitor memory:**

```bash
docker stats
```

---

## 🎯 Technical Concepts Demonstrated

- Constant-memory streaming
- Large dataset handling (10M rows)
- Format-specific serialization
- Container resource enforcement
- Strategy design pattern
- Performance benchmarking
- Production-ready Docker setup

---

## 🏁 Project Status

This project successfully streams 10M records into multiple formats under strict memory limits and validates performance trade-offs across data serialization formats.

---

## 📌 Author

**Pavan Kumar Goli**