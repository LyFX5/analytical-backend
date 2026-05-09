# Data Ingestion Pipeline - Architecture & Usage Guide

## Overview

This document describes the data ingestion pipeline architecture for Phase 0 and Phase 1 of the EU MRV analytics project.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION PIPELINE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ Excel Files  │───▶│   Parser     │───▶│ Transformer  │      │
│  │ (EU MRV)     │    │ (excel_parser│    │ (pipeline.py)│      │
│  │              │    │  .py)        │    │              │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                              │                       │          │
│                              │                       ▼          │
│                              │            ┌──────────────┐      │
│                              │            │  Validator   │      │
│                              │            │  & Logger    │      │
│                              │            └──────────────┘      │
│                              │                       │          │
│                              ▼                       ▼          │
│                     ┌─────────────────────────────────┐         │
│                     │      Database Manager           │         │
│                     │       (db_manager.py)           │         │
│                     └─────────────────────────────────┘         │
│                                      │                          │
│                                      ▼                          │
│                     ┌─────────────────────────────────┐         │
│                     │        SQLite Database          │         │
│                     │   (PostgreSQL-compatible)       │         │
│                     └─────────────────────────────────┘         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Module Structure

### `/workspace/ingestion/`

```
ingestion/
├── __init__.py           # Package initialization
├── data_loader.py        # Synthetic data generator (for testing)
├── excel_parser.py       # EU MRV Excel file parser
├── db_manager.py         # Database abstraction layer
└── pipeline.py           # Main ingestion orchestrator
```

## Key Components

### 1. EUMRVExcelParser (`excel_parser.py`)

**Purpose**: Parse EU MRV Excel files into normalized DataFrames

**Features**:
- Auto-detect relevant sheets
- Handle complex headers and merged cells
- Column name normalization (mapping to canonical schema)
- Data type coercion
- Validation reporting

**Usage**:
```python
from ingestion.excel_parser import EUMRVExcelParser

parser = EUMRVExcelParser("path/to/eu_mrv_file.xlsx")

# Inspect available sheets
sheets = parser.inspect_sheets()

# Parse with validation
df, validation_report = parser.parse_with_validation()

if validation_report['is_valid']:
    print(f"Parsed {len(df)} rows successfully")
else:
    print(f"Missing columns: {validation_report['missing_required']}")
```

### 2. DatabaseManager (`db_manager.py`)

**Purpose**: Abstract database operations with PostgreSQL migration path

**Features**:
- Schema management (CREATE TABLE, indexes)
- Bulk data loading
- Idempotency tracking (upload log)
- Query interface
- PostgreSQL-compatible SQL

**Database Schema**:

```sql
-- Main data table
entity_year (
    id, entity_id, year, operator_id, type,
    size, age, fuel, co2, km,
    created_at, updated_at
)

-- Derived features (populated by preprocessing)
features_entity_year (
    id, entity_id, year,
    co2_per_km, fuel_per_km,
    zscore_co2_peer, percentile_co2_peer,
    age_bucket, size_bucket,
    co2_prev_1y, trend_co2_3y,
    peer_group, cluster_id
)

-- Operator aggregations
operator_year (
    id, operator_id, year,
    fleet_size, avg_co2_per_km,
    weighted_efficiency, composition_vector
)

-- Upload tracking (idempotency)
upload_log (
    id, filepath, filename, file_hash,
    rows_ingested, status, error_message,
    uploaded_at
)
```

**Usage**:
```python
from ingestion.db_manager import DatabaseManager

db = DatabaseManager("data/pipeline.db")
db.initialize_schema()

# Load data
rows_inserted = db.load_dataframe(df, 'entity_year', if_exists='append')

# Query data
results = db.query(
    "SELECT * FROM entity_year WHERE type = ? AND year = ?",
    ('long-haul', 2023)
)

# Get summary
entities = db.get_all_entities()
years = db.get_years_available()
```

### 3. IngestionPipeline (`pipeline.py`)

**Purpose**: Orchestrate complete ingestion workflow

**Features**:
- File hash-based deduplication
- Parse → Validate → Transform → Load workflow
- Comprehensive error handling
- Logging and status tracking
- Batch processing support

**Usage**:
```python
from ingestion.pipeline import IngestionPipeline

# Initialize pipeline
pipeline = IngestionPipeline(db_path="data/pipeline.db")

# Ingest single file
result = pipeline.ingest_file("path/to/file.xlsx")
print(f"Status: {result['status']}")
print(f"Rows ingested: {result['rows_ingested']}")

# Ingest multiple files
files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
results = pipeline.ingest_multiple_files(files)

# Get summary
summary = pipeline.get_ingestion_summary()
print(f"Total entities: {summary['total_entities']}")
print(f"Years available: {summary['years_available']}")
```

## Testing

Run the test suite:

```bash
cd /workspace
PYTHONPATH=/workspace python tests/test_ingestion_pipeline.py
```

Tests cover:
1. ✓ Synthetic Excel generation
2. ✓ Excel parsing and validation
3. ✓ Database ingestion
4. ✓ Idempotency (duplicate prevention)
5. ✓ Database queries

## PostgreSQL Migration Guide

### When to Migrate?

**Stay with SQLite if**:
- Dataset < 1M rows
- Single-user access
- No concurrent writes
- Simple deployment (no separate DB server)
- Development/prototyping phase

**Migrate to PostgreSQL if**:
- Dataset > 1M rows or growing fast
- Multiple concurrent users/writers
- Need for advanced features (JSONB, full-text search, etc.)
- Production deployment with high availability requirements
- Complex transactions needed

### Migration Steps

The codebase is designed for easy migration:

1. **Install PostgreSQL adapter**:
   ```bash
   pip install psycopg2-binary
   # or
   pip install sqlalchemy
   ```

2. **Update `db_manager.py` connection**:
   ```python
   # Current (SQLite)
   import sqlite3
   conn = sqlite3.connect(db_path)
   
   # Future (PostgreSQL with psycopg2)
   import psycopg2
   conn = psycopg2.connect(
       host="localhost",
       database="eu_mrv",
       user="username",
       password="password"
   )
   
   # Or with SQLAlchemy (recommended for abstraction)
   from sqlalchemy import create_engine
   engine = create_engine("postgresql://user:pass@host/dbname")
   ```

3. **Schema adjustments**:
   ```sql
   -- SQLite AUTOINCREMENT → PostgreSQL SERIAL
   id INTEGER PRIMARY KEY AUTOINCREMENT
   → id SERIAL PRIMARY KEY
   
   -- SQLite REAL → PostgreSQL DOUBLE PRECISION
   column_name REAL
   → column_name DOUBLE PRECISION
   ```

4. **Upsert syntax**:
   ```sql
   -- SQLite
   INSERT OR IGNORE INTO table (...) VALUES (...)
   
   -- PostgreSQL
   INSERT INTO table (...) VALUES (...)
   ON CONFLICT (unique_column) DO NOTHING
   ```

### Using SQLAlchemy (Recommended Approach)

For maximum portability, consider adding SQLAlchemy:

```python
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class EntityYear(Base):
    __tablename__ = 'entity_year'
    id = Column(Integer, primary_key=True)
    entity_id = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    # ... other columns
    
    __table_args__ = (
        UniqueConstraint('entity_id', 'year', name='uq_entity_year')
    )

# Works with both SQLite and PostgreSQL
engine = create_engine("sqlite:///pipeline.db")  # or postgresql://...
Base.metadata.create_all(engine)
```

## Acceptance Criteria Checklist

### Phase 0 — Exploration and Data Contract

- [x] Data schema defined (EU MRV + enrichment structure)
- [x] Synthetic dataset generator
- [x] Working pipeline skeleton
- [ ] KPI definitions documented
- [ ] Peer model defined
- [ ] Initial insights (3-4) identified
- [ ] Architecture diagram (this document)

### Phase 1 — Data Pipeline & Storage

- [x] Excel ingestion (EU MRV format)
- [x] Schema validation
- [x] Database design (SQL)
- [x] Working ingestion pipeline
- [x] Logging + failure handling
- [x] Idempotency (no duplicate data on repeated runs)
- [x] Database is queryable
- [ ] Admin upload flow (API endpoint - future work)

## Next Steps

1. **Download real EU MRV data** from https://mrv.emsa.europa.eu/#public/emission-report
2. **Test parser** with actual Excel files
3. **Adjust column mappings** in `EUMRVExcelParser.COLUMN_MAPPINGS` based on real data structure
4. **Define core KPIs** (CO₂/km, fuel efficiency, etc.)
5. **Implement preprocessing** layer (cleaning, normalization, bucketing)
6. **Build feature engineering** module for peer clustering

## Questions & Considerations

### SQLite vs PostgreSQL Decision Matrix

| Criterion | SQLite | PostgreSQL |
|-----------|--------|------------|
| Setup complexity | None (file-based) | Requires server |
| Concurrent writes | Limited (file locks) | Excellent |
| Max database size | ~140 TB theoretical | Unlimited |
| Performance (<1M rows) | Excellent | Excellent |
| Performance (>10M rows) | Degrades | Optimized |
| JSON support | Basic (TEXT) | Advanced (JSONB) |
| Full-text search | Limited | Built-in |
| Backup | Copy file | pg_dump, replication |
| Deployment | Embedded | Client-server |

**Recommendation**: Start with SQLite for Phase 0-1. Monitor data volume and access patterns. Migrate to PostgreSQL when you hit:
- Concurrent write conflicts
- Query performance degradation
- Need for advanced PostgreSQL features

### Data Volume Estimation

Based on EU MRV public data:
- Ships: ~10,000-15,000 vessels
- Years: 2018-present (~7 years)
- Rows: ~100,000 total

**SQLite handles this easily**. PostgreSQL becomes necessary at ~1M+ rows or with many concurrent users.
