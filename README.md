# Analytical Core Inference API

Analytical core and inference API for non-obvious insights from fleet emission data.

## Project Status: Phase 0-1 (Data Pipeline Implementation)

This project implements a complete data ingestion pipeline for EU MRV (Monitoring, Reporting, Verification) emission data, with architecture designed for easy PostgreSQL migration.

## Quick Start

### Run Tests
```bash
cd /workspace
PYTHONPATH=/workspace python tests/test_ingestion_pipeline.py
```

### Example Usage
```bash
cd /workspace
PYTHONPATH=/workspace python examples/ingest_eu_mrv_data.py
```

## Project Structure

```
/workspace/
├── ingestion/              # Data ingestion pipeline
│   ├── __init__.py
│   ├── data_loader.py      # Synthetic data generator
│   ├── excel_parser.py     # EU MRV Excel parser
│   ├── db_manager.py       # Database abstraction (SQLite→PostgreSQL)
│   └── pipeline.py         # Main ingestion orchestrator
│
├── preprocessing/          # Data cleaning & validation
│   ├── cleaning.py
│   ├── preprocessing.py
│   └── validation.py
│
├── features/              # Feature engineering
│   └── feature_builder.py
│
├── analytics/             # Analytical core
│   ├── clustering.py       # Peer clustering
│   ├── benchmarking.py     # Percentile calculations
│   ├── ranking.py          # Performance rankings
│   ├── anomaly.py          # Anomaly detection
│   ├── forecasting.py      # Trend projection
│   └── aggregation.py      # Operator-level aggregations
│
├── serving/               # API layer
│   ├── api_schema.py
│   └── serializers.py
│
├── docs/                  # Documentation
│   └── INGESTION_PIPELINE_GUIDE.md
│
├── examples/              # Usage examples
│   └── ingest_eu_mrv_data.py
│
├── tests/                 # Test suite
│   └── test_ingestion_pipeline.py
│
└── main.py                # Pipeline entry point
```

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Excel Files  │───▶│   Parser     │───▶│ Transformer  │
│ (EU MRV)     │    │              │    │              │
└──────────────┘    └──────────────┘    └──────────────┘
                           │                       │
                           ▼                       ▼
                  ┌─────────────────────────────────┐
                  │      Database Manager           │
                  │   (SQLite → PostgreSQL ready)   │
                  └─────────────────────────────────┘
                                   │
                                   ▼
                  ┌─────────────────────────────────┐
                  │        SQLite Database          │
                  └─────────────────────────────────┘
```

## Key Features

### ✅ Implemented (Phase 0-1)

- **Excel Ingestion**: Parse EU MRV format Excel files
- **Schema Validation**: Automatic column mapping and data validation
- **Idempotency**: File hash-based duplicate prevention
- **Database Layer**: SQLite with PostgreSQL-compatible design
- **Logging**: Comprehensive logging and error tracking
- **Synthetic Data**: Test data generator for development

### 🔄 In Progress

- Core KPI definitions
- Peer model implementation
- Initial insights (3-4)

### 📋 Planned

- Admin upload flow (API endpoint)
- Advanced analytics module
- Full API serving layer

## Database Schema

### entity_year (Main table)
- `entity_id`, `year`, `operator_id`, `type`
- `size`, `age`, `fuel`, `co2`, `km`
- Timestamps: `created_at`, `updated_at`

### features_entity_year (Derived)
- Normalized metrics: `co2_per_km`, `fuel_per_km`
- Relative metrics: `zscore_co2_peer`, `percentile_co2_peer`
- Temporal features: `co2_prev_1y`, `trend_co2_3y`
- Peer grouping: `peer_group`, `cluster_id`

### operator_year (Aggregations)
- `fleet_size`, `avg_co2_per_km`, `weighted_efficiency`
- `composition_vector` (JSON)

### upload_log (Idempotency)
- File tracking with SHA256 hashes
- Status and error logging

## SQLite vs PostgreSQL

**Current**: SQLite (file-based, zero configuration)

**Migration to PostgreSQL** is straightforward when needed:
- Dataset exceeds ~1M rows
- Multiple concurrent writers
- Production deployment requirements

See `docs/INGESTION_PIPELINE_GUIDE.md` for detailed migration steps.

## Usage Examples

### Basic Ingestion

```python
from ingestion.pipeline import IngestionPipeline

# Initialize
pipeline = IngestionPipeline(db_path="data/pipeline.db")

# Ingest single file
result = pipeline.ingest_file("path/to/eu_mrv_file.xlsx")
print(f"Status: {result['status']}")
print(f"Rows ingested: {result['rows_ingested']}")

# Get summary
summary = pipeline.get_ingestion_summary()
print(f"Total entities: {summary['total_entities']}")
```

### Batch Processing

```python
# Ingest multiple files
files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
results = pipeline.ingest_multiple_files(files)

# Check results
for result in results:
    if result['status'] == 'success':
        print(f"✓ {result['filename']}: {result['rows_ingested']} rows")
```

### Query Database

```python
from ingestion.db_manager import DatabaseManager

db = DatabaseManager("data/pipeline.db")

# Custom query
df = db.query(
    "SELECT * FROM entity_year WHERE type = ? AND year = ?",
    ('long-haul', 2023)
)

# Built-in methods
entities = db.get_all_entities()
years = db.get_years_available()
```

## Testing

The test suite validates:
1. ✓ Synthetic Excel generation
2. ✓ Excel parsing and validation
3. ✓ Database ingestion
4. ✓ Idempotency (duplicate prevention)
5. ✓ Database queries

Run tests:
```bash
PYTHONPATH=/workspace python tests/test_ingestion_pipeline.py
```

## Next Steps

1. **Download real EU MRV data** from https://mrv.emsa.europa.eu/#public/emission-report
2. **Test parser** with actual Excel files
3. **Adjust column mappings** based on real data structure
4. **Define core KPIs** (CO₂/km, fuel efficiency, etc.)
5. **Implement preprocessing** layer
6. **Build feature engineering** for peer clustering

## Documentation

- **Full Guide**: `docs/INGESTION_PIPELINE_GUIDE.md`
- **Architecture Notes**: `rnd notes and materials/03-05-2026.md`
- **Task Sample**: `rnd notes and materials/Task Sample.md`

## Acceptance Criteria Progress

### Phase 0 — Exploration and Data Contract
- [x] Data schema defined
- [x] Synthetic dataset + working pipeline
- [x] Architecture diagram
- [ ] KPI definitions
- [ ] Peer model
- [ ] Initial insights (3-4)

### Phase 1 — Data Pipeline & Storage
- [x] Excel ingestion (EU MRV)
- [x] Schema validation
- [x] Database design (SQL)
- [x] Working ingestion pipeline
- [x] Logging + failure handling
- [x] Idempotency (no duplicates)
- [x] Database queryable
- [ ] Admin upload flow (API)

## License

Internal R&D project
