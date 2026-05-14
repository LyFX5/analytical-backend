"""
Test script for the data ingestion pipeline.

Demonstrates:
1. Generate synthetic Excel file
2. Parse Excel with EUMRVExcelParser
3. Ingest into SQLite database
4. Query and verify data
5. Test idempotency (duplicate prevention)
"""

from sys import exit
import logging
import tempfile
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def test_synthetic_excel_generation():
    """Test 1: Generate synthetic Excel file."""
    print("\n" + "=" * 60)
    print("TEST 1: Synthetic Excel Generation")
    print("=" * 60)

    from ingestion.data_loader import SyntheticFleetGenerator

    # Generate small dataset for testing
    generator = SyntheticFleetGenerator(n_entities=50, years=range(2020, 2023))

    # Create temp file
    temp_dir = Path(tempfile.mkdtemp())
    excel_path = temp_dir / "test_fleet_data.xlsx"

    # Export to Excel
    generator.to_excel(str(excel_path))

    print(f"✓ Generated Excel file: {excel_path}")
    print(f"  File size: {excel_path.stat().st_size} bytes")

    return excel_path


def test_excel_parsing(excel_path):
    """Test 2: Parse Excel file."""
    print("\n" + "=" * 60)
    print("TEST 2: Excel Parsing")
    print("=" * 60)

    from ingestion.excel_parser import EUMRVExcelParser

    parser = EUMRVExcelParser(str(excel_path))

    # Inspect sheets
    sheets = parser.inspect_sheets()
    print(f"✓ Sheets found: {sheets}")

    # Parse with validation
    df, validation = parser.parse_with_validation()

    print(f"✓ Parsed {len(df)} rows")
    print(f"✓ Columns: {validation['columns_found']}")
    print(
        f"✓ Validation status: {'PASS' if validation['is_valid'] else 'FAIL'}"
    )

    if validation["null_counts"]:
        print(f"✓ Null counts: {validation['null_counts']}")

    print("\nSample data:")
    print(df.head())

    return df


def test_database_ingestion(excel_path):
    """Test 3: Ingest into database."""
    print("\n" + "=" * 60)
    print("TEST 3: Database Ingestion")
    print("=" * 60)

    from ingestion.pipeline import IngestionPipeline
    import tempfile
    from pathlib import Path

    # Use temp database
    temp_dir = Path(tempfile.mkdtemp())
    db_path = temp_dir / "test_pipeline.db"

    # Initialize pipeline
    pipeline = IngestionPipeline(db_path=str(db_path))

    # Ingest file
    result = pipeline.ingest_file(str(excel_path))

    print(f"✓ Ingestion status: {result['status']}")
    print(f"✓ Rows parsed: {result['rows_parsed']}")
    print(f"✓ Rows ingested: {result['rows_ingested']}")

    if result["error_message"]:
        print(f"⚠ Error: {result['error_message']}")

    # Get summary
    summary = pipeline.get_ingestion_summary()
    print(f"\n✓ Database summary:")
    print(f"  Total entities: {summary['total_entities']}")
    print(f"  Years available: {summary['years_available']}")
    print(f"  Total records: {summary['total_records']}")

    if summary.get("type_distribution"):
        print(f"  Type distribution:")
        for item in summary["type_distribution"]:
            print(f"    - {item['type']}: {item['count']}")

    return pipeline


def test_idempotency(pipeline, excel_path):
    """Test 4: Verify duplicate prevention."""
    print("\n" + "=" * 60)
    print("TEST 4: Idempotency (Duplicate Prevention)")
    print("=" * 60)

    # Try to ingest same file again
    result = pipeline.ingest_file(str(excel_path))

    print(f"✓ Second ingestion status: {result['status']}")

    if result["status"] == "skipped":
        print("✓ Duplicate correctly detected and skipped!")
    else:
        print(f"⚠ Expected 'skipped' but got: {result['status']}")

    # Check final count
    summary = pipeline.get_ingestion_summary()
    print(f"✓ Total records after second attempt: {summary['total_records']}")

    return result["status"] == "skipped"


def test_database_queries(pipeline):
    """Test 5: Query database."""
    print("\n" + "=" * 60)
    print("TEST 5: Database Queries")
    print("=" * 60)

    # Query specific entity
    entities = pipeline.db_manager.get_all_entities()
    if entities:
        sample_entity = entities[0]
        query_result = pipeline.db_manager.query(
            "SELECT * FROM entity_year WHERE entity_id = ? LIMIT 5",
            (sample_entity,),
        )
        print(f"✓ Sample entity ({sample_entity}) history:")
        print(query_result.to_string())

    # Aggregation query
    agg_query = """
        SELECT 
            type,
            COUNT(*) as count,
            AVG(co2) as avg_co2,
            AVG(km) as avg_km
        FROM entity_year
        WHERE type IS NOT NULL
        GROUP BY type
        ORDER BY count DESC
    """

    result = pipeline.db_manager.query(agg_query)
    print(f"\n✓ Aggregation by type:")
    print(result.to_string())


def run_all_tests():
    """Run all tests in sequence."""
    print("\n" + "#" * 60)
    print("# DATA INGESTION PIPELINE - TEST SUITE")
    print("#" * 60)

    try:
        # Test 1: Generate Excel
        excel_path = test_synthetic_excel_generation()

        # Test 2: Parse Excel
        df = test_excel_parsing(excel_path)

        # Test 3: Ingest to DB
        pipeline = test_database_ingestion(excel_path)

        # Test 4: Idempotency
        idempotency_passed = test_idempotency(pipeline, excel_path)

        # Test 5: Queries
        test_database_queries(pipeline)

        # Summary
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        print(f"✓ All tests completed successfully!")
        print(
            f"✓ Idempotency check: {'PASS' if idempotency_passed else 'FAIL'}"
        )

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
