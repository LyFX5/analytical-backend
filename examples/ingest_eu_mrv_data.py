"""
Example: Using the Ingestion Pipeline with Real EU MRV Data

This script demonstrates how to use the ingestion pipeline once you have
downloaded Excel files from https://mrv.emsa.europa.eu/#public/emission-report

Usage:
    1. Download Excel files from EU MRV portal
    2. Place them in a directory (e.g., data/raw/)
    3. Run this script:
       python examples/ingest_eu_mrv_data.py
"""

import logging
from pathlib import Path
from ingestion.pipeline import IngestionPipeline
from ingestion.excel_parser import EUMRVExcelParser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def inspect_eu_mrv_file(filepath):
    """
    Inspect an EU MRV Excel file before ingestion.
    
    This helps you understand the file structure and adjust parser settings.
    """
    print(f"\n{'='*60}")
    print(f"INSPECTING: {filepath}")
    print('='*60)
    
    parser = EUMRVExcelParser(filepath)
    
    # List sheets
    sheets = parser.inspect_sheets()
    print(f"\n✓ Sheets in file: {sheets}")
    
    # Try parsing first sheet
    print("\nAttempting to parse first sheet...")
    try:
        df, validation = parser.parse_with_validation(sheet_name=sheets[0])
        
        print(f"✓ Rows parsed: {len(df)}")
        print(f"✓ Columns found: {validation['columns_found']}")
        print(f"✓ Validation: {'PASS' if validation['is_valid'] else 'FAIL'}")
        
        if not validation['is_valid']:
            print(f"⚠ Missing required columns: {validation['missing_required']}")
        
        print("\nFirst 5 rows:")
        print(df.head())
        
        return True
        
    except Exception as e:
        print(f"✗ Error parsing: {e}")
        return False


def ingest_all_files(data_dir, db_path="data/pipeline.db"):
    """
    Ingest all Excel files from a directory.
    
    Args:
        data_dir: Directory containing EU MRV Excel files
        db_path: Path to SQLite database
    """
    data_dir = Path(data_dir)
    
    if not data_dir.exists():
        print(f"Error: Directory {data_dir} does not exist")
        return
    
    # Find all Excel files
    excel_files = list(data_dir.glob("*.xlsx")) + list(data_dir.glob("*.xls"))
    
    if not excel_files:
        print(f"No Excel files found in {data_dir}")
        return
    
    print(f"\nFound {len(excel_files)} Excel file(s)")
    
    # Initialize pipeline
    pipeline = IngestionPipeline(db_path=db_path)
    
    # Ingest each file
    results = []
    for filepath in excel_files:
        print(f"\nProcessing: {filepath.name}")
        result = pipeline.ingest_file(str(filepath))
        results.append(result)
        
        if result['status'] == 'success':
            print(f"  ✓ Success: {result['rows_ingested']} rows ingested")
        elif result['status'] == 'skipped':
            print(f"  ⊘ Skipped (already uploaded)")
        else:
            print(f"  ✗ Failed: {result['error_message']}")
    
    # Summary
    print(f"\n{'='*60}")
    print("INGESTION SUMMARY")
    print('='*60)
    
    successful = sum(1 for r in results if r['status'] == 'success')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    failed = sum(1 for r in results if r['status'] not in ['success', 'skipped'])
    
    print(f"Total files: {len(excel_files)}")
    print(f"Successful: {successful}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")
    
    # Database summary
    summary = pipeline.get_ingestion_summary()
    print(f"\nDatabase status:")
    print(f"  Total entities: {summary['total_entities']}")
    print(f"  Years available: {summary['years_available']}")
    print(f"  Total records: {summary['total_records']}")
    
    if summary.get('type_distribution'):
        print(f"\n  Distribution by type:")
        for item in summary['type_distribution'][:5]:
            print(f"    - {item['type']}: {item['count']}")


def main():
    """Main entry point."""
    print("="*60)
    print("EU MRV DATA INGESTION EXAMPLE")
    print("="*60)
    
    # Configuration
    DATA_DIR = Path("data/raw")  # Where you put downloaded Excel files
    DB_PATH = "data/pipeline.db"
    
    # Check if data directory exists
    if not DATA_DIR.exists():
        print(f"\nData directory '{DATA_DIR}' not found.")
        print("\nTo use this script:")
        print("1. Create directory: mkdir -p data/raw")
        print("2. Download Excel files from: https://mrv.emsa.europa.eu/#public/emission-report")
        print("3. Place files in data/raw/")
        print("4. Run this script again")
        
        # For now, demonstrate with synthetic data
        print("\n" + "="*60)
        print("Running with synthetic data for demonstration...")
        print("="*60)
        
        from ingestion.data_loader import SyntheticFleetGenerator
        import tempfile
        
        generator = SyntheticFleetGenerator(n_entities=100, years=range(2020, 2024))
        
        temp_dir = Path(tempfile.mkdtemp())
        test_file = temp_dir / "synthetic_eu_mrv.xlsx"
        generator.to_excel(str(test_file))
        
        print(f"Generated synthetic file: {test_file}")
        
        # Inspect
        inspect_eu_mrv_file(str(test_file))
        
        # Ingest
        ingest_all_files(temp_dir, db_path=DB_PATH)
        
        return
    
    # Inspect first file
    excel_files = list(DATA_DIR.glob("*.xlsx"))
    if excel_files:
        inspect_eu_mrv_file(str(excel_files[0]))
        
        # Ask for confirmation
        response = input("\nProceed with ingestion of all files? (y/n): ")
        if response.lower() == 'y':
            ingest_all_files(DATA_DIR, db_path=DB_PATH)
        else:
            print("Ingestion cancelled.")


if __name__ == "__main__":
    main()
