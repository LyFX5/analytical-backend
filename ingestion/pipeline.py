"""
Data Ingestion Pipeline Module

Orchestrates the complete ingestion flow:
1. Excel file upload
2. Parsing and validation
3. Database storage with idempotency
4. Logging and error handling
"""

import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

from .excel_parser import EUMRVExcelParser
from .db_manager import DatabaseManager

logger = logging.getLogger(__name__)


def compute_file_hash(filepath: str) -> str:
    """
    Compute SHA256 hash of a file for deduplication.
    
    Args:
        filepath: Path to file
        
    Returns:
        Hex digest of file hash
    """
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read in chunks for large files
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


class IngestionPipeline:
    """
    Main ingestion pipeline orchestrator.
    
    Handles:
    - File validation
    - Idempotency checks (prevent duplicate uploads)
    - Parse → Validate → Load workflow
    - Error handling and logging
    """
    
    def __init__(self, db_path: str = "data/pipeline.db"):
        """
        Initialize ingestion pipeline.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_manager = DatabaseManager(db_path=db_path)
        self.db_manager.initialize_schema()
        logger.info("IngestionPipeline initialized")
    
    def ingest_file(
        self,
        filepath: str,
        sheet_name: Optional[str] = None,
        skip_duplicates: bool = True
    ) -> Dict:
        """
        Ingest a single Excel file into the database.
        
        Args:
            filepath: Path to Excel file
            sheet_name: Specific sheet to parse (auto-detect if None)
            skip_duplicates: If True, skip files already uploaded
            
        Returns:
            Ingestion result dictionary with status and metrics
        """
        filepath = Path(filepath)
        filename = filepath.name
        file_hash = compute_file_hash(str(filepath))
        
        result = {
            'filepath': str(filepath),
            'filename': filename,
            'file_hash': file_hash,
            'status': 'pending',
            'rows_parsed': 0,
            'rows_ingested': 0,
            'validation_errors': [],
            'error_message': None
        }
        
        try:
            # Check for duplicate upload
            if skip_duplicates and self.db_manager.is_file_already_uploaded(file_hash):
                logger.info(f"File already processed, skipping: {filename}")
                result['status'] = 'skipped'
                result['error_message'] = 'File already uploaded'
                return result
            
            logger.info(f"Starting ingestion: {filename}")
            
            # Step 1: Parse Excel
            parser = EUMRVExcelParser(str(filepath))
            df, validation_report = parser.parse_with_validation(
                sheet_name=sheet_name
            )
            
            result['rows_parsed'] = len(df)
            result['columns_found'] = validation_report['columns_found']
            
            # Step 2: Validate structure
            if not validation_report['is_valid']:
                result['status'] = 'validation_failed'
                result['validation_errors'] = validation_report['missing_required']
                result['error_message'] = (
                    f"Missing required columns: {validation_report['missing_required']}"
                )
                
                self.db_manager.log_upload(
                    filepath=str(filepath),
                    filename=filename,
                    file_hash=file_hash,
                    rows_ingested=0,
                    status='validation_failed',
                    error_message=result['error_message']
                )
                
                logger.warning(
                    f"Validation failed for {filename}: "
                    f"{result['validation_errors']}"
                )
                return result
            
            # Step 3: Transform data for database
            df_clean = self._transform_for_storage(df)
            
            # Step 4: Load to database
            rows_inserted = self.db_manager.load_dataframe(
                df=df_clean,
                table_name='entity_year',
                if_exists='append'
            )
            
            result['rows_ingested'] = rows_inserted
            result['status'] = 'success'
            
            # Step 5: Log successful upload
            self.db_manager.log_upload(
                filepath=str(filepath),
                filename=filename,
                file_hash=file_hash,
                rows_ingested=rows_inserted,
                status='success'
            )
            
            logger.info(
                f"Ingestion complete: {filename} - "
                f"{rows_inserted} rows inserted"
            )
            
        except Exception as e:
            result['status'] = 'error'
            result['error_message'] = str(e)
            logger.exception(f"Ingestion failed for {filename}: {e}")
            
            self.db_manager.log_upload(
                filepath=str(filepath),
                filename=filename,
                file_hash=file_hash,
                rows_ingested=0,
                status='error',
                error_message=str(e)
            )
        
        return result
    
    def _transform_for_storage(self, df) -> any:
        """
        Transform parsed DataFrame for database storage.
        
        Performs:
        - Generate entity_id if missing
        - Normalize categorical values
        - Handle missing values
        - Ensure correct data types
        
        Args:
            df: Raw parsed DataFrame
            
        Returns:
            Transformed DataFrame ready for storage
        """
        import pandas as pd
        import numpy as np
        
        df = df.copy()
        
        # Generate entity_id if missing
        if 'entity_id' not in df.columns:
            if 'entity_name' in df.columns:
                # Create hash-based ID from name
                df['entity_id'] = df['entity_name'].apply(
                    lambda x: hashlib.md5(str(x).encode()).hexdigest()[:12]
                    if pd.notna(x) else None
                )
            else:
                # Fallback: use row index
                df['entity_id'] = range(len(df))
        
        # Normalize type/category values
        if 'type' in df.columns:
            df['type'] = df['type'].astype(str).str.lower().str.strip()
        
        # Normalize operator_id
        if 'operator_id' in df.columns:
            df['operator_id'] = df['operator_id'].astype(str).str.strip()
        
        # Ensure year is integer
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        
        # Keep only canonical columns for entity_year table
        canonical_columns = [
            'entity_id', 'year', 'operator_id', 'type', 
            'size', 'age', 'fuel', 'co2', 'km'
        ]
        
        available_columns = [
            col for col in canonical_columns 
            if col in df.columns
        ]
        
        df_clean = df[available_columns].copy()
        
        # Remove rows with missing critical fields
        critical_fields = ['entity_id', 'year', 'co2', 'km']
        available_critical = [f for f in critical_fields if f in df_clean.columns]
        
        initial_rows = len(df_clean)
        df_clean = df_clean.dropna(subset=available_critical)
        dropped_rows = initial_rows - len(df_clean)
        
        if dropped_rows > 0:
            logger.info(f"Dropped {dropped_rows} rows with missing critical fields")
        
        return df_clean
    
    def ingest_multiple_files(
        self,
        filepaths: List[str],
        **kwargs
    ) -> List[Dict]:
        """
        Ingest multiple Excel files.
        
        Args:
            filepaths: List of file paths
            **kwargs: Additional arguments passed to ingest_file
            
        Returns:
            List of ingestion results
        """
        results = []
        
        for filepath in filepaths:
            result = self.ingest_file(filepath, **kwargs)
            results.append(result)
        
        # Summary
        total_files = len(results)
        successful = sum(1 for r in results if r['status'] == 'success')
        skipped = sum(1 for r in results if r['status'] == 'skipped')
        failed = total_files - successful - skipped
        
        logger.info(
            f"Batch ingestion complete: "
            f"{successful}/{total_files} successful, "
            f"{skipped} skipped, "
            f"{failed} failed"
        )
        
        return results
    
    def get_ingestion_summary(self) -> Dict:
        """
        Get summary statistics of ingested data.
        
        Returns:
            Dictionary with summary metrics
        """
        summary = {
            'total_entities': len(self.db_manager.get_all_entities()),
            'years_available': self.db_manager.get_years_available(),
            'total_records': self.db_manager.get_table_count('entity_year'),
            'uploads_log_count': self.db_manager.get_table_count('upload_log')
        }
        
        # Get operator distribution
        if summary['total_records'] > 0:
            operator_dist = self.db_manager.query("""
                SELECT operator_id, COUNT(*) as count
                FROM entity_year
                WHERE operator_id IS NOT NULL
                GROUP BY operator_id
                ORDER BY count DESC
                LIMIT 10
            """)
            summary['top_operators'] = operator_dist.to_dict('records')
        
        # Get type distribution
        type_dist = self.db_manager.query("""
            SELECT type, COUNT(*) as count
            FROM entity_year
            WHERE type IS NOT NULL
            GROUP BY type
            ORDER BY count DESC
        """)
        summary['type_distribution'] = type_dist.to_dict('records')
        
        return summary
