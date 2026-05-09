"""
Data Ingestion Package

Modules:
- data_loader: Synthetic data generation for testing
- excel_parser: EU MRV Excel file parsing
- db_manager: Database abstraction layer (SQLite/PostgreSQL)
- pipeline: Main ingestion orchestrator
"""

from .data_loader import DataLoader, SyntheticFleetGenerator
from .excel_parser import EUMRVExcelParser
from .db_manager import DatabaseManager
from .pipeline import IngestionPipeline, compute_file_hash

__all__ = [
    'DataLoader',
    'SyntheticFleetGenerator',
    'EUMRVExcelParser',
    'DatabaseManager',
    'IngestionPipeline',
    'compute_file_hash',
]
