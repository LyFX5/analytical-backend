"""
Database Manager Module

Handles SQLite database operations for the data pipeline.
Designed with abstraction layer to facilitate future PostgreSQL migration.
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    SQLite database manager with PostgreSQL-compatible design.
    
    Key design decisions for easy PostgreSQL migration:
    1. Use standard SQL (avoid SQLite-specific features)
    2. Abstract connection handling
    3. Use parameterized queries
    4. Separate schema definitions from logic
    5. Avoid SQLite-specific data types
    
    PostgreSQL migration considerations:
    - SQLite: INTEGER, REAL, TEXT, BLOB, NULL
    - PostgreSQL: INTEGER, DOUBLE PRECISION, VARCHAR/TEXT, BYTEA, NULL
    - Most basic operations translate directly
    - Main differences: sequences vs autoincrement, some date functions
    """
    
    # Canonical schema for entity_year table
    SCHEMA_ENTITY_YEAR = """
        CREATE TABLE IF NOT EXISTS entity_year (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            operator_id TEXT,
            type TEXT,
            size REAL,
            age INTEGER,
            fuel REAL,
            co2 REAL,
            km REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(entity_id, year)
        )
    """
    
    # Schema for derived features
    SCHEMA_FEATURES = """
        CREATE TABLE IF NOT EXISTS features_entity_year (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            co2_per_km REAL,
            fuel_per_km REAL,
            zscore_co2_peer REAL,
            percentile_co2_peer REAL,
            age_bucket TEXT,
            size_bucket TEXT,
            co2_prev_1y REAL,
            trend_co2_3y REAL,
            peer_group TEXT,
            cluster_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (entity_id, year) REFERENCES entity_year(entity_id, year),
            UNIQUE(entity_id, year)
        )
    """
    
    # Schema for operator aggregations
    SCHEMA_OPERATOR_YEAR = """
        CREATE TABLE IF NOT EXISTS operator_year (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operator_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            fleet_size INTEGER,
            avg_co2_per_km REAL,
            weighted_efficiency REAL,
            composition_vector TEXT,  -- JSON string
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(operator_id, year)
        )
    """
    
    # Schema for upload tracking (idempotency)
    SCHEMA_UPLOAD_LOG = """
        CREATE TABLE IF NOT EXISTS upload_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filepath TEXT NOT NULL,
            filename TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            rows_ingested INTEGER,
            status TEXT,
            error_message TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(file_hash)
        )
    """
    
    def __init__(self, db_path: str = "data/pipeline.db"):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Database initialized at: {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Yields:
            sqlite3.Connection object
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def initialize_schema(self):
        """Create all tables if they don't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            schemas = [
                self.SCHEMA_ENTITY_YEAR,
                self.SCHEMA_FEATURES,
                self.SCHEMA_OPERATOR_YEAR,
                self.SCHEMA_UPLOAD_LOG
            ]
            
            for schema in schemas:
                cursor.execute(schema)
            
            # Create indexes for common query patterns
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_entity ON entity_year(entity_id)",
                "CREATE INDEX IF NOT EXISTS idx_year ON entity_year(year)",
                "CREATE INDEX IF NOT EXISTS idx_operator ON entity_year(operator_id)",
                "CREATE INDEX IF NOT EXISTS idx_type ON entity_year(type)",
                "CREATE INDEX IF NOT EXISTS idx_entity_year ON entity_year(entity_id, year)",
                "CREATE INDEX IF NOT EXISTS idx_features_entity ON features_entity_year(entity_id, year)",
                "CREATE INDEX IF NOT EXISTS idx_operator_year ON operator_year(operator_id, year)",
            ]
            
            for idx_sql in indexes:
                cursor.execute(idx_sql)
            
            logger.info("Database schema initialized successfully")
    
    def load_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        if_exists: str = 'append'
    ) -> int:
        """
        Load DataFrame into database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: 'append', 'replace', or 'skip'
            
        Returns:
            Number of rows inserted
        """
        with self.get_connection() as conn:
            if if_exists == 'skip' and self.table_exists(table_name):
                # Check for duplicates using UNIQUE constraint
                pass
            
            rows_before = self.get_table_count(table_name)
            
            # Use pandas to_sql for efficient bulk insert
            df.to_sql(
                table_name, 
                conn, 
                if_exists=if_exists,
                index=False,
                method='multi',  # Batch inserts for performance
                chunksize=1000
            )
            
            rows_after = self.get_table_count(table_name)
            rows_inserted = rows_after - rows_before
            
            logger.info(
                f"Loaded {rows_inserted} rows into {table_name} "
                f"(total: {rows_after})"
            )
            
            return rows_inserted
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            return cursor.fetchone() is not None
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        if not self.table_exists(table_name):
            return 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
    
    def log_upload(
        self,
        filepath: str,
        filename: str,
        file_hash: str,
        rows_ingested: int,
        status: str,
        error_message: Optional[str] = None
    ):
        """Log an upload attempt for idempotency tracking."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO upload_log 
                (filepath, filename, file_hash, rows_ingested, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (filepath, filename, file_hash, rows_ingested, status, error_message)
            )
    
    def is_file_already_uploaded(self, file_hash: str) -> bool:
        """Check if a file has already been processed."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM upload_log WHERE file_hash = ?",
                (file_hash,)
            )
            result = cursor.fetchone()
            if result:
                return result['status'] == 'success'
            return False
    
    def query(
        self, 
        sql: str, 
        params: Optional[tuple] = None
    ) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as DataFrame.
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            DataFrame with query results
        """
        with self.get_connection() as conn:
            return pd.read_sql_query(sql, conn, params=params or ())
    
    def get_all_entities(self) -> List[str]:
        """Get list of all unique entity IDs."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT entity_id FROM entity_year")
            return [row['entity_id'] for row in cursor.fetchall()]
    
    def get_years_available(self) -> List[int]:
        """Get list of all years in the dataset."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT year FROM entity_year ORDER BY year")
            return [row['year'] for row in cursor.fetchall()]
    
    def clear_table(self, table_name: str):
        """Clear all data from a table."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            logger.info(f"Cleared table: {table_name}")
    
    # PostgreSQL Migration Notes
    # =========================
    # When migrating to PostgreSQL, changes needed:
    #
    # 1. Connection string:
    #    - SQLite: sqlite3.connect(db_path)
    #    - PostgreSQL: psycopg2.connect(host, user, password, dbname)
    #
    # 2. Auto-increment:
    #    - SQLite: AUTOINCREMENT
    #    - PostgreSQL: SERIAL or GENERATED ALWAYS AS IDENTITY
    #
    # 3. Data types:
    #    - SQLite REAL → PostgreSQL DOUBLE PRECISION
    #    - SQLite TEXT → PostgreSQL TEXT or VARCHAR(n)
    #    - SQLite TIMESTAMP → PostgreSQL TIMESTAMP
    #
    # 4. Upsert syntax:
    #    - SQLite: INSERT OR IGNORE / INSERT OR REPLACE
    #    - PostgreSQL: INSERT ... ON CONFLICT DO NOTHING/UPDATE
    #
    # 5. Indexes: Same syntax, works for both
    #
    # The abstraction layer (get_connection, query methods) should be
    # modified to use SQLAlchemy or direct psycopg2 for PostgreSQL.
    # Core SQL queries remain largely unchanged.
