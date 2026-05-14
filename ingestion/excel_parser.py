"""
EU MRV Excel Parser Module

Handles parsing of EU MRV emission report Excel files.
Designed to be extensible for different Excel formats/versions.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class EUMRVExcelParser:
    """
    Parser for EU MRV emission report Excel files.

    The EU MRV dataset typically contains:
    - Multiple sheets (overview, detailed data, metadata)
    - Complex headers with merged cells
    - Mixed data types in columns

    This parser handles:
    - Sheet detection and selection
    - Header row identification
    - Column normalization
    - Data type coercion
    """

    # Expected column mappings (Excel header → canonical name)
    COLUMN_MAPPINGS = {
        # Identifiers
        "reporting_entity_name": "entity_name",
        "entity name": "entity_name",
        "company name": "entity_name",
        "operator": "operator_id",
        "operator id": "operator_id",
        "operator_id": "operator_id",
        # Vehicle/Asset properties
        "vehicle_type": "type",
        "truck type": "type",
        "type": "type",
        "size": "size",
        "gross_mass": "size",
        "mass": "size",
        "age": "age",
        "year_built": "year_built",
        "manufacturing_year": "year_built",
        # Performance metrics
        "fuel_consumption": "fuel",
        "fuel": "fuel",
        "fuel_liters": "fuel",
        "fuel_kl": "fuel",
        "co2_emissions": "co2",
        "co2": "co2",
        "co2_tonnes": "co2",
        "distance": "km",
        "kilometers": "km",
        "km": "km",
        "distance_km": "km",
        # Temporal
        "year": "year",
        "reporting_year": "year",
        "period": "year",
    }

    # Required columns for valid record
    REQUIRED_COLUMNS = {"entity_name", "year", "co2", "km"}

    def __init__(self, filepath: str):
        """
        Initialize parser with Excel file path.

        Args:
            filepath: Path to Excel file
        """
        self.filepath = Path(filepath)
        self._validate_file()

    def _validate_file(self):
        """Validate file exists and is readable Excel."""
        if not self.filepath.exists():
            raise FileNotFoundError(f"Excel file not found: {self.filepath}")

        valid_extensions = {".xlsx", ".xls"}
        if self.filepath.suffix.lower() not in valid_extensions:
            raise ValueError(
                f"Invalid file extension: {self.filepath.suffix}. "
                f"Expected one of {valid_extensions}"
            )

    def inspect_sheets(self) -> List[str]:
        """
        List all sheet names in the Excel file.

        Returns:
            List of sheet names
        """
        excel_file = pd.ExcelFile(self.filepath)
        return excel_file.sheet_names

    def _normalize_column_name(self, col: str) -> str:
        """
        Normalize column name to canonical form.

        Args:
            col: Original column name

        Returns:
            Normalized column name
        """
        col_lower = col.lower().strip()
        return self.COLUMN_MAPPINGS.get(col_lower, col_lower.replace(" ", "_"))

    def _identify_header_row(
        self, df_sample: pd.DataFrame, min_data_rows: int = 5
    ) -> int:
        """
        Identify which row contains the actual headers.

        EU MRV files often have:
        - Title rows at top
        - Metadata sections
        - Then actual data table

        Args:
            df_sample: Sample of dataframe to analyze
            min_data_rows: Minimum rows needed after header

        Returns:
            Row index that should be used as header
        """
        # Try first row as header
        potential_headers = df_sample.iloc[0].astype(str)

        # Check if first row looks like headers (>50% match known patterns)
        known_patterns = set(self.COLUMN_MAPPINGS.keys())

        print("======================================")
        print(potential_headers)
        for h in potential_headers:
            print(h)
        print("======================================")

        matches = sum(
            1 for h in potential_headers if h.lower().strip() in known_patterns
        )

        if matches / len(potential_headers) > 0.3:
            return 0

        # Otherwise, find first row with mostly string values
        for idx in range(len(df_sample)):
            row = df_sample.iloc[idx].astype(str)
            non_empty = row[row != "nan"]
            if len(non_empty) > len(df_sample.columns) * 0.5:
                return idx

        return 0  # Default to first row

    def parse(
        self,
        sheet_name: Optional[str] = None,
        skip_rows: int = 0,
        header_row: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Parse Excel file into normalized DataFrame.

        Args:
            sheet_name: Name of sheet to parse. If None, auto-detect.
            skip_rows: Number of rows to skip before data
            header_row: Row index to use as header (relative to skip_rows)

        Returns:
            Normalized DataFrame with canonical column names
        """
        # Auto-detect sheet if not specified
        if sheet_name is None:
            sheets = self.inspect_sheets()
            # Prefer sheets with 'data', 'report', or 'emission' in name
            for pattern in ["data", "report", "emission", "detail"]:
                for sheet in sheets:
                    if pattern.lower() in sheet.lower():
                        sheet_name = sheet
                        logger.info(f"Auto-selected sheet: {sheet_name}")
                        break
                if sheet_name:
                    break

            if not sheet_name:
                sheet_name = sheets[0]  # Default to first sheet
                logger.info(f"Using first sheet: {sheet_name}")

        logger.info(f"Parsing sheet: {sheet_name}")

        # Read raw data
        df_raw = pd.read_excel(
            self.filepath,
            sheet_name=sheet_name,
            skiprows=skip_rows,
            dtype=str,  # Read everything as string initially
        )

        # Auto-detect header row if not specified
        if header_row is None:
            header_row = self._identify_header_row(df_sample=df_raw.head(10))

        # Set proper headers
        df_raw.columns = [
            self._normalize_column_name(str(col)) for col in df_raw.columns
        ]

        # Remove completely empty rows/columns
        df_raw = df_raw.dropna(how="all", axis=0)
        df_raw = df_raw.dropna(how="all", axis=1)

        # Convert numeric columns
        numeric_columns = ["fuel", "co2", "km", "size", "age", "year"]
        for col in numeric_columns:
            if col in df_raw.columns:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

        # Log parsing summary
        logger.info(
            f"Parsed {len(df_raw)} rows, {len(df_raw.columns)} columns. "
            f"Columns: {list(df_raw.columns)}"
        )

        return df_raw

    def parse_with_validation(self, **kwargs) -> Tuple[pd.DataFrame, Dict]:
        """
        Parse Excel and validate structure.

        Returns:
            Tuple of (DataFrame, validation_report)
        """
        df = self.parse(**kwargs)

        # Validation checks
        validation_report = {
            "filepath": str(self.filepath),
            "total_rows": len(df),
            "columns_found": list(df.columns),
            "missing_required": [],
            "null_counts": {},
            "is_valid": True,
        }

        # Check required columns
        for req_col in self.REQUIRED_COLUMNS:
            if req_col not in df.columns:
                validation_report["missing_required"].append(req_col)
                validation_report["is_valid"] = False

        # Count nulls in key columns
        for col in ["co2", "km", "year", "entity_name"]:
            if col in df.columns:
                validation_report["null_counts"][col] = int(
                    df[col].isna().sum()
                )

        return df, validation_report
