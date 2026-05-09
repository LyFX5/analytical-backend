import numpy as np
import pandas as pd


class SyntheticFleetGenerator:
    def __init__(self, n_entities=500, years=range(2018, 2025), seed=42):
        self.n_entities = n_entities
        self.years = list(years)
        self.rng = np.random.default_rng(seed)

        self.types = ["long-haul", "regional", "refrigerated", "tanker"]
        self.operators = [f"C-{i:03d}" for i in range(20)]

    def generate(self) -> pd.DataFrame:
        records = []

        for entity_id in range(self.n_entities):
            base_type = self.rng.choice(self.types)
            base_size = self.rng.normal(30, 8)
            base_operator = self.rng.choice(self.operators)

            start_age = self.rng.integers(0, 10)

            efficiency_factor = self.rng.normal(1.0, 0.1)

            for i, year in enumerate(self.years):
                age = start_age + i

                km = self.rng.normal(150_000, 30_000)

                fuel_intensity = (
                    0.3
                    + 0.02 * age
                    + 0.05 * (base_type == "refrigerated")
                    + self.rng.normal(0, 0.02)
                ) * efficiency_factor

                fuel = fuel_intensity * km / 1000
                co2 = fuel * 2.64

                records.append(
                    {
                        "entity_id": entity_id,
                        "year": year,
                        "operator_id": base_operator,
                        "type": base_type,
                        "size": base_size,
                        "age": age,
                        "km": km,
                        "fuel": fuel,
                        "co2": co2,
                    }
                )

        df = pd.DataFrame(records)
        return df
    
    def to_excel(self, filepath: str, **kwargs):
        """Export synthetic data to Excel file for testing ingestion."""
        # Pivot to create a more realistic Excel structure
        df_wide = self.generate()
        
        # Add entity_name column
        df_wide['entity_name'] = df_wide['entity_id'].apply(lambda x: f"Entity_{x:04d}")
        
        # Reorder columns to match expected EU MRV format
        columns_order = [
            'entity_name', 'operator_id', 'type', 'size', 'age',
            'year', 'fuel', 'co2', 'km'
        ]
        
        df_export = df_wide[columns_order].copy()
        
        # Export to Excel
        df_export.to_excel(filepath, index=False, **kwargs)
        return filepath


class DataLoader:
    def __init__(self, source="synthetic"):
        self.source = source

    def load(self) -> pd.DataFrame:
        if self.source == "synthetic":
            generator = SyntheticFleetGenerator()
            return generator.generate()
        else:
            raise NotImplementedError("Only synthetic for now")
