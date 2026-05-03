import pandas as pd


class Preprocessor:
    def __init__(self):
        pass

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # sanity filters
        df = df[df["km"] > 0]
        df = df[df["fuel"] > 0]

        # derived metrics
        df["fuel_per_km"] = df["fuel"] / df["km"]
        df["co2_per_km"] = df["co2"] / df["km"]

        return df
