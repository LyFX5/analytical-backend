import pandas as pd


class FeatureBuilder:
    def __init__(self):
        pass

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # bucketing
        df["size_bucket"] = pd.qcut(df["size"], q=4, labels=False)
        df["age_bucket"] = pd.cut(df["age"], bins=[-1, 3, 7, 12, 100], labels=False)

        # lag feature
        df = df.sort_values(["entity_id", "year"])
        df["co2_per_km_lag1"] = df.groupby("entity_id")["co2_per_km"].shift(1)

        return df
