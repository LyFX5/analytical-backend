import pandas as pd


class PercentileBenchmark:
    def __init__(self, use_clustering=False):
        self.use_clustering = use_clustering

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if self.use_clustering:
            group_cols = ["cluster", "year"]
        else:
            group_cols = ["type", "size_bucket", "age_bucket", "year"]

        df["percentile_co2"] = df.groupby(group_cols)["co2_per_km"].rank(pct=True)

        df["zscore_co2"] = df.groupby(group_cols)["co2_per_km"].transform(
            lambda x: (x - x.mean()) / x.std(ddof=0)
        )

        return df
