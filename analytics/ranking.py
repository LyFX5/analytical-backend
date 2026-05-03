import pandas as pd


class Ranker:
    def __init__(self):
        pass

    def rank(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df["rank_in_type"] = df.groupby(["type", "year"])["co2_per_km"].rank(
            method="min"
        )

        df["rank_global"] = df.groupby(["year"])["co2_per_km"].rank(method="min")

        return df
