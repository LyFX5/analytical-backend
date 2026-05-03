import pandas as pd
from sklearn.cluster import KMeans


class PeerClusterer:
    def __init__(self, n_clusters=6):
        self.n_clusters = n_clusters
        self.model = KMeans(n_clusters=n_clusters, random_state=42)

    def fit(self, df: pd.DataFrame):
        X = df[["size", "age"]].fillna(0)
        self.model.fit(X)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        X = df[["size", "age"]].fillna(0)
        df["cluster"] = self.model.predict(X)
        return df
