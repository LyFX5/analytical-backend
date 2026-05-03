from ingestion.data_loader import DataLoader
from preprocessing.preprocessing import Preprocessor
from features.feature_builder import FeatureBuilder

from analytics.clustering import PeerClusterer
from analytics.benchmarking import PercentileBenchmark
from analytics.ranking import Ranker


def run_pipeline():

    # 1. Load
    df = DataLoader().load()

    # 2. Preprocess
    df = Preprocessor().transform(df)

    # 3. Features
    df = FeatureBuilder().build(df)

    # 4. Clustering
    clusterer = PeerClusterer(n_clusters=6)
    clusterer.fit(df)
    df = clusterer.transform(df)

    # 5. Benchmarking
    benchmark = PercentileBenchmark(use_clustering=False)
    df = benchmark.compute(df)

    # 6. Ranking
    df = Ranker().rank(df)

    return df


if __name__ == "__main__":
    df = run_pipeline()
    print(df.head())
