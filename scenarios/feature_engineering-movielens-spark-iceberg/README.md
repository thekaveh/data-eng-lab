# feature_engineering-movielens-spark-iceberg

Engineer ML-ready features from MovieLens ratings data. Aggregates user-level and movie-level
statistics (average ratings, popularity counts) from the raw ratings CSV, creating feature marts
that bridge to ml-lab model training pipelines. Demonstrates how Spark handles ML feature preparation
at scale.

## 1. Scenario summary
Aggregate MovieLens ratings by userId (avg_rating, num_ratings) and movieId (movie_avg, popularity);
write both feature marts to Iceberg tables. Reads from S3 CSV, writes two feature tables to
`lakehouse.gold.{ml_user_features,ml_movie_features}`.

## 2. Why this exists
Teaches feature engineering patterns on top of the medallion architecture. The user and movie feature
marts are canonical inputs to collaborative filtering and content-based recommendation models in ml-lab.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Scala) and `jupyter/notebook.ipynb` (PySpark), sections Overview->Verify;
a `dag.py` placeholder.

## 4. How to run
Open either notebook on the Atlas stack (after ensuring MovieLens datasets are available),
or trigger the `feature_engineering_movielens` Airflow DAG.

## 5. Data & dependencies
Requires `s3a://landing/movielens/ratings.csv` (header + inferSchema);
Spark + Iceberg `lakehouse` catalog (Atlas A1-A4).

## 6. Known issues & caveats
Notebook execution + Scala/PySpark parity are live-gated on Atlas A1-A4. This scenario writes to
`lakehouse.gold.{ml_user_features,ml_movie_features}`, which require the `gold` namespace to exist in the
Iceberg REST catalog. Run `scripts/register_iceberg.py` (creates `bronze`, `silver`, and `gold`) and
`make datasets` before executing this scenario standalone.
