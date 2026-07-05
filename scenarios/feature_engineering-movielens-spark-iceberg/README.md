# feature_engineering-movielens-spark-iceberg

Aggregates MovieLens ratings into user-level and movie-level feature marts for downstream machine learning recommendation models.

## 1. Overview

This scenario demonstrates ML feature engineering by reading raw MovieLens ratings from S3 and aggregating them into two feature tables. User-level features include average rating and total ratings per user. Movie-level features include average rating and popularity (total ratings count). These feature marts serve as input for collaborative filtering and other recommendation systems.

## 2. Why This Exists

Feature engineering for ML is a critical step between raw data ingestion and model training. This scenario shows how to efficiently aggregate large rating datasets into tabular feature stores that can be consumed by training pipelines, bridging the gap between lakehouse data and machine learning.

## 3. Architecture

```
s3a://landing/movielens/ratings.csv  →  Spark (batch)  →  lakehouse.gold.ml_user_features, lakehouse.gold.ml_movie_features
```

Key components:
- **Source:** MovieLens ratings CSV from S3
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.gold.ml_user_features`, `lakehouse.gold.ml_movie_features`
- **Orchestration:** `feature_engineering_movielens` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: `s3a://landing/movielens/ratings.csv` (header + inferred schema)

| Column | Type | Notes |
|--------|------|-------|
| `userId` | int | User identifier |
| `movieId` | int | Movie identifier |
| `rating` | double | Rating value |
| `timestamp` | int | Unix timestamp |

### 4.2 Output

- **Table:** `lakehouse.gold.ml_user_features`
- **Layer:** Gold
- **Key columns:** `userId`, `avg_rating`, `num_ratings`

- **Table:** `lakehouse.gold.ml_movie_features`
- **Layer:** Gold
- **Key columns:** `movieId`, `movie_avg_rating`, `popularity`

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; reads CSV from S3, aggregates ratings by user and by movie, writes two feature tables, and verifies aggregated output.
- **Jupyter (PySpark):** Sections Overview → Verify; same feature engineering logic using `groupBy().agg()` in PySpark with average, count aggregation functions.
- Both languages implement identical feature engineering logic with raw CSV ingestion, user and movie aggregation, and verification sections.

## 6. How to Run

1. Ensure the `gold` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Populate the MovieLens dataset: run `make datasets` to download ratings CSV to S3.
3. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger feature_engineering_movielens
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.ml_user_features LIMIT 10"
   spark-sql -e "SELECT * FROM lakehouse.gold.ml_movie_features LIMIT 10"
   ```

## 7. Dependencies

- **Dataset:** MovieLens ratings CSV from `s3a://landing/movielens/ratings.csv`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `gold` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- `make datasets` is required to populate the MovieLens landing zone before the notebook can read data.
- This scenario reads directly from S3 landing (no medallion intermediate layers).

## 9. See Also

- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
