# feature_engineering-movielens-spark-iceberg

Processes MovieLens dataset to create a feature store for machine learning, aggregating user and item features from rating history into Iceberg.

## 1. Purpose

This scenario demonstrates feature engineering for ML pipelines. It processes the MovieLens dataset to compute user-level features (average rating, total ratings, rating deviation), item-level features (average rating, total ratings, genre distributions), and user-item interaction counts. The output is a set of feature tables ready for ML model training, stored as Iceberg tables for efficient feature serving.

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/movielens/ratings.csv` and `s3a://landing/movielens/movies.csv` (downloaded via `make datasets`).

| Column | Type | Source |
|---|---|---|
| `UserId` | int | MovieLens ratings |
| `MovieId` | int | MovieLens ratings + movies |
| `Rating` | double | MovieLens ratings |
| `Timestamp` | int | MovieLens ratings |
| `title` | string | MovieLens movies |
| `genres` | string | MovieLens movies |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.user_features` | Silver | `UserId`, `avg_rating`, `total_ratings`, `rating_deviation` |
| `lakehouse.silver.item_features` | Silver | `MovieId`, `avg_rating`, `total_ratings`, `genres` |
| `lakehouse.silver.user_item_interactions` | Silver | `UserId`, `MovieId`, `rating`, `timestamp` |

## 3. Architecture

![Architecture](architectures/feature_engineering-movielens-spark-iceberg.html)

MovieLens ratings and movies data flows from S3 landing zone into Spark for feature engineering. User-level features (aggregated ratings, deviation from mean) and item-level features (average ratings, genre distributions) are computed and stored in separate Iceberg silver tables, along with raw user-item interactions for feature serving.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read MovieLens Data, User Feature Engineering, Item Feature Engineering, User-Item Interactions, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same feature engineering logic using PySpark with `groupBy` aggregations and `Window` operations

Both languages implement identical feature engineering with user aggregation, item aggregation, and user-item interaction tables.

## 5. Orchestration

Airflow DAG: `feature_engineering_movielens` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` and `gold` Iceberg namespaces exist: `scripts/register_iceberg.py`
2. Populate the landing zone: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger feature_engineering_movielens
     ```
4. Verify:
     ```bash
     spark-sql -e "SELECT * FROM lakehouse.silver.user_features LIMIT 10"
     spark-sql -e "SELECT * FROM lakehouse.silver.item_features LIMIT 10"
     ```

## 7. Dependencies

- **Dataset:** MovieLens ratings and movies CSVs from `s3a://landing/movielens/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. Both `silver` and `gold` namespaces must exist; run `scripts/register_iceberg.py` first. `make datasets` is required to populate the MovieLens landing zone before running the notebook.

## See Also

- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
