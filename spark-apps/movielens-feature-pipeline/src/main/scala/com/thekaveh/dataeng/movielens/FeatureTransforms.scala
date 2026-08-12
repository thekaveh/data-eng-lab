package com.thekaveh.dataeng.movielens

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types._

object FeatureTransforms {
  val RatingsSchema: StructType = StructType(Seq(
    StructField("userId", LongType, nullable = true),
    StructField("movieId", LongType, nullable = true),
    StructField("rating", DoubleType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  ))
  val UserSchema: StructType = StructType(Seq(
    StructField("userId", LongType, nullable = true),
    StructField("avg_rating", DoubleType, nullable = true),
    StructField("num_ratings", LongType, nullable = false)
  ))
  val MovieSchema: StructType = StructType(Seq(
    StructField("movieId", LongType, nullable = true),
    StructField("movie_avg", DoubleType, nullable = true),
    StructField("popularity", LongType, nullable = false)
  ))

  def validateRatings(frame: DataFrame): Unit = {
    require(frame.schema == RatingsSchema, "ratings must have the exact production schema")
    require(frame.limit(1).count() == 1, "ratings must be nonempty")
    require(frame.where(frame.columns.map(name => F.col(name).isNull).reduce(_ || _)).limit(1).count() == 0,
      "ratings must not contain nulls")
    require(frame.where(F.isnan(F.col("rating")) || F.col("rating") === Double.PositiveInfinity ||
      F.col("rating") === Double.NegativeInfinity).limit(1).count() == 0,
      "ratings must be finite")
  }

  def userFeatures(ratings: DataFrame): DataFrame =
    ratings.groupBy("userId")
      .agg(F.avg("rating").as("avg_rating"), F.count(F.lit(1)).as("num_ratings"))
      .select("userId", "avg_rating", "num_ratings")

  def movieFeatures(ratings: DataFrame): DataFrame =
    ratings.groupBy("movieId")
      .agg(F.avg("rating").as("movie_avg"), F.count(F.lit(1)).as("popularity"))
      .select("movieId", "movie_avg", "popularity")

  def validateFeatures(frame: DataFrame, expected: StructType, key: String, countColumn: String,
                       sourceRows: Long): Unit = {
    require(frame.schema == expected, s"$key features have the wrong schema")
    require(frame.limit(1).count() == 1, s"$key features must be nonempty")
    require(frame.where(frame.columns.map(name => F.col(name).isNull).reduce(_ || _)).limit(1).count() == 0,
      s"$key features must not contain nulls")
    require(frame.groupBy(key).count().where(F.col("count") =!= 1).limit(1).count() == 0,
      s"$key features must have one row per key")
    require(frame.where(F.isnan(F.col(frame.columns(1))) || F.col(frame.columns(1)) === Double.PositiveInfinity ||
      F.col(frame.columns(1)) === Double.NegativeInfinity || F.col(countColumn) <= 0).limit(1).count() == 0,
      s"$key features contain invalid measures")
    val total = frame.agg(F.sum(F.col(countColumn))).head().getLong(0)
    require(total == sourceRows, s"$key feature counts do not equal the source rating count")
  }
}
