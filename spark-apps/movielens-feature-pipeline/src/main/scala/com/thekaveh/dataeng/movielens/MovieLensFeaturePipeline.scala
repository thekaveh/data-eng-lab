package com.thekaveh.dataeng.movielens

import org.apache.spark.sql.{DataFrame, SparkSession}

final case class RunResult(userRows: Long, movieRows: Long, provenance: Provenance)

trait TableWriter {
  def createNamespace(): Unit
  def replace(table: String, frame: DataFrame, provenance: Provenance): Unit
  def readFrame(table: String): DataFrame
  def readProperties(table: String): Map[String, String]
}

final class IcebergTableWriter(spark: SparkSession) extends TableWriter {
  def createNamespace(): Unit = spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

  def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = {
    var builder = frame.writeTo(table).using("iceberg")
    provenance.properties.toSeq.sortBy(_._1).foreach { case (key, value) =>
      builder = builder.tableProperty(key, value)
    }
    builder.createOrReplace()
  }

  def readFrame(table: String): DataFrame = spark.table(table)

  def readProperties(table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().map(row => row.getString(0) -> row.getString(1)).toMap
}

object MovieLensFeaturePipeline {
  val UserTable = "lakehouse.gold.ml_user_features"
  val MovieTable = "lakehouse.gold.ml_movie_features"

  private def sameRows(expected: DataFrame, actual: DataFrame): Boolean =
    expected.exceptAll(actual).limit(1).count() == 0 && actual.exceptAll(expected).limit(1).count() == 0

  private def verifyReadback(table: String, expectedFrame: DataFrame, expectedSchema: org.apache.spark.sql.types.StructType,
                             key: String, countColumn: String, sourceRows: Long, provenance: Provenance,
                             writer: TableWriter): Unit = {
    val actual = writer.readFrame(table)
    try {
      FeatureTransforms.validateFeatures(actual, expectedSchema, key, countColumn, sourceRows)
      if (!sameRows(expectedFrame, actual))
        throw new IllegalStateException(s"$table readback rows do not match the written features")
      val actualProperties = writer.readProperties(table).filter { case (name, _) => provenance.properties.contains(name) }
      if (actualProperties != provenance.properties)
        throw new IllegalStateException(s"$table provenance does not match the intended MovieLens generation")
    } catch {
      case failure: IllegalStateException => throw failure
      case failure: Exception => throw new IllegalStateException(s"$table readback validation failed", failure)
    }
  }

  def runResolved(sources: ResolvedSources, ratings: DataFrame, writer: TableWriter): RunResult = {
    FeatureTransforms.validateRatings(ratings)
    val sourceRows = ratings.count()
    val users = FeatureTransforms.userFeatures(ratings).cache()
    val movies = FeatureTransforms.movieFeatures(ratings).cache()
    try {
      val userRows = users.count()
      val movieRows = movies.count()
      FeatureTransforms.validateFeatures(users, FeatureTransforms.UserSchema, "userId", "num_ratings", sourceRows)
      FeatureTransforms.validateFeatures(movies, FeatureTransforms.MovieSchema, "movieId", "popularity", sourceRows)
      writer.createNamespace()
      writer.replace(UserTable, users, sources.provenance)
      writer.replace(MovieTable, movies, sources.provenance)
      verifyReadback(UserTable, users, FeatureTransforms.UserSchema, "userId", "num_ratings", sourceRows,
        sources.provenance, writer)
      verifyReadback(MovieTable, movies, FeatureTransforms.MovieSchema, "movieId", "popularity", sourceRows,
        sources.provenance, writer)
      val userProperties = writer.readProperties(UserTable).filter { case (name, _) => sources.provenance.properties.contains(name) }
      val movieProperties = writer.readProperties(MovieTable).filter { case (name, _) => sources.provenance.properties.contains(name) }
      if (userProperties != movieProperties)
        throw new IllegalStateException("MovieLens output tables do not bind the same generation")
      RunResult(userRows, movieRows, sources.provenance)
    } finally {
      users.unpersist()
      movies.unpersist()
    }
  }

  def main(args: Array[String]): Unit = {
    val sources = MovieLensSources.parse(args)
    val spark = SparkSession.builder().appName("movielens-feature-pipeline").getOrCreate()
    try {
      val ratings = spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .option("delimiter", ",")
        .schema(FeatureTransforms.RatingsSchema)
        .csv(sources.sparkUri("ratings.csv"))
      val result = runResolved(sources, ratings, new IcebergTableWriter(spark))
      // scalastyle:off println
      println(s"wrote $UserTable=${result.userRows}, $MovieTable=${result.movieRows}, " +
        s"publication=${result.provenance.publicationId}")
      // scalastyle:on println
    } finally spark.stop()
  }
}
