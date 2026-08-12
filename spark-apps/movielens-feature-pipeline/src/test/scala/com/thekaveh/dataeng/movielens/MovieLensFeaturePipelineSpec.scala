package com.thekaveh.dataeng.movielens

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class MovieLensFeaturePipelineSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val plan = "1" * 64
  private val publication = "0123456789ab4def8123456789abcdef"
  private val manifest = "2" * 64
  private val smallObjects = Seq(
    "links.csv" -> "movielens_latest_small_links",
    "tags.csv" -> "movielens_latest_small_tags",
    "ratings.csv" -> "movielens_latest_small_ratings",
    "README.txt" -> "movielens_latest_small_readme",
    "movies.csv" -> "movielens_latest_small_movies"
  )
  private val mediumObjects = Seq(
    "tags.csv" -> "movielens_25m_tags",
    "links.csv" -> "movielens_25m_links",
    "README.txt" -> "movielens_25m_readme",
    "ratings.csv" -> "movielens_25m_ratings",
    "genome-tags.csv" -> "movielens_25m_genome_tags",
    "genome-scores.csv" -> "movielens_25m_genome_scores",
    "movies.csv" -> "movielens_25m_movies"
  )

  private def uri(name: String, p: String = plan, pub: String = publication): String =
    s"s3://landing/movielens/_generations/$p/$pub/$name"

  private def args(
      scale: String = "tiny",
      objects: Seq[(String, String)] = smallObjects,
      uris: Seq[String] = Seq.empty,
      p: String = plan,
      pub: String = publication,
      digest: String = manifest
  ): Array[String] = {
    val sourceUris = if (uris.nonEmpty) uris else objects.map { case (name, _) => uri(name) }
    (sourceUris ++ Seq(
      "--dataset-scale", scale,
      "--plan-id", p,
      "--publication-id", pub,
      "--manifest-sha256", digest
    )).toArray
  }

  private def ratings(rows: Seq[(Long, Long, Double, Long)]): DataFrame =
    ratingsRows(rows.map { case (u, m, r, t) => Row(u, m, r, t) })

  private def ratingsRows(rows: Seq[Row]): DataFrame = {
    val schema = StructType(Seq(
      StructField("userId", LongType, nullable = true),
      StructField("movieId", LongType, nullable = true),
      StructField("rating", DoubleType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    ))
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )
  }

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("movielens-feature-test").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("parses tiny and small using the exact latest-small registry order") {
    Seq("tiny", "small").foreach { scale =>
      val parsed = MovieLensSources.parse(args(scale = scale))
      assert(parsed.canonicalUris == smallObjects.map { case (name, _) => uri(name) })
      assert(parsed.sparkUri("ratings.csv") == uri("ratings.csv").replace("s3://", "s3a://"))
      assert(parsed.provenance == Provenance(scale, plan, publication, manifest))
    }
  }

  test("parses medium using the exact 25m registry order") {
    val parsed = MovieLensSources.parse(args(scale = "medium", objects = mediumObjects))
    assert(parsed.canonicalUris == mediumObjects.map { case (name, _) => uri(name) })
    assert(parsed.sparkUri("ratings.csv") == uri("ratings.csv").replace("s3://", "s3a://"))
    assert(parsed.provenance == Provenance("medium", plan, publication, manifest))
  }

  test("rejects missing extra duplicate reordered malformed and cross-generation inputs") {
    val canonical = smallObjects.map { case (name, _) => uri(name) }
    val invalid = Seq(
      args(uris = canonical.dropRight(1)),
      args(uris = canonical :+ canonical.head),
      args(uris = canonical.updated(4, canonical.head)),
      args(uris = canonical.reverse),
      args(uris = canonical.updated(2, canonical(2).replace("s3://landing", "s3a://landing"))),
      args(uris = canonical.updated(2, canonical(2).replace("/ratings.csv", "/deep/ratings.csv"))),
      args(uris = canonical.updated(2, uri("ratings.csv", pub = "0123456789ab4def9123456789abcdef"))),
      args(uris = canonical, p = "3" * 64),
      args(uris = canonical, pub = "not-a-uuid"),
      args(uris = canonical, digest = "A" * 64),
      args(scale = "large", uris = canonical)
    )
    invalid.foreach(value => assertThrows[IllegalArgumentException](MovieLensSources.parse(value)))
  }

  test("requires exact source schema and rejects null empty and nonfinite ratings") {
    val valid = ratings(Seq((1L, 10L, 4.0, 100L)))
    FeatureTransforms.validateRatings(valid)
    val invalid = Seq(
      valid.drop("timestamp"),
      valid.withColumn("rating", org.apache.spark.sql.functions.col("rating").cast(StringType)),
      valid.withColumn("extra", org.apache.spark.sql.functions.lit(1)),
      ratingsRows(Seq(Row(null, 10L, 4.0, 100L))),
      ratingsRows(Seq(Row(1L, null, 4.0, 100L))),
      ratingsRows(Seq(Row(1L, 10L, null, 100L))),
      ratingsRows(Seq(Row(1L, 10L, 4.0, null))),
      ratings(Seq.empty),
      ratings(Seq((1L, 10L, Double.NaN, 100L))),
      ratings(Seq((1L, 10L, Double.PositiveInfinity, 100L)))
    )
    invalid.foreach(frame => assertThrows[IllegalArgumentException](FeatureTransforms.validateRatings(frame)))
  }

  test("builds exact notebook features and counts duplicate rating events separately") {
    val source = ratings(Seq(
      (1L, 10L, 4.0, 100L),
      (1L, 10L, 4.0, 100L),
      (1L, 11L, 2.0, 101L),
      (2L, 10L, 5.0, 102L)
    ))
    FeatureTransforms.validateRatings(source)
    val users = FeatureTransforms.userFeatures(source)
    val movies = FeatureTransforms.movieFeatures(source)
    assert(users.schema == StructType(Seq(
      StructField("userId", LongType, nullable = true),
      StructField("avg_rating", DoubleType, nullable = true),
      StructField("num_ratings", LongType, nullable = false)
    )))
    assert(movies.schema == StructType(Seq(
      StructField("movieId", LongType, nullable = true),
      StructField("movie_avg", DoubleType, nullable = true),
      StructField("popularity", LongType, nullable = false)
    )))
    assert(users.orderBy("userId").collect().map(r => (r.getLong(0), r.getDouble(1), r.getLong(2))).toSeq ==
      Seq((1L, 10.0 / 3.0, 3L), (2L, 5.0, 1L)))
    assert(movies.orderBy("movieId").collect().map(r => (r.getLong(0), r.getDouble(1), r.getLong(2))).toSeq ==
      Seq((10L, 13.0 / 3.0, 3L), (11L, 2.0, 1L)))
    assert(users.agg(org.apache.spark.sql.functions.sum("num_ratings")).head().getLong(0) == 4L)
    assert(movies.agg(org.apache.spark.sql.functions.sum("popularity")).head().getLong(0) == 4L)
  }

  test("feature results are independent of source row order") {
    val sourceRows = Seq((1L, 10L, 1.0, 1L), (1L, 11L, 5.0, 2L), (2L, 10L, 3.0, 3L))
    val forward = FeatureTransforms.userFeatures(ratings(sourceRows)).orderBy("userId").collect().toSeq
    val reverse = FeatureTransforms.userFeatures(ratings(sourceRows.reverse)).orderBy("userId").collect().toSeq
    assert(forward == reverse)
  }

  test("readback accepts catalog-nullable metadata while enforcing non-null feature rows") {
    val catalogSchema = StructType(Seq(
      StructField("userId", LongType, nullable = true),
      StructField("avg_rating", DoubleType, nullable = true),
      StructField("num_ratings", LongType, nullable = true)
    ))
    val valid = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1L, 4.0, 1L))),
      catalogSchema
    )
    FeatureTransforms.validateFeatures(
      valid, FeatureTransforms.UserSchema, "userId", "num_ratings", sourceRows = 1L
    )
    val invalid = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1L, null, 1L))),
      catalogSchema
    )
    assertThrows[IllegalArgumentException](FeatureTransforms.validateFeatures(
      invalid, FeatureTransforms.UserSchema, "userId", "num_ratings", sourceRows = 1L
    ))
  }

  test("writes user before movie verifies readback and converges after a partial failure") {
    val source = ratings(Seq((1L, 10L, 4.0, 100L), (2L, 10L, 2.0, 101L)))
    val resolved = MovieLensSources.parse(args())

    final class RecordingWriter(failMovieOnce: Boolean) extends TableWriter {
      val calls = mutable.ArrayBuffer.empty[String]
      val frames = mutable.Map.empty[String, Seq[Row]]
      val schemas = mutable.Map.empty[String, StructType]
      val properties = mutable.Map.empty[String, Map[String, String]]
      private var fail = failMovieOnce

      def createNamespace(): Unit = calls += "namespace"
      def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = {
        calls += table
        if (table == MovieLensFeaturePipeline.MovieTable && fail) {
          fail = false
          throw new RuntimeException("injected")
        }
        frames(table) = frame.orderBy(frame.columns.head).collect().toSeq
        schemas(table) = frame.schema
        properties(table) = provenance.properties
      }
      def readFrame(table: String): DataFrame = {
        spark.createDataFrame(spark.sparkContext.parallelize(frames(table)), schemas(table))
      }
      def readProperties(table: String): Map[String, String] = properties.getOrElse(table, Map.empty)
    }

    val writer = new RecordingWriter(failMovieOnce = true)
    assertThrows[RuntimeException](MovieLensFeaturePipeline.runResolved(resolved, source, writer))
    assert(writer.calls == Seq("namespace", MovieLensFeaturePipeline.UserTable, MovieLensFeaturePipeline.MovieTable))
    assert(writer.properties.keySet == Set(MovieLensFeaturePipeline.UserTable))
    val result = MovieLensFeaturePipeline.runResolved(resolved, source, writer)
    assert(result == RunResult(2L, 1L, resolved.provenance))
    assert(writer.properties(MovieLensFeaturePipeline.UserTable) == resolved.provenance.properties)
    assert(writer.properties(MovieLensFeaturePipeline.MovieTable) == resolved.provenance.properties)
  }

  test("first-write failure prevents the second write and source failure performs no writes") {
    val source = ratings(Seq((1L, 10L, 4.0, 100L)))
    val resolved = MovieLensSources.parse(args())
    val calls = mutable.ArrayBuffer.empty[String]
    val writer = new TableWriter {
      def createNamespace(): Unit = calls += "namespace"
      def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = {
        calls += table
        if (table == MovieLensFeaturePipeline.UserTable) throw new RuntimeException("first write failed")
      }
      def readFrame(table: String): DataFrame = throw new IllegalStateException("not reached")
      def readProperties(table: String): Map[String, String] = Map.empty
    }
    assertThrows[RuntimeException](MovieLensFeaturePipeline.runResolved(resolved, source, writer))
    assert(calls == Seq("namespace", MovieLensFeaturePipeline.UserTable))

    calls.clear()
    val broken = source.withColumn("rating", org.apache.spark.sql.functions.expr(
      "cast(raise_error('source failure') as double)"))
    assertThrows[Exception](MovieLensFeaturePipeline.runResolved(resolved, broken, writer))
    assert(calls.isEmpty)
  }

  test("fails closed when output schema key rows counts or provenance readback differ") {
    val source = ratings(Seq((1L, 10L, 4.0, 100L)))
    val resolved = MovieLensSources.parse(args())
    val stored = mutable.Map.empty[String, DataFrame]
    val writer = new TableWriter {
      def createNamespace(): Unit = ()
      def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = stored(table) = frame
      def readFrame(table: String): DataFrame = stored(table).withColumn(
        if (table == MovieLensFeaturePipeline.UserTable) "userId" else "movieId",
        org.apache.spark.sql.functions.lit(null).cast(LongType)
      )
      def readProperties(table: String): Map[String, String] = Map.empty
    }
    assertThrows[IllegalStateException](MovieLensFeaturePipeline.runResolved(resolved, source, writer))
  }
}
