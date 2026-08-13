package com.thekaveh.dataeng.gharchive

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.time.Instant
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.MessageDigest
import java.util.zip.GZIPOutputStream
import scala.collection.mutable

class GhArchivePipelineSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val plan = "1" * 64
  private val publication = "0123456789ab4def8123456789abcdef"
  private val manifest = "2" * 64
  private val names = Vector.tabulate(6)(hour => s"2023-01-01-$hour.json.gz")

  private def uri(name: String, p: String = plan, pub: String = publication): String =
    s"s3://landing/gh_archive/_generations/$p/$pub/$name"

  private def args(scale: String, selected: Seq[String], p: String = plan,
                   pub: String = publication, digest: String = manifest): Array[String] =
    (selected.map(uri(_)) ++ Seq(
      "--dataset-scale", scale,
      "--plan-id", p,
      "--publication-id", pub,
      "--manifest-sha256", digest
    )).toArray

  private val ActorSchema = StructType(Seq(StructField("login", StringType, nullable = true)))
  private val RepoSchema = StructType(Seq(StructField("name", StringType, nullable = true)))
  private val NestedSchema = StructType(Seq(
    StructField("id", StringType, nullable = true),
    StructField("type", StringType, nullable = true),
    StructField("actor", ActorSchema, nullable = true),
    StructField("repo", RepoSchema, nullable = true),
    StructField("created_at", StringType, nullable = true)
  ))

  private def nested(rows: Seq[(String, String, String, String, String)]): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map { case (id, kind, actor, repo, created) =>
      Row(id, kind, Row(actor), Row(repo), created)
    }), NestedSchema)

  private def eventRows(rows: Seq[(String, String, String, String, String)]): DataFrame =
    GhArchiveTransforms.flatten(nested(rows))

  private def gzip(text: String): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val compressed = new GZIPOutputStream(bytes)
    try compressed.write(text.getBytes("UTF-8")) finally compressed.close()
    bytes.toByteArray
  }

  private def sourceLock(bytes: Array[Byte]): SourceLock = SourceLock(
    bytes.length.toLong,
    MessageDigest.getInstance("SHA-256").digest(bytes).map(value => f"${value & 0xff}%02x").mkString
  )

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("gh-archive-pipeline-test").master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("parses each scale using the exact chronological immutable inventory") {
    val expected = Seq(
      "tiny" -> names.take(1),
      "small" -> names.take(3),
      "medium" -> names
    )
    expected.foreach { case (scale, objects) =>
      val parsed = GhArchiveSources.parse(args(scale, objects))
      assert(parsed.canonicalUris == objects.map(uri(_)))
      assert(parsed.sparkUris == objects.map(name => "s3a://" + uri(name).stripPrefix("s3://")))
      assert(parsed.provenance == Provenance(scale, plan, publication, manifest))
    }
  }

  test("rejects malformed duplicate missing extra reordered flat and cross-generation arguments") {
    val canonical = names.take(3).map(uri(_))
    val metadata = Seq("--dataset-scale", "small", "--plan-id", plan,
      "--publication-id", publication, "--manifest-sha256", manifest)
    val invalid = Seq(
      (canonical.dropRight(1) ++ metadata).toArray,
      ((canonical :+ canonical.head) ++ metadata).toArray,
      (canonical.reverse ++ metadata).toArray,
      ((canonical :+ uri(names(3))) ++ metadata).toArray,
      (canonical.updated(1, canonical(1).replace("/_generations/", "/")) ++ metadata).toArray,
      (canonical.updated(1, canonical(1).replace("s3://", "s3a://")) ++ metadata).toArray,
      (canonical.updated(1, canonical(1).replace(names(1), "deep/" + names(1))) ++ metadata).toArray,
      (canonical.updated(1, uri(names(1), pub = "0123456789ab4def9123456789abcdef")) ++ metadata).toArray,
      args("small", names.take(3), p = "3" * 64),
      args("small", names.take(3), pub = "not-a-uuid"),
      args("small", names.take(3), digest = "A" * 64),
      args("large", names.take(3))
    )
    invalid.foreach(value => assertThrows[IllegalArgumentException](GhArchiveSources.parse(value)))
  }

  test("flattens exact required nested strings and conserves rows") {
    val source = nested(Seq(
      ("2", "PushEvent", "alice", "acme/repo", "2023-01-01T00:00:01Z"),
      ("1", "PushEvent", "alice", "acme/repo", "2023-01-01T00:00:00Z")
    )).withColumn("ignored_payload", org.apache.spark.sql.functions.lit("allowed"))
    val flat = GhArchiveTransforms.flatten(source)
    assert(flat.schema == GhArchiveTransforms.EventsSchema)
    assert(flat.count() == source.count())
    assert(flat.orderBy("id").collect().map(_.getString(0)).toSeq == Seq("1", "2"))
  }

  test("rejects missing wrong null blank conflicting-ID and empty nested source values") {
    val valid = nested(Seq(("1", "PushEvent", "alice", "acme/repo", "2023-01-01T00:00:00Z")))
    val invalid = Seq(
      valid.drop("type"),
      valid.withColumn("id", org.apache.spark.sql.functions.col("id").cast(LongType)),
      nested(Seq((null, "PushEvent", "alice", "acme/repo", "2023-01-01T00:00:00Z"))),
      nested(Seq(("1", " ", "alice", "acme/repo", "2023-01-01T00:00:00Z"))),
      nested(Seq(("1", "PushEvent", null, "acme/repo", "2023-01-01T00:00:00Z"))),
      nested(Seq(("1", "PushEvent", "alice", "", "2023-01-01T00:00:00Z"))),
      nested(Seq(
        ("1", "PushEvent", "alice", "acme/repo", "2023-01-01T00:00:00Z"),
        ("1", "CreateEvent", "bob", "other/repo", "2023-01-01T00:00:01Z")
      )),
      nested(Seq.empty)
    )
    invalid.foreach(frame => assertThrows[IllegalArgumentException](GhArchiveTransforms.flatten(frame).count()))
  }

  test("accepts only exact whole-second UTC timestamps") {
    val accepted = eventRows(Seq(("1", "PushEvent", "alice", "acme/repo", "2023-01-01T23:59:59Z")))
    assert(accepted.head().getTimestamp(4) == Timestamp.from(Instant.parse("2023-01-01T23:59:59Z")))
    val rejected = Seq(
      "2023-01-01T00:00:00.000Z", "2023-01-01T00:00:00+00:00",
      "2023-01-01T01:00:00+01:00", "2023-01-01T00:00:00",
      "2023-01-01T00:00:00z", " 2023-01-01T00:00:00Z",
      "2023-01-01T00:00:00Z ", "2023-02-29T00:00:00Z",
      "2023-13-01T00:00:00Z", "2023-01-01T24:00:00Z"
    )
    rejected.zipWithIndex.foreach { case (value, index) =>
      assertThrows[IllegalArgumentException](eventRows(Seq(
        (index.toString, "PushEvent", "alice", "acme/repo", value)
      )).count())
    }
  }

  test("raw gzip preflight requires physical JSON strings and closes every source") {
    val valid = gzip(
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z","extra":{"allowed":true}}""" + "\n"
    )
    final class TrackedInput(bytes: Array[Byte]) extends ByteArrayInputStream(bytes) {
      var closed = false
      override def close(): Unit = { closed = true; super.close() }
    }
    val tracked = new TrackedInput(valid)
    assert(GhArchiveRawPreflight.validateGzip(tracked, sourceLock(valid)) == 1L)
    assert(tracked.closed)

    val invalid = Seq(
      """{"id":2,"type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":true,"actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":{"login":null},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":"alice","repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":{"login":false},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":7},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00.000Z"}""" + "\n",
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"} trailing""" + "\n",
      """[{"id":"1"}]""" + "\n"
    )
    invalid.foreach { text =>
      val bytes = gzip(text)
      assertThrows[IllegalArgumentException](
        GhArchiveRawPreflight.validateGzip(new ByteArrayInputStream(bytes), sourceLock(bytes)))
    }
  }

  test("raw gzip preflight rejects oversized unterminated and deeply nested records") {
    val prefix = """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z","extra":""""
    val oversized = gzip(prefix + ("x" * GhArchiveRawPreflight.MaxLineBytes) + "\"}\n")
    val unterminated = gzip(prefix + "value")
    val deep = gzip(
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z","extra":""" +
        ("[" * (GhArchiveRawPreflight.MaxDepth + 1)) + ("]" * (GhArchiveRawPreflight.MaxDepth + 1)) + "}\n"
    )
    Seq(oversized, unterminated, deep).foreach { bytes =>
      assertThrows[IllegalArgumentException](
        GhArchiveRawPreflight.validateGzip(new ByteArrayInputStream(bytes), sourceLock(bytes)))
    }
    val valid = gzip(
      """{"id":"1","type":"PushEvent","actor":{"login":"alice"},"repo":{"name":"a/r"},"created_at":"2023-01-01T00:00:00Z"}""" + "\n"
    )
    assertThrows[IllegalArgumentException](GhArchiveRawPreflight.validateGzip(
      new ByteArrayInputStream(valid), sourceLock(valid).copy(sizeBytes = valid.length + 1L)))
    assertThrows[IllegalArgumentException](GhArchiveRawPreflight.validateGzip(
      new ByteArrayInputStream(valid), sourceLock(valid).copy(sha256 = "0" * 64)))
    assertThrows[IllegalArgumentException](GhArchiveRawPreflight.validateGzip(
      new ByteArrayInputStream(Array[Byte](1, 2, 3)), SourceLock(3L, "0" * 64)))
  }

  test("sessionizes deterministically at the exact 1800 second boundary") {
    val events = eventRows(Seq(
      ("b", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("a", "CreateEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("c", "PushEvent", "alice", "a/r", "2023-01-01T00:30:00Z"),
      ("d", "PushEvent", "alice", "a/r", "2023-01-01T01:00:01Z"),
      ("e", "PushEvent", "bob", "b/r", "2023-01-01T04:00:00Z")
    ))
    val sessions = GhArchiveTransforms.sessionize(events)
    assert(sessions.schema == GhArchiveTransforms.SessionsSchema)
    val selected = sessions.orderBy("actor_login", "created_at", "id").collect().map { row =>
      (row.getString(0), Option(row.getTimestamp(5)), row.getInt(6), row.getLong(7))
    }.toSeq
    assert(selected.map { case (id, _, fresh, session) => (id, fresh, session) } == Seq(
      ("a", 1, 1L), ("b", 0, 1L), ("c", 0, 1L), ("d", 1, 2L), ("e", 1, 1L)
    ))
    assert(selected.head._2.isEmpty)
    assert(selected(1)._2.contains(Timestamp.from(Instant.parse("2023-01-01T00:00:00Z"))))
    assert(sessions.count() == events.count())
  }

  test("session results are repeatable under input reordering") {
    val rows = Seq(
      ("1", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("2", "PushEvent", "alice", "a/r", "2023-01-01T00:45:00Z"),
      ("3", "PushEvent", "bob", "b/r", "2023-01-01T00:00:00Z")
    )
    val forward = GhArchiveTransforms.sessionize(eventRows(rows)).orderBy("id").collect().toSeq
    val reverse = GhArchiveTransforms.sessionize(eventRows(rows.reverse)).orderBy("id").collect().toSeq
    assert(forward == reverse)
  }

  test("identical duplicate IDs remain distinct through flatten and sessionization") {
    val duplicated = Seq.fill(2)(
      ("same", "IssuesEvent", "github-actions[bot]", "Shopify/shopify_python_api",
        "2023-01-01T00:16:54Z")
    )
    val events = GhArchiveTransforms.flatten(nested(duplicated))
    val sessions = GhArchiveTransforms.sessionize(events)
    assert(events.count() == 2L)
    assert(sessions.count() == 2L)
    val annotations = sessions.select("previous_created_at", "new_session", "session_id")
      .collect().map(row => (Option(row.getTimestamp(0)), row.getInt(1), row.getLong(2))).toSeq
    assert(annotations.count(_._1.isEmpty) == 1)
    assert(annotations.map { case (_, fresh, session) => fresh -> session }.sorted ==
      Seq(0 -> 1L, 1 -> 1L))
  }

  test("conflicting records with the same ID fail before any table write") {
    val resolved = GhArchiveSources.parse(args("tiny", names.take(1)))
    val conflicting = nested(Seq(
      ("same", "IssuesEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("same", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z")
    ))
    val writer = new MemoryWriter
    assertThrows[IllegalArgumentException](GhArchiveFlatten.runResolved(resolved, conflicting, writer))
    assert(writer.calls.isEmpty)
  }

  test("duplicate-preserving event and session multisets are stable under shuffle") {
    val duplicated = Seq(
      ("same", "IssuesEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("same", "IssuesEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("later", "PushEvent", "alice", "a/r", "2023-01-01T00:45:00Z")
    )
    val forwardEvents = GhArchiveTransforms.flatten(nested(duplicated))
    val reverseEvents = GhArchiveTransforms.flatten(nested(duplicated.reverse))
    assert(IcebergTables.sameRows(forwardEvents, reverseEvents))
    assert(IcebergTables.sameRows(
      GhArchiveTransforms.sessionize(forwardEvents),
      GhArchiveTransforms.sessionize(reverseEvents)
    ))
  }

  test("session validation derives the exact duplicate-aware annotation multiset") {
    val events = eventRows(Seq(
      ("same", "IssuesEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("same", "IssuesEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("later", "PushEvent", "alice", "a/r", "2023-01-01T01:00:01Z")
    ))
    val valid = GhArchiveTransforms.sessionize(events)
    (1 to 20).foreach { partitions =>
      GhArchiveTransforms.validateSessions(valid.repartition((partitions % 4) + 1), events.repartition(3))
    }

    val wrongPredecessor = valid.withColumn(
      "previous_created_at",
      org.apache.spark.sql.functions.when(org.apache.spark.sql.functions.col("id") === "later",
        org.apache.spark.sql.functions.to_timestamp(org.apache.spark.sql.functions.lit("2023-01-01 00:30:00")))
        .otherwise(org.apache.spark.sql.functions.col("previous_created_at"))
    )
    val wrongFlag = valid.withColumn(
      "new_session",
      org.apache.spark.sql.functions.when(org.apache.spark.sql.functions.col("id") === "later", 0)
        .otherwise(org.apache.spark.sql.functions.col("new_session"))
    )
    val wrongId = valid.withColumn(
      "session_id",
      org.apache.spark.sql.functions.when(org.apache.spark.sql.functions.col("id") === "later", 3L)
        .otherwise(org.apache.spark.sql.functions.col("session_id"))
    )
    Seq(wrongPredecessor, wrongFlag, wrongId).foreach { invalid =>
      assertThrows[IllegalArgumentException](GhArchiveTransforms.validateSessions(invalid, events))
    }
  }

  test("flatten replaces the exact event table and verifies rows schema key and provenance") {
    val source = nested(Seq(
      ("1", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("2", "CreateEvent", "bob", "b/r", "2023-01-01T00:00:01Z")
    ))
    val resolved = GhArchiveSources.parse(args("tiny", names.take(1)))
    val writer = new MemoryWriter
    val result = GhArchiveFlatten.runResolved(resolved, source, writer)
    assert(result == FlattenResult(2L, resolved.provenance))
    assert(writer.calls == Seq("namespace", s"replace:${GhArchiveFlatten.EventsTable}",
      s"read:${GhArchiveFlatten.EventsTable}", s"properties:${GhArchiveFlatten.EventsTable}"))
    assert(writer.frames(GhArchiveFlatten.EventsTable).schema == GhArchiveTransforms.EventsSchema)
    assert(writer.properties(GhArchiveFlatten.EventsTable) == resolved.provenance.properties)
  }

  test("flatten preserves the primary failure and stops at every source write and readback boundary") {
    val resolved = GhArchiveSources.parse(args("tiny", names.take(1)))
    val good = nested(Seq(("1", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z")))
    val sourceFailure = good.withColumn(
      "id",
      org.apache.spark.sql.functions.raise_error(
        org.apache.spark.sql.functions.lit("injected source failure")
      ).cast(StringType)
    )
    val sourceWriter = new MemoryWriter
    assertThrows[Exception](GhArchiveFlatten.runResolved(resolved, sourceFailure, sourceWriter))
    assert(sourceWriter.calls.isEmpty)

    Seq(2, 3, 4).foreach { failOn =>
      val writer = new MemoryWriter
      writer.failOnCall = Some(failOn)
      assertThrows[RuntimeException](GhArchiveFlatten.runResolved(resolved, good, writer))
      assert(writer.calls.size == failOn)
      assert(!writer.calls.drop(failOn).exists(_.startsWith("properties:")))
    }
  }

  test("sessionization checks matching event properties before reading rows or writing") {
    val resolved = GhArchiveSources.parse(args("tiny", names.take(1)))
    val writer = new MemoryWriter
    writer.properties(GhArchiveFlatten.EventsTable) = resolved.provenance.properties.updated(
      "data_eng_lab.dataset.publication_id", "fedcba9876544abc8123456789abcdef")
    assertThrows[IllegalStateException](GhArchiveSessionization.runResolved(resolved, writer))
    assert(writer.calls == Seq(s"properties:${GhArchiveFlatten.EventsTable}"))

    writer.calls.clear()
    writer.properties(GhArchiveFlatten.EventsTable) = resolved.provenance.properties +
      ("data_eng_lab.dataset.unexpected" -> "forbidden")
    assertThrows[IllegalStateException](GhArchiveSessionization.runResolved(resolved, writer))
    assert(writer.calls == Seq(s"properties:${GhArchiveFlatten.EventsTable}"))
  }

  test("sessionization stops at every source property read write and readback boundary") {
    val resolved = GhArchiveSources.parse(args("tiny", names.take(1)))
    val events = eventRows(Seq(
      ("1", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("2", "PushEvent", "alice", "a/r", "2023-01-01T00:45:00Z")
    ))
    (1 to 7).foreach { failOn =>
      val writer = new MemoryWriter
      writer.frames(GhArchiveFlatten.EventsTable) = events
      writer.properties(GhArchiveFlatten.EventsTable) = resolved.provenance.properties
      writer.failOnCall = Some(failOn)
      assertThrows[RuntimeException](GhArchiveSessionization.runResolved(resolved, writer))
      assert(writer.calls.size == failOn)
    }
  }

  test("same-generation rerun converges after failure between flatten and session writes") {
    val resolved = GhArchiveSources.parse(args("tiny", names.take(1)))
    val source = nested(Seq(
      ("1", "PushEvent", "alice", "a/r", "2023-01-01T00:00:00Z"),
      ("2", "PushEvent", "alice", "a/r", "2023-01-01T00:45:00Z")
    ))
    val writer = new MemoryWriter
    GhArchiveFlatten.runResolved(resolved, source, writer)
    writer.failNextSessionWrite = true
    assertThrows[RuntimeException](GhArchiveSessionization.runResolved(resolved, writer))
    assert(writer.frames.keySet == Set(GhArchiveFlatten.EventsTable))

    GhArchiveFlatten.runResolved(resolved, source, writer)
    val result = GhArchiveSessionization.runResolved(resolved, writer)
    assert(result == SessionResult(2L, resolved.provenance))
    assert(writer.frames.keySet == Set(GhArchiveFlatten.EventsTable, GhArchiveSessionization.SessionsTable))
    assert(writer.properties.values.toSet == Set(resolved.provenance.properties))
    assert(writer.frames(GhArchiveSessionization.SessionsTable).orderBy("id").collect().map(_.getLong(7)).toSeq ==
      Seq(1L, 2L))
  }

  private final class MemoryWriter extends TableWriter {
    val calls = mutable.ArrayBuffer.empty[String]
    val frames = mutable.Map.empty[String, DataFrame]
    val properties = mutable.Map.empty[String, Map[String, String]]
    var failNextSessionWrite = false
    var failOnCall: Option[Int] = None

    private def record(call: String): Unit = {
      calls += call
      if (failOnCall.contains(calls.size)) throw new RuntimeException(s"injected failure at $call")
    }

    def createNamespace(): Unit = record("namespace")

    def replace(table: String, frame: DataFrame, provenance: Provenance): Unit = {
      record(s"replace:$table")
      if (table == GhArchiveSessionization.SessionsTable && failNextSessionWrite) {
        failNextSessionWrite = false
        throw new RuntimeException("injected session write failure")
      }
      frames(table) = spark.createDataFrame(frame.rdd, frame.schema)
      properties(table) = provenance.properties
    }

    def readFrame(table: String): DataFrame = {
      record(s"read:$table")
      frames.getOrElse(table, throw new IllegalStateException(s"missing table $table"))
    }

    def readProperties(table: String): Map[String, String] = {
      record(s"properties:$table")
      properties.getOrElse(table, Map.empty)
    }
  }
}
