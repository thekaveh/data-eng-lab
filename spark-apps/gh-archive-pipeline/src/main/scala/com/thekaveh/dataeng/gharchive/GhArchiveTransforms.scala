package com.thekaveh.dataeng.gharchive

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.types._

object GhArchiveTransforms {
  private val TimestampPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  private val TimestampRegex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"

  val EventsSchema: StructType = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("type", StringType, nullable = false),
    StructField("actor_login", StringType, nullable = false),
    StructField("repo_name", StringType, nullable = false),
    StructField("created_at", TimestampType, nullable = false)
  ))

  val SessionsSchema: StructType = StructType(EventsSchema.fields ++ Seq(
    StructField("previous_created_at", TimestampType, nullable = true),
    StructField("new_session", IntegerType, nullable = false),
    StructField("session_id", LongType, nullable = false)
  ))

  private def requireNestedString(frame: DataFrame, parent: String, child: String): Unit = {
    require(frame.schema.fieldNames.contains(parent), s"GitHub Archive source is missing $parent.$child")
    frame.schema(parent).dataType match {
      case nested: StructType =>
        require(nested.fieldNames.contains(child) && nested(child).dataType == StringType,
          s"GitHub Archive source has invalid type for $parent.$child")
      case _ => throw new IllegalArgumentException(s"GitHub Archive source has invalid type for $parent.$child")
    }
  }

  private def requireRootString(frame: DataFrame, name: String): Unit = {
    require(frame.schema.fieldNames.contains(name), s"GitHub Archive source is missing $name")
    require(frame.schema(name).dataType == StringType, s"GitHub Archive source has invalid type for $name")
  }

  def validateNestedSource(frame: DataFrame): Unit = {
    Seq("id", "type", "created_at").foreach(requireRootString(frame, _))
    requireNestedString(frame, "actor", "login")
    requireNestedString(frame, "repo", "name")
  }

  private def requireNamesAndTypes(frame: DataFrame, expected: StructType, label: String): Unit = {
    val actual = frame.schema.fields.map(field => field.name -> field.dataType).toSeq
    val contract = expected.fields.map(field => field.name -> field.dataType).toSeq
    require(actual == contract, s"$label schema is not exact")
  }

  private def requireNonBlank(frame: DataFrame, names: Seq[String], label: String): Unit = {
    val invalid = names.map(name => F.col(name).isNull || F.length(F.trim(F.col(name))) === 0).reduce(_ || _)
    require(frame.where(invalid).limit(1).count() == 0, s"$label has null or blank required values")
  }

  private def requireNoConflictingIds(frame: DataFrame, label: String): Unit = {
    val flattenedValue = F.struct(
      F.col("type"), F.col("actor_login"), F.col("repo_name"), F.col("created_at")
    )
    require(frame.groupBy("id").agg(F.countDistinct(flattenedValue).as("versions"))
      .where(F.col("versions") > 1).limit(1).count() == 0,
      s"$label has conflicting records for the same event ID")
  }

  def validateEvents(frame: DataFrame): Unit = {
    requireNamesAndTypes(frame, EventsSchema, "gh_events")
    require(frame.limit(1).count() > 0, "gh_events must be nonempty")
    requireNonBlank(frame, Seq("id", "type", "actor_login", "repo_name"), "gh_events")
    require(frame.where(F.col("created_at").isNull).limit(1).count() == 0, "gh_events has null timestamps")
    requireNoConflictingIds(frame, "gh_events")
  }

  def flatten(source: DataFrame): DataFrame = {
    validateNestedSource(source)
    val projected = source.select(
      F.col("id"),
      F.col("type"),
      F.col("actor.login").as("actor_login"),
      F.col("repo.name").as("repo_name"),
      F.col("created_at").as("created_at_text")
    ).withColumn("created_at", F.try_to_timestamp(F.col("created_at_text"), F.lit(TimestampPattern)))
    val invalidTimestamp = !F.col("created_at_text").rlike(TimestampRegex) ||
      F.col("created_at").isNull ||
      F.date_format(F.col("created_at"), TimestampPattern) =!= F.col("created_at_text")
    require(projected.where(invalidTimestamp).limit(1).count() == 0,
      "GitHub Archive created_at must be exact whole-second UTC")
    val selected = projected.select(EventsSchema.fieldNames.map(F.col): _*)
    requireNonBlank(selected, Seq("id", "type", "actor_login", "repo_name"), "GitHub Archive source")
    require(selected.limit(1).count() > 0, "GitHub Archive source must be nonempty")
    requireNoConflictingIds(selected, "GitHub Archive source")
    val exact = source.sparkSession.createDataFrame(selected.rdd, EventsSchema)
    validateEvents(exact)
    exact
  }

  def sessionize(events: DataFrame): DataFrame = {
    validateEvents(events)
    val ordered = Window.partitionBy("actor_login").orderBy(F.col("created_at"), F.col("id"))
    val cumulative = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val calculated = events
      .withColumn("previous_created_at", F.lag(F.col("created_at"), 1).over(ordered))
      .withColumn("new_session", F.when(
        F.col("previous_created_at").isNull ||
          (F.unix_timestamp(F.col("created_at")) - F.unix_timestamp(F.col("previous_created_at"))) > 1800L,
        F.lit(1)
      ).otherwise(F.lit(0)).cast(IntegerType))
      .withColumn("session_id", F.sum(F.col("new_session")).over(cumulative).cast(LongType))
      .select(SessionsSchema.fieldNames.map(F.col): _*)
    val exact = events.sparkSession.createDataFrame(calculated.rdd, SessionsSchema)
    validateSessions(exact, events)
    exact
  }

  private def independentlyDerivedSessions(events: DataFrame): DataFrame = {
    val ordering = Window.partitionBy("actor_login").orderBy(F.col("created_at"), F.col("id"))
    val fromStart = ordering.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val withPrevious = events.withColumn(
      "previous_created_at",
      F.lag(F.col("created_at"), 1).over(ordering)
    )
    val withBoundary = withPrevious.withColumn(
      "new_session",
      F.when(F.col("previous_created_at").isNull, F.lit(1))
        .when(
          F.col("created_at").cast(LongType) - F.col("previous_created_at").cast(LongType) > 1800L,
          F.lit(1)
        ).otherwise(F.lit(0)).cast(IntegerType)
    )
    val derived = withBoundary
      .withColumn("session_id", F.sum(F.col("new_session")).over(fromStart).cast(LongType))
      .select(SessionsSchema.fieldNames.map(F.col): _*)
    events.sparkSession.createDataFrame(derived.rdd, SessionsSchema)
  }

  def validateSessions(frame: DataFrame, events: DataFrame): Unit = {
    requireNamesAndTypes(frame, SessionsSchema, "gh_sessions")
    validateEvents(events)
    val sourceRows = events.count()
    require(sourceRows > 0 && frame.count() == sourceRows, "gh_sessions must conserve event rows")
    requireNonBlank(frame, Seq("id", "type", "actor_login", "repo_name"), "gh_sessions")
    require(frame.where(F.col("created_at").isNull || F.col("new_session").isNull ||
      F.col("session_id").isNull).limit(1).count() == 0, "gh_sessions has null required values")
    require(frame.where(!F.col("new_session").isin(0, 1) || F.col("session_id") < 1L).limit(1).count() == 0,
      "gh_sessions has invalid session values")
    requireNoConflictingIds(frame, "gh_sessions")
    val eventProjection = frame.select(EventsSchema.fieldNames.map(F.col): _*)
    require(IcebergTables.sameRows(events, eventProjection), "gh_sessions event rows do not match gh_events")
    val expected = independentlyDerivedSessions(events)
    require(IcebergTables.sameRows(expected, frame),
      "gh_sessions annotations do not match the deterministic duplicate-aware session contract")
  }
}
