package com.thekaveh.dataeng.gharchive

import org.apache.spark.sql.{DataFrame, SparkSession}

final case class FlattenResult(eventRows: Long, provenance: Provenance)

object GhArchiveFlatten {
  val EventsTable = "lakehouse.silver.gh_events"

  def runResolved(sources: ResolvedSources, source: DataFrame, writer: TableWriter): FlattenResult = {
    val events = GhArchiveTransforms.flatten(source).cache()
    try {
      val eventRows = events.count()
      GhArchiveTransforms.validateEvents(events)
      writer.createNamespace()
      writer.replace(EventsTable, events, sources.provenance)
      val actual = writer.readFrame(EventsTable)
      try {
        GhArchiveTransforms.validateEvents(actual)
        if (actual.count() != eventRows || !IcebergTables.sameRows(events, actual))
          throw new IllegalStateException(s"$EventsTable readback rows do not match the flattened events")
      } catch {
        case failure: IllegalStateException => throw failure
        case failure: Exception => throw new IllegalStateException(s"$EventsTable readback validation failed", failure)
      }
      IcebergTables.requireProvenance(EventsTable, writer.readProperties(EventsTable), sources.provenance)
      FlattenResult(eventRows, sources.provenance)
    } finally events.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val sources = GhArchiveSources.parse(args)
    val spark = SparkSession.builder().appName("gh-archive-flatten")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .getOrCreate()
    try {
      val source = spark.read.option("mode", "FAILFAST").json(sources.sparkUris: _*)
      val result = runResolved(sources, source, new IcebergTableWriter(spark))
      // scalastyle:off println
      println(s"wrote $EventsTable=${result.eventRows}, publication=${result.provenance.publicationId}")
      // scalastyle:on println
    } finally spark.stop()
  }
}
