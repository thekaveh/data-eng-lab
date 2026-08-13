package com.thekaveh.dataeng.gharchive

import org.apache.spark.sql.SparkSession

final case class SessionResult(sessionRows: Long, provenance: Provenance)

object GhArchiveSessionization {
  val SessionsTable = "lakehouse.silver.gh_sessions"

  def runResolved(sources: ResolvedSources, writer: TableWriter): SessionResult = {
    IcebergTables.requireProvenance(
      GhArchiveFlatten.EventsTable,
      writer.readProperties(GhArchiveFlatten.EventsTable),
      sources.provenance
    )
    val events = writer.readFrame(GhArchiveFlatten.EventsTable).cache()
    try {
      GhArchiveTransforms.validateEvents(events)
      val eventRows = events.count()
      val sessions = GhArchiveTransforms.sessionize(events).cache()
      try {
        val sessionRows = sessions.count()
        GhArchiveTransforms.validateSessions(sessions, events)
        writer.createNamespace()
        writer.replace(SessionsTable, sessions, sources.provenance)
        val actual = writer.readFrame(SessionsTable)
        try {
          GhArchiveTransforms.validateSessions(actual, events)
          if (actual.count() != sessionRows || !IcebergTables.sameRows(sessions, actual))
            throw new IllegalStateException(s"$SessionsTable readback rows do not match the intended sessions")
        } catch {
          case failure: IllegalStateException => throw failure
          case failure: Exception => throw new IllegalStateException(s"$SessionsTable readback validation failed", failure)
        }
        IcebergTables.requireProvenance(SessionsTable, writer.readProperties(SessionsTable), sources.provenance)
        IcebergTables.requireProvenance(
          GhArchiveFlatten.EventsTable,
          writer.readProperties(GhArchiveFlatten.EventsTable),
          sources.provenance
        )
        SessionResult(sessionRows, sources.provenance)
      } finally sessions.unpersist()
    } finally events.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val sources = GhArchiveSources.parse(args)
    val spark = SparkSession.builder().appName("gh-archive-sessionization")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .getOrCreate()
    try {
      val result = runResolved(sources, new IcebergTableWriter(spark))
      // scalastyle:off println
      println(s"wrote $SessionsTable=${result.sessionRows}, publication=${result.provenance.publicationId}")
      // scalastyle:on println
    } finally spark.stop()
  }
}
