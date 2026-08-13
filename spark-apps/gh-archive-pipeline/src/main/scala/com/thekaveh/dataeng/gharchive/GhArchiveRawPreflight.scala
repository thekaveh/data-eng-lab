package com.thekaveh.dataeng.gharchive

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.{BufferedInputStream, ByteArrayInputStream, FilterInputStream, InputStream}
import java.security.{DigestInputStream, MessageDigest}
import java.time.Instant
import java.util.Arrays
import java.util.zip.GZIPInputStream

object GhArchiveRawPreflight {
  val MaxLineBytes = 1 << 20
  val MaxDepth = 32
  private val MaxRecords = 2000000L
  private val MaxExpandedBytes = 2L << 30
  private val TimestampRegex = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$".r
  private val JsonFactoryInstance = new JsonFactory()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
  private val Mapper = new ObjectMapper(JsonFactoryInstance)

  private final class CountingInputStream(delegate: InputStream, maximum: Long)
      extends FilterInputStream(delegate) {
    var count: Long = 0L
    override def read(): Int = {
      val value = super.read()
      if (value >= 0) increment(1L)
      value
    }
    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      val read = super.read(buffer, offset, length)
      if (read > 0) increment(read.toLong)
      read
    }
    private def increment(amount: Long): Unit = {
      count += amount
      if (count > maximum) throw new IllegalArgumentException("GitHub Archive source exceeds locked size")
    }
  }

  private def requireText(parent: JsonNode, name: String): String = {
    val value = parent.get(name)
    require(value != null && value.isTextual && value.textValue().trim.nonEmpty,
      s"GitHub Archive source requires JSON string $name")
    value.textValue()
  }

  private def depth(node: JsonNode): Int = {
    if (!node.isContainerNode) 0
    else {
      val children = node.elements()
      var maximum = 0
      while (children.hasNext) maximum = Math.max(maximum, depth(children.next()))
      1 + maximum
    }
  }

  private def validateLine(line: Array[Byte]): Unit = {
    val parser = JsonFactoryInstance.createParser(new ByteArrayInputStream(line))
    try {
      val document = Mapper.readTree[JsonNode](parser)
      require(document != null && document.isObject, "GitHub Archive source record must be one object")
      require(parser.nextToken() == null, "GitHub Archive source line contains trailing JSON")
      require(depth(document) <= MaxDepth, "GitHub Archive source record exceeds maximum depth")
      requireText(document, "id")
      requireText(document, "type")
      val actor = document.get("actor")
      require(actor != null && actor.isObject, "GitHub Archive source requires object actor")
      requireText(actor, "login")
      val repo = document.get("repo")
      require(repo != null && repo.isObject, "GitHub Archive source requires object repo")
      requireText(repo, "name")
      val createdAt = requireText(document, "created_at")
      require(TimestampRegex.pattern.matcher(createdAt).matches(),
        "GitHub Archive created_at must be exact whole-second UTC")
      require(Instant.parse(createdAt).toString == createdAt,
        "GitHub Archive created_at must be exact whole-second UTC")
    } finally parser.close()
  }

  private def nextLine(stream: InputStream): Array[Byte] = {
    val buffer = new Array[Byte](MaxLineBytes)
    var length = 0
    var value = stream.read()
    if (value < 0) return null
    while (value >= 0 && value != '\n') {
      if (length == MaxLineBytes)
        throw new IllegalArgumentException("GitHub Archive source contains an oversized JSON line")
      buffer(length) = value.toByte
      length += 1
      value = stream.read()
    }
    if (value < 0)
      throw new IllegalArgumentException("GitHub Archive source contains an unterminated JSON line")
    require(length > 0, "GitHub Archive source contains a blank JSON line")
    Arrays.copyOf(buffer, length)
  }

  def validateGzip(input: InputStream, lock: SourceLock): Long = {
    require(lock.sizeBytes > 0 && lock.sha256.matches("[0-9a-f]{64}"), "invalid GH Archive source lock")
    val digest = MessageDigest.getInstance("SHA-256")
    val counted = new CountingInputStream(input, lock.sizeBytes)
    val digested = new DigestInputStream(counted, digest)
    var buffered: BufferedInputStream = null
    try {
      val expanded = new CountingInputStream(new GZIPInputStream(digested), MaxExpandedBytes)
      buffered = new BufferedInputStream(expanded, 64 * 1024)
      var records = 0L
      var line = nextLine(buffered)
      while (line != null) {
        records += 1
        require(records <= MaxRecords, "GitHub Archive source exceeds maximum record count")
        validateLine(line)
        line = nextLine(buffered)
      }
      require(records > 0, "GitHub Archive source must be nonempty")
      while (digested.read() >= 0) {}
      val actualSha = digest.digest().map(value => f"${value & 0xff}%02x").mkString
      require(counted.count == lock.sizeBytes, "GitHub Archive source size does not match registry lock")
      require(actualSha == lock.sha256, "GitHub Archive source digest does not match registry lock")
      records
    } catch {
      case failure: IllegalArgumentException => throw failure
      case failure: Exception => throw new IllegalArgumentException("GitHub Archive raw source preflight failed", failure)
    } finally {
      if (buffered != null) buffered.close() else input.close()
    }
  }

  def validate(spark: SparkSession, sources: ResolvedSources): Long = {
    sources.sparkUris.zip(GhArchiveSources.ExpectedNames(sources.provenance.scale)).map {
      case (uri, name) =>
        val path = new Path(uri)
        val fileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val lock = GhArchiveSources.ExpectedLocks(name)
        require(fileSystem.getFileStatus(path).getLen == lock.sizeBytes,
          s"GitHub Archive source size does not match registry lock: $name")
        validateGzip(fileSystem.open(path), lock)
    }.sum
  }
}
