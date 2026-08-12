package com.thekaveh.dataeng.tpch

final case class Provenance(scale: String, planId: String, publicationId: String, manifestSha256: String) {
  val properties: Map[String, String] = Map(
    "data_eng_lab.dataset" -> "tpch",
    "data_eng_lab.dataset.scale" -> scale,
    "data_eng_lab.dataset.plan_id" -> planId,
    "data_eng_lab.dataset.publication_id" -> publicationId,
    "data_eng_lab.dataset.manifest_sha256" -> manifestSha256
  )
}

final case class ResolvedSources(canonicalUris: Seq[String], sparkUris: Map[String, String], provenance: Provenance) {
  def sparkUri(objectName: String): String = sparkUris.getOrElse(objectName,
    throw new IllegalArgumentException(s"unknown TPC-H object: $objectName"))
}

object TpchSources {
  val ObjectNames: Seq[String] = Seq("customer.parquet", "lineitem.parquet", "nation.parquet", "orders.parquet",
    "part.parquet", "partsupp.parquet", "region.parquet", "supplier.parquet")
  private val Sha256 = "[0-9a-f]{64}"
  private val Uuid4 = "[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}"
  private val ImmutableUri = s"^s3://landing/tpch/_generations/($Sha256)/($Uuid4)/([A-Za-z0-9._-]+)$$".r
  private val MetadataNames = Seq("--dataset-scale", "--plan-id", "--publication-id", "--manifest-sha256")

  def parse(args: Array[String]): ResolvedSources = {
    require(args.length == ObjectNames.size + MetadataNames.size * 2, "eight immutable TPC-H URIs and provenance are required")
    val canonical = args.take(ObjectNames.size).toSeq
    require(canonical.distinct.size == ObjectNames.size, "immutable TPC-H URIs must be unique")
    val parsed = canonical.map {
      case ImmutableUri(plan, publication, name) => (plan, publication, name)
      case _ => throw new IllegalArgumentException("verified immutable TPC-H URI arguments are required")
    }
    require(parsed.map(_._3) == ObjectNames, "immutable TPC-H URIs must have the exact object order")
    require(parsed.map(p => (p._1, p._2)).distinct.size == 1, "immutable TPC-H URIs must share one generation")
    val metadataArgs = args.drop(ObjectNames.size).grouped(2).toSeq
    require(metadataArgs.forall(_.length == 2) && metadataArgs.map(_.head) == MetadataNames,
      "provenance arguments must follow immutable TPC-H URIs")
    val metadata = metadataArgs.map(pair => pair(0) -> pair(1)).toMap
    val scale = metadata("--dataset-scale")
    val plan = metadata("--plan-id")
    val publication = metadata("--publication-id")
    val manifest = metadata("--manifest-sha256")
    require(Set("tiny", "small", "medium").contains(scale), "invalid TPC-H scale")
    require(plan.matches(Sha256) && manifest.matches(Sha256) && publication.matches(Uuid4), "invalid TPC-H provenance")
    require(parsed.head._1 == plan && parsed.head._2 == publication, "TPC-H provenance does not match URI generation")
    ResolvedSources(canonical, ObjectNames.zip(canonical.map(uri => "s3a://" + uri.stripPrefix("s3://"))).toMap,
      Provenance(scale, plan, publication, manifest))
  }
}
