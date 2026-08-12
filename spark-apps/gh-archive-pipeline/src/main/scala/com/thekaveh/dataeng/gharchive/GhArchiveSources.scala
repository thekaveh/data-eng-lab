package com.thekaveh.dataeng.gharchive

final case class Provenance(scale: String, planId: String, publicationId: String, manifestSha256: String) {
  val properties: Map[String, String] = Map(
    "data_eng_lab.dataset" -> "gh_archive",
    "data_eng_lab.dataset.scale" -> scale,
    "data_eng_lab.dataset.plan_id" -> planId,
    "data_eng_lab.dataset.publication_id" -> publicationId,
    "data_eng_lab.dataset.manifest_sha256" -> manifestSha256
  )
}

final case class ResolvedSources(canonicalUris: Seq[String], sparkUris: Seq[String], provenance: Provenance)

object GhArchiveSources {
  val ExpectedNames: Map[String, Vector[String]] = Map(
    "tiny" -> Vector("2023-01-01-0.json.gz"),
    "small" -> Vector("2023-01-01-0.json.gz", "2023-01-01-1.json.gz", "2023-01-01-2.json.gz"),
    "medium" -> Vector(
      "2023-01-01-0.json.gz", "2023-01-01-1.json.gz", "2023-01-01-2.json.gz",
      "2023-01-01-3.json.gz", "2023-01-01-4.json.gz", "2023-01-01-5.json.gz"
    )
  )
  private val Sha256 = "[0-9a-f]{64}"
  private val Uuid4 = "[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}"
  private val ImmutableUri = s"^s3://landing/gh_archive/_generations/($Sha256)/($Uuid4)/([A-Za-z0-9._-]+)$$".r
  private val MetadataNames = Seq("--dataset-scale", "--plan-id", "--publication-id", "--manifest-sha256")

  def parse(args: Array[String]): ResolvedSources = {
    require(args.length >= 9, "immutable GitHub Archive URIs and provenance are required")
    val canonical = args.dropRight(MetadataNames.size * 2).toSeq
    val metadataArgs = args.takeRight(MetadataNames.size * 2).grouped(2).toSeq
    require(metadataArgs.forall(_.length == 2) && metadataArgs.map(_.head) == MetadataNames,
      "provenance arguments must follow immutable GitHub Archive URIs")
    val metadata = metadataArgs.map(pair => pair(0) -> pair(1)).toMap
    val scale = metadata("--dataset-scale")
    val plan = metadata("--plan-id")
    val publication = metadata("--publication-id")
    val manifest = metadata("--manifest-sha256")
    require(ExpectedNames.contains(scale), "invalid GitHub Archive scale")
    require(plan.matches(Sha256) && manifest.matches(Sha256) && publication.matches(Uuid4),
      "invalid GitHub Archive provenance")
    val expectedNames = ExpectedNames(scale)
    require(canonical.size == expectedNames.size && canonical.distinct.size == canonical.size,
      "GitHub Archive immutable inventory is not exact")
    val parsed = canonical.map {
      case ImmutableUri(uriPlan, uriPublication, name) => (uriPlan, uriPublication, name)
      case _ => throw new IllegalArgumentException("verified immutable GitHub Archive URI arguments are required")
    }
    require(parsed.map(_._3) == expectedNames, "immutable GitHub Archive URIs must have the exact object order")
    require(parsed.map(item => (item._1, item._2)).distinct.size == 1,
      "immutable GitHub Archive URIs must share one generation")
    require(parsed.head._1 == plan && parsed.head._2 == publication,
      "GitHub Archive provenance does not match URI generation")
    ResolvedSources(
      canonical,
      canonical.map(value => "s3a://" + value.stripPrefix("s3://")),
      Provenance(scale, plan, publication, manifest)
    )
  }
}
