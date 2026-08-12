package com.thekaveh.dataeng.movielens

final case class Provenance(scale: String, planId: String, publicationId: String, manifestSha256: String) {
  val properties: Map[String, String] = Map(
    "data_eng_lab.dataset" -> "movielens",
    "data_eng_lab.dataset.scale" -> scale,
    "data_eng_lab.dataset.plan_id" -> planId,
    "data_eng_lab.dataset.publication_id" -> publicationId,
    "data_eng_lab.dataset.manifest_sha256" -> manifestSha256
  )
}

final case class ResolvedSources(canonicalUris: Seq[String], sparkUris: Map[String, String], provenance: Provenance) {
  def sparkUri(objectName: String): String = sparkUris.getOrElse(
    objectName,
    throw new IllegalArgumentException(s"unknown MovieLens object: $objectName")
  )
}

object MovieLensSources {
  val LatestSmallObjects: Seq[(String, String)] = Seq(
    "links.csv" -> "movielens_latest_small_links",
    "tags.csv" -> "movielens_latest_small_tags",
    "ratings.csv" -> "movielens_latest_small_ratings",
    "README.txt" -> "movielens_latest_small_readme",
    "movies.csv" -> "movielens_latest_small_movies"
  )
  val Release25mObjects: Seq[(String, String)] = Seq(
    "tags.csv" -> "movielens_25m_tags",
    "links.csv" -> "movielens_25m_links",
    "README.txt" -> "movielens_25m_readme",
    "ratings.csv" -> "movielens_25m_ratings",
    "genome-tags.csv" -> "movielens_25m_genome_tags",
    "genome-scores.csv" -> "movielens_25m_genome_scores",
    "movies.csv" -> "movielens_25m_movies"
  )

  private val Sha256 = "[0-9a-f]{64}"
  private val Uuid4 = "[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}"
  private val ImmutableUri =
    s"^s3://landing/movielens/_generations/($Sha256)/($Uuid4)/([A-Za-z0-9._-]+)$$".r
  private val MetadataNames = Seq("--dataset-scale", "--plan-id", "--publication-id", "--manifest-sha256")

  private def expectedObjects(scale: String): Seq[(String, String)] = scale match {
    case "tiny" | "small" => LatestSmallObjects
    case "medium" => Release25mObjects
    case _ => throw new IllegalArgumentException("invalid MovieLens scale")
  }

  def parse(args: Array[String]): ResolvedSources = {
    val metadataStart = args.indexOf("--dataset-scale")
    require(metadataStart > 0, "immutable MovieLens URIs and provenance are required")
    val metadataArgs = args.drop(metadataStart).grouped(2).toSeq
    require(
      metadataArgs.size == MetadataNames.size && metadataArgs.forall(_.length == 2) &&
        metadataArgs.map(_.head) == MetadataNames,
      "provenance arguments must follow immutable MovieLens URIs"
    )
    val metadata = metadataArgs.map(pair => pair(0) -> pair(1)).toMap
    val scale = metadata("--dataset-scale")
    val expected = expectedObjects(scale).map(_._1)
    val canonical = args.take(metadataStart).toSeq
    require(canonical.size == expected.size, "complete immutable MovieLens publication is required")
    require(canonical.distinct.size == expected.size, "immutable MovieLens URIs must be unique")
    val parsed = canonical.map {
      case ImmutableUri(plan, publication, name) => (plan, publication, name)
      case _ => throw new IllegalArgumentException("verified immutable MovieLens URI arguments are required")
    }
    require(parsed.map(_._3) == expected, "immutable MovieLens URIs must have the exact scale-specific order")
    require(parsed.map(value => (value._1, value._2)).distinct.size == 1,
      "immutable MovieLens URIs must share one generation")

    val plan = metadata("--plan-id")
    val publication = metadata("--publication-id")
    val manifest = metadata("--manifest-sha256")
    require(plan.matches(Sha256) && publication.matches(Uuid4) && manifest.matches(Sha256),
      "invalid MovieLens provenance")
    require(parsed.head._1 == plan && parsed.head._2 == publication,
      "MovieLens provenance does not match URI generation")

    ResolvedSources(
      canonical,
      expected.zip(canonical.map(value => "s3a://" + value.stripPrefix("s3://"))).toMap,
      Provenance(scale, plan, publication, manifest)
    )
  }
}
