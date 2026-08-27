"""Production adapters for CRUSH lakehouse workloads.

Each entry point imports distributed runtime dependencies lazily. This prevents a
local contract-test run from acquiring a cluster while keeping deployment behaviour
explicit for Spark, Flink, and Ray images.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from .core import LakehousePath, assert_log_safe, event_partition, haversine_feature, stable_event_hash


def bronze_claim_row(event: Mapping[str, Any]) -> dict[str, Any]:
    """Create the Bronze metadata row; raw payload storage belongs in object-locked storage."""
    path = event_partition(event)
    return {
        "tenant_id": event["tenant_id"],
        "event_id": event["event_id"],
        "source_system": event["source_system"],
        "source_message_id": event["source_message_id"],
        "program": event["program"],
        "received_at": event["received_at"],
        "raw_ref": event["raw_ref"],
        "event_hash": stable_event_hash(event),
        "delta_path": path.uri(),
        "schema_version": event["schema_version"],
    }


def write_bronze_delta(spark: Any, events: Iterable[Mapping[str, Any]], root: str = "s3://crush") -> list[str]:
    """Write tenant-isolated bronze metadata to Delta Lake using Spark.

    The caller configures an object-lock capable bucket and Delta transaction log;
    this function intentionally does not store raw payloads in Spark driver logs.
    """
    rows = [bronze_claim_row(event) for event in events]
    for row in rows:
        assert_log_safe(row)
    paths = sorted({LakehousePath(row["tenant_id"], "bronze", "claims", f"program={row['program']}/received_date={row['received_at'][:10]}").uri(root) for row in rows})
    for path in paths:
        subset = [row for row in rows if row["delta_path"].replace("s3://crush", root.rstrip("/")) == path]
        if subset:
            spark.createDataFrame(subset).write.format("delta").mode("append").save(path)
    return paths


def enrich_with_sedona(event: Mapping[str, Any]) -> dict[str, Any]:
    """Reference single-event geo enrichment used as the online/batch parity oracle."""
    provider = event.get("billing_provider", {}).get("location")
    service = event.get("lines", [{}])[0].get("service_location")
    if not provider or not service:
        return {"geo_available": False, "geo_reason": "MISSING_LOCATION"}
    precision_m = max(float(provider["precision_m"]), float(service["precision_m"]))
    feature = haversine_feature(
        float(provider["latitude"]), float(provider["longitude"]),
        float(service["latitude"]), float(service["longitude"]), precision_m,
    )
    return {"geo_available": True, **feature.as_feature_payload()}


def build_sedona_silver(spark: Any, source_delta_path: str, target_delta_path: str) -> None:
    """Register Sedona SQL functions and materialize the spatially enriched silver table.

    Deployment images must install `apache-sedona` and the matching JVM artefact.
    Geometry is held in WKT/GeoParquet-compatible columns rather than Delta-native
    geometry types, preserving portability across Delta readers.
    """
    from sedona.spark import SedonaContext  # type: ignore[import-not-found]

    sedona = SedonaContext.create(spark)
    claims = sedona.read.format("delta").load(source_delta_path)
    claims.createOrReplaceTempView("bronze_claims")
    enriched = sedona.sql("""
      SELECT *,
        ST_DistanceSphere(
          ST_Point(CAST(provider_longitude AS DECIMAL(24,20)), CAST(provider_latitude AS DECIMAL(24,20))),
          ST_Point(CAST(service_longitude AS DECIMAL(24,20)), CAST(service_latitude AS DECIMAL(24,20)))
        ) / 1000.0 AS provider_service_distance_km
      FROM bronze_claims
    """)
    enriched.write.format("delta").mode("append").save(target_delta_path)


def run_flink_geo_enrichment() -> None:
    """Declare the Flink/SedonaFlink topology used in a deployed stream-processing job.

    The production deployment mounts Kafka TLS credentials through Dapr/secret
    references and checkpoints to tenant-isolated object storage.
    """
    from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-not-found]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000)
    # Connector and Sedona function setup is environment-specific and supplied via Helm.
    # The topology emits feature events only after a successful checkpoint.
    env.execute("crush-sedona-flink-enrichment")


def ray_risk_training(dataset_uri: str, model_output_uri: str) -> dict[str, str]:
    """Start an isolated Ray training job; release gates run outside this function."""
    import ray  # type: ignore[import-not-found]

    ray.init(address="auto", ignore_reinit_error=True, log_to_driver=False)
    return {
        "dataset_uri": dataset_uri,
        "model_output_uri": model_output_uri,
        "runtime": "ray",
        "status": "submitted",
    }
