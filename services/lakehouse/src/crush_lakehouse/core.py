"""Portable CRUSH lakehouse utilities.

This module has no runtime-only imports, enabling contract and geospatial tests to
run without a Spark cluster. Spark, Flink, and Ray adapters call these deterministic
functions and retain every resulting provenance field.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from math import asin, cos, radians, sin, sqrt
from typing import Any, Mapping

RAW_PHI_KEYS = frozenset({"name", "ssn", "tin", "medical_record_number", "address", "phone", "email", "date_of_birth"})


@dataclass(frozen=True)
class LakehousePath:
    tenant_id: str
    zone: str
    dataset: str
    partition: str

    def uri(self, root: str = "s3://crush") -> str:
        if self.zone not in {"bronze", "silver", "gold", "audit", "ml"}:
            raise ValueError(f"unsupported zone: {self.zone}")
        if not self.tenant_id or not self.tenant_id.replace("-", "").isalnum():
            raise ValueError("tenant_id must be an alphanumeric-hyphen identifier")
        return f"{root.rstrip('/')}/{self.tenant_id}/{self.zone}/{self.dataset}/{self.partition}"


@dataclass(frozen=True)
class GeoFeature:
    provider_distance_km: float
    precision_m: float
    geography_quality: str
    calculation_version: str = "haversine-v1"

    def as_feature_payload(self) -> dict[str, Any]:
        return asdict(self)


def event_partition(event: Mapping[str, Any]) -> LakehousePath:
    """Derive a tenant-isolated Delta/Parquet path without accepting pooled PHI."""
    tenant_id = required_string(event, "tenant_id")
    received_at = required_string(event, "received_at")
    program = required_string(event, "program")
    date = datetime.fromisoformat(received_at.replace("Z", "+00:00")).date().isoformat()
    return LakehousePath(tenant_id, "bronze", "claims", f"program={program}/received_date={date}")


def stable_event_hash(event: Mapping[str, Any]) -> str:
    """Hash a normalized event for idempotency and raw-artifact provenance."""
    import json

    payload = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return f"sha256:{sha256(payload).hexdigest()}"


def assert_log_safe(payload: Mapping[str, Any]) -> None:
    """Reject direct identifiers from logs, traces, labels, and metric attributes."""
    exposed = RAW_PHI_KEYS.intersection(payload.keys())
    if exposed:
        raise ValueError(f"direct identifiers may not enter operational telemetry: {', '.join(sorted(exposed))}")


def haversine_feature(
    provider_lat: float,
    provider_lon: float,
    service_lat: float,
    service_lon: float,
    precision_m: float,
) -> GeoFeature:
    """Generate an explainable, advisory distance feature.

    A production Spark/Sedona job uses spatial indexes and geometry validity checks;
    this reference calculation provides a stable test oracle and documents how the
    online model feature must be bounded for low-precision locations.
    """
    for latitude, longitude in ((provider_lat, provider_lon), (service_lat, service_lon)):
        if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
            raise ValueError("coordinates are outside valid WGS84 bounds")
    if precision_m < 0:
        raise ValueError("precision_m must be non-negative")

    phi1, phi2 = radians(provider_lat), radians(service_lat)
    dphi = radians(service_lat - provider_lat)
    dlambda = radians(service_lon - provider_lon)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    distance_km = 2 * 6371.0088 * asin(sqrt(a))
    quality = "LOW_PRECISION" if precision_m > 5000 else "USABLE"
    return GeoFeature(round(distance_km, 6), precision_m, quality)


def required_string(event: Mapping[str, Any], field: str) -> str:
    value = event.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value
