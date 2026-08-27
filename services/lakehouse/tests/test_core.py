from crush_lakehouse.core import assert_log_safe, event_partition, haversine_feature, stable_event_hash


def event() -> dict[str, object]:
    return {
        "event_id": "evt_12345678",
        "tenant_id": "demo-tenant",
        "program": "MEDICARE_FFS",
        "source_system": "synthetic",
        "source_message_id": "msg-1",
        "received_at": "2026-08-27T10:00:00Z",
        "raw_ref": "sha256:" + "a" * 64,
        "schema_version": "1.0.0",
    }


def test_event_partition_is_tenant_isolated() -> None:
    path = event_partition(event()).uri()
    assert path == "s3://crush/demo-tenant/bronze/claims/program=MEDICARE_FFS/received_date=2026-08-27"


def test_event_hash_is_repeatable_and_sensitive_to_content() -> None:
    original = event()
    assert stable_event_hash(original) == stable_event_hash(dict(original))
    changed = dict(original)
    changed["source_message_id"] = "msg-2"
    assert stable_event_hash(original) != stable_event_hash(changed)


def test_direct_identifiers_are_rejected_from_telemetry() -> None:
    try:
        assert_log_safe({"tenant_id": "demo", "name": "not-allowed"})
    except ValueError as error:
        assert "direct identifiers" in str(error)
    else:
        raise AssertionError("direct identifier was not rejected")


def test_haversine_geo_feature_has_quality_flag() -> None:
    feature = haversine_feature(38.9072, -77.0369, 40.7128, -74.0060, 12)
    assert 320 < feature.provider_distance_km < 340
    assert feature.geography_quality == "USABLE"
    assert feature.calculation_version == "haversine-v1"
