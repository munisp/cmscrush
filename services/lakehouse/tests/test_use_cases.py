from crush_lakehouse.use_cases import evaluate_top_five, laboratory_referral_ring, revoked_provider_migration


def test_revoked_provider_migration_signal() -> None:
    signal = revoked_provider_migration({
        "event_id": "claim-1",
        "program": "MEDICARE_ADVANTAGE",
        "service_date": "2026-08-27",
        "provider": {"provider_id": "p-1", "revoked": True, "revocation_effective_date": "2026-08-01"},
    })
    assert signal.signal_id == "PROVIDER_BILLED_AFTER_REVOCATION_IN_MA"
    assert signal.severity == "HIGH"


def test_laboratory_ring_and_incomplete_data() -> None:
    signal = laboratory_referral_ring({
        "event_id": "lab-1",
        "ordering_provider_id": "op-1",
        "laboratory_id": "lab-1",
        "beneficiary_token": "b-1",
        "laboratory_beneficiary_count_24h": 75,
        "top_ordering_provider_share_24h": 0.90,
    })
    assert signal.signal_id == "HIGH_CONCENTRATION_LAB_REFERRALS"
    assert laboratory_referral_ring({"event_id": "lab-2"}).abstained


def test_top_five_returns_one_non_adverse_signal_per_workflow() -> None:
    signals = evaluate_top_five({"event_id": "event-1"})
    assert len(signals) == 5
    assert {signal.use_case for signal in signals} == {
        "REVOKED_PROVIDER_MIGRATION", "LAB_REFERRAL_RING", "DMEPOS_INTEGRITY",
        "CLAIMS_INTEGRITY", "AI_CODING_OVERSIGHT",
    }
    assert all(signal.signal_id != "DENY" for signal in signals)
