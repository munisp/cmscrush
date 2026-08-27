"""Deterministic business-use-case signals for CRUSH pilot workflows.

These signals prioritize evidence collection and human review. They do not make
payment, enrollment, revocation, suspension, or denial decisions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class UseCaseSignal:
    use_case: str
    signal_id: str
    severity: str
    abstained: bool
    reason: str
    evidence_refs: tuple[str, ...] = ()


def _signal(use_case: str, signal_id: str, severity: str, reason: str, event: Mapping[str, Any], *refs: str) -> UseCaseSignal:
    return UseCaseSignal(use_case, signal_id, severity, False, reason, tuple(refs or (str(event.get("event_id", "unknown")),)))


def _abstain(use_case: str, reason: str, event: Mapping[str, Any]) -> UseCaseSignal:
    return UseCaseSignal(use_case, "DATA_INSUFFICIENT", "UNKNOWN", True, reason, (str(event.get("event_id", "unknown")),))


def revoked_provider_migration(event: Mapping[str, Any]) -> UseCaseSignal:
    provider = event.get("provider", {})
    if not provider.get("provider_id") or not provider.get("revocation_effective_date") or not event.get("service_date"):
        return _abstain("REVOKED_PROVIDER_MIGRATION", "provider identity or effective date is missing", event)
    if provider.get("revoked") is True and event["service_date"] >= provider["revocation_effective_date"] and event.get("program") in {"MEDICARE_ADVANTAGE", "PART_D"}:
        return _signal("REVOKED_PROVIDER_MIGRATION", "PROVIDER_BILLED_AFTER_REVOCATION_IN_MA", "HIGH", "billing occurs on or after revocation effective date in an MA/Part D context", event, provider["provider_id"])
    return _signal("REVOKED_PROVIDER_MIGRATION", "NO_MIGRATION_SIGNAL", "LOW", "no post-revocation MA/Part D billing signal", event, provider["provider_id"])


def laboratory_referral_ring(event: Mapping[str, Any]) -> UseCaseSignal:
    required = (event.get("ordering_provider_id"), event.get("laboratory_id"), event.get("beneficiary_token"))
    if any(value in (None, "") for value in required):
        return _abstain("LAB_REFERRAL_RING", "ordering provider, laboratory, or beneficiary token is missing", event)
    beneficiaries = int(event.get("laboratory_beneficiary_count_24h", 0))
    ordering_share = float(event.get("top_ordering_provider_share_24h", 0.0))
    if beneficiaries >= 50 and ordering_share >= 0.80:
        return _signal("LAB_REFERRAL_RING", "HIGH_CONCENTRATION_LAB_REFERRALS", "HIGH", "high beneficiary volume and ordering-provider concentration require review", event, str(event["laboratory_id"]), str(event["ordering_provider_id"]))
    return _signal("LAB_REFERRAL_RING", "NO_RING_THRESHOLD", "LOW", "referral concentration is below pilot threshold", event)


def dmepos_integrity(event: Mapping[str, Any]) -> UseCaseSignal:
    if not event.get("supplier_id") or not event.get("item_code"):
        return _abstain("DMEPOS_INTEGRITY", "supplier or item identity is missing", event)
    order_date = event.get("order_date")
    delivery_date = event.get("delivery_date")
    if order_date and delivery_date and delivery_date < order_date:
        return _signal("DMEPOS_INTEGRITY", "DELIVERY_BEFORE_ORDER", "HIGH", "delivery date precedes order date", event, str(event["supplier_id"]))
    if event.get("delivery_confirmed") is False:
        return _signal("DMEPOS_INTEGRITY", "DELIVERY_NOT_CONFIRMED", "MED", "claim lacks delivery confirmation", event, str(event["supplier_id"]))
    return _signal("DMEPOS_INTEGRITY", "NO_DMEPOS_EXCEPTION", "LOW", "no deterministic DMEPOS exception found", event)


def claims_integrity(event: Mapping[str, Any]) -> UseCaseSignal:
    if not event.get("claim_id") or not event.get("provider_id"):
        return _abstain("CLAIMS_INTEGRITY", "claim or provider identity is missing", event)
    if event.get("duplicate_candidate") is True:
        return _signal("CLAIMS_INTEGRITY", "DUPLICATE_CLAIM_CANDIDATE", "HIGH", "claim matches a duplicate candidate set", event, str(event["claim_id"]))
    if event.get("impossible_travel") is True:
        return _signal("CLAIMS_INTEGRITY", "IMPOSSIBLE_TRAVEL", "MED", "provider/service geography requires review", event, str(event["provider_id"]))
    return _signal("CLAIMS_INTEGRITY", "NO_DETERMINISTIC_EXCEPTION", "LOW", "no deterministic claims exception found", event)


def coding_oversight(event: Mapping[str, Any]) -> UseCaseSignal:
    if not event.get("provider_id") or not event.get("coding_model_version"):
        return _abstain("AI_CODING_OVERSIGHT", "provider or coding-model version is missing", event)
    if event.get("coding_model_version_changed") is True and float(event.get("acuity_shift_z", 0.0)) >= 3.0:
        return _signal("AI_CODING_OVERSIGHT", "MODEL_CHANGE_ACUITY_SHIFT", "MED", "coding-model change coincides with an unusual acuity distribution shift", event, str(event["provider_id"]), str(event["coding_model_version"]))
    return _signal("AI_CODING_OVERSIGHT", "NO_CODING_DRIFT_SIGNAL", "LOW", "no governed coding drift signal found", event)


def evaluate_top_five(event: Mapping[str, Any]) -> tuple[UseCaseSignal, ...]:
    return (revoked_provider_migration(event), laboratory_referral_ring(event), dmepos_integrity(event), claims_integrity(event), coding_oversight(event))
