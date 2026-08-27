import assert from "node:assert/strict";
import test from "node:test";
import { approvalEvent, CaseTask, initialState, targetStateFor, validateTransition, recommendedActionForSignal, UseCaseSignal } from "../src/casePolicy.js";

const task: CaseTask = {
  case_uid: "case_0123456789",
  decision_uid: "dec_0123456789",
  tenant_id: "demo-tenant",
  requested_action: "SUSPEND_RECOMMEND",
  reason_codes: ["RC-001-EXCLUDED-PROVIDER"],
  created_at: "2026-08-27T10:00:00.000Z",
  statutory_clock: { deadline_at: "2026-08-30T10:00:00.000Z", kind: "REVIEW" },
  evidence_refs: ["sha256:abc"],
};

test("final suspension requires an authenticated human approval", () => {
  const initial = initialState(task);
  assert.equal(initial, "UNDER_REVIEW");
  assert.equal(targetStateFor(task), "SUSPENDED");
  assert.throws(() => validateTransition(initial, "SUSPENDED"), /requires an authenticated human actor/);
});

test("top-five signals map to non-adverse human queues", () => {
  const signals: UseCaseSignal[] = [
    { use_case: "REVOKED_PROVIDER_MIGRATION", signal_id: "PROVIDER_BILLED_AFTER_REVOCATION_IN_MA", severity: "HIGH", abstained: false, reason: "post-revocation billing", evidence_refs: ["evt-1"] },
    { use_case: "LAB_REFERRAL_RING", signal_id: "HIGH_CONCENTRATION_LAB_REFERRALS", severity: "HIGH", abstained: false, reason: "concentration", evidence_refs: ["evt-2"] },
    { use_case: "DMEPOS_INTEGRITY", signal_id: "DELIVERY_NOT_CONFIRMED", severity: "MED", abstained: false, reason: "missing proof", evidence_refs: ["evt-3"] },
    { use_case: "CLAIMS_INTEGRITY", signal_id: "DUPLICATE_CLAIM_CANDIDATE", severity: "HIGH", abstained: false, reason: "duplicate", evidence_refs: ["evt-4"] },
    { use_case: "AI_CODING_OVERSIGHT", signal_id: "DATA_INSUFFICIENT", severity: "UNKNOWN", abstained: true, reason: "missing model version", evidence_refs: ["evt-5"] },
  ];
  assert.deepEqual(signals.map(recommendedActionForSignal), ["PEND_REVIEW", "PEND_REVIEW", "PEND_REVIEW", "PEND_REVIEW", "PREPAY_DOC_REQUEST"]);
});

test("approval event retains reviewer identity and rationale", () => {
  const event = approvalEvent(task, "UNDER_REVIEW", "SUSPENDED", {
    actor_id: "reviewer-100",
    rationale: "Exclusion verified against authoritative list.",
    approved_at: "2026-08-27T10:30:00.000Z",
  });
  assert.equal(event.to_state, "SUSPENDED");
  assert.equal(event.actor_id, "reviewer-100");
  assert.match(event.rationale ?? "", /authoritative list/);
});
