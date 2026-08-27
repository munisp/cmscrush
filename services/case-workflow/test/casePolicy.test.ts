import assert from "node:assert/strict";
import test from "node:test";
import { approvalEvent, CaseTask, initialState, targetStateFor, validateTransition } from "../src/casePolicy.js";

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
