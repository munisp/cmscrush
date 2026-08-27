export type RecommendedAction =
  | "PEND_REVIEW"
  | "PREPAY_DOC_REQUEST"
  | "DENY_RECOMMEND"
  | "SUSPEND_RECOMMEND"
  | "REFER";

export type TopFiveUseCase =
  | "REVOKED_PROVIDER_MIGRATION"
  | "LAB_REFERRAL_RING"
  | "DMEPOS_INTEGRITY"
  | "CLAIMS_INTEGRITY"
  | "AI_CODING_OVERSIGHT";

export type UseCaseSeverity = "LOW" | "MED" | "HIGH" | "UNKNOWN";

export interface UseCaseSignal {
  use_case: TopFiveUseCase;
  signal_id: string;
  severity: UseCaseSeverity;
  abstained: boolean;
  reason: string;
  evidence_refs: string[];
}

/** Map signals to human queues; this function never returns an autonomous adverse action. */
export function recommendedActionForSignal(signal: UseCaseSignal): RecommendedAction {
  if (signal.abstained || signal.severity === "UNKNOWN") return "PREPAY_DOC_REQUEST";
  if (signal.severity === "HIGH" || signal.severity === "MED") return "PEND_REVIEW";
  return "PREPAY_DOC_REQUEST";
}

export type CaseState =
  | "OPEN"
  | "UNDER_REVIEW"
  | "DOCS_REQUESTED"
  | "ADVERSE_ACTION_RECOMMENDED"
  | "DENIED"
  | "SUSPENDED"
  | "REFERRED"
  | "CLOSED"
  | "OVERTURNED";

export interface CaseTask {
  case_uid: string;
  decision_uid: string;
  tenant_id: string;
  requested_action: RecommendedAction;
  reason_codes: string[];
  created_at: string;
  statutory_clock: { deadline_at: string; kind: "REVIEW" | "DOC_RESPONSE" | "SUSPENSION_REVIEW" | "REBUTTAL" | "APPEAL" };
  evidence_refs: string[];
}

export interface HumanApproval {
  actor_id: string;
  rationale: string;
  approved_at: string;
}

export interface CaseEvent {
  case_uid: string;
  tenant_id: string;
  decision_uid: string;
  from_state: CaseState;
  to_state: CaseState;
  at: string;
  actor_id?: string;
  rationale?: string;
}

const terminalStates: ReadonlySet<CaseState> = new Set(["CLOSED", "OVERTURNED"]);

export function initialState(task: CaseTask): CaseState {
  if (task.requested_action === "PREPAY_DOC_REQUEST") return "DOCS_REQUESTED";
  if (task.requested_action === "REFER") return "REFERRED";
  return "UNDER_REVIEW";
}

export function targetStateFor(task: CaseTask): CaseState {
  switch (task.requested_action) {
    case "DENY_RECOMMEND": return "DENIED";
    case "SUSPEND_RECOMMEND": return "SUSPENDED";
    case "REFER": return "REFERRED";
    case "PREPAY_DOC_REQUEST": return "DOCS_REQUESTED";
    case "PEND_REVIEW": return "ADVERSE_ACTION_RECOMMENDED";
  }
}

export function isHumanApprovalRequired(state: CaseState): boolean {
  return state === "DENIED" || state === "SUSPENDED" || state === "ADVERSE_ACTION_RECOMMENDED";
}

export function validateTransition(
  current: CaseState,
  target: CaseState,
  approval?: HumanApproval,
): void {
  if (terminalStates.has(current)) {
    throw new Error(`terminal state ${current} cannot transition`);
  }
  if (current === target) {
    throw new Error("state transition must change state");
  }
  if (isHumanApprovalRequired(target)) {
    if (!approval?.actor_id || !approval.rationale || !approval.approved_at) {
      throw new Error(`transition to ${target} requires an authenticated human actor, rationale, and timestamp`);
    }
  }
}

export function approvalEvent(task: CaseTask, from: CaseState, to: CaseState, approval?: HumanApproval): CaseEvent {
  validateTransition(from, to, approval);
  return {
    case_uid: task.case_uid,
    tenant_id: task.tenant_id,
    decision_uid: task.decision_uid,
    from_state: from,
    to_state: to,
    at: approval?.approved_at ?? new Date().toISOString(),
    actor_id: approval?.actor_id,
    rationale: approval?.rationale,
  };
}
