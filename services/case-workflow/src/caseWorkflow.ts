import { condition, defineSignal, setHandler, sleep } from "@temporalio/workflow";
import {
  approvalEvent,
  CaseEvent,
  CaseState,
  CaseTask,
  HumanApproval,
  initialState,
  targetStateFor,
} from "./casePolicy.js";

export const submitHumanApproval = defineSignal<[HumanApproval]>("submitHumanApproval");

export interface CaseWorkflowResult {
  final_state: CaseState | "ESCALATED";
  events: CaseEvent[];
  escalation_reason?: "STATUTORY_DEADLINE_EXPIRED";
}

/**
 * The workflow is the final authority for case transitions. API clients may create
 * a CaseTask but cannot set DENIED or SUSPENDED directly; they must send a recorded
 * human approval signal that is retained in workflow history.
 */
export async function adjudicateCase(task: CaseTask): Promise<CaseWorkflowResult> {
  let approval: HumanApproval | undefined;
  let clockExpired = false;
  const events: CaseEvent[] = [];
  const currentState = initialState(task);
  const targetState = targetStateFor(task);

  setHandler(submitHumanApproval, (input: HumanApproval) => {
    approval = input;
  });

  const deadlineMs = Math.max(0, Date.parse(task.statutory_clock.deadline_at) - Date.parse(task.created_at));
  void sleep(deadlineMs).then(() => {
    clockExpired = true;
  });

  await condition(() => approval !== undefined || clockExpired);
  if (clockExpired && approval === undefined) {
    return { final_state: "ESCALATED", events, escalation_reason: "STATUTORY_DEADLINE_EXPIRED" };
  }

  const event = approvalEvent(task, currentState, targetState, approval);
  events.push(event);
  return { final_state: targetState, events };
}
