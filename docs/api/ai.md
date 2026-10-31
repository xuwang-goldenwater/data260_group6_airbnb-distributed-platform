# AI Service API

Owner: Haozhe Wang
Role: D Agent Orchestration
Status: draft for team review

## Scope

The AI service accepts a natural-language guest request, creates a task and trace, coordinates independent agents through Kafka, streams debate progress to the client, and publishes a recommendation for human approval. Agents do not call one another directly over HTTP.

## Shared Event Envelope

Every Kafka message uses the required §6.2 fields below. The additional fields are optional metadata and do not replace a required field.

```json
{
  "event_type": "debate.round.0",
  "trace_id": "tr-01JAI001",
  "timestamp": "2026-10-30T10:00:00Z",
  "actor_id": "matching",
  "entity": {
    "entity_type": "debate",
    "entity_id": "tr-01JAI001:attempt-1"
  },
  "payload": {},
  "idempotency_key": "tr-01JAI001:attempt-1:matching:round-0",
  "attempt": 1,
  "event_id": "ev-01JAI001",
  "producer": "ai-service",
  "schema_version": "1.0"
}
```

`trace_id` is the Kafka message key for debate events. Messages with the same key go to the same partition, so Kafka preserves their order within that topic and partition. This does not isolate different traces: different keys can share a partition. Isolation is enforced by the Chair, which groups messages by `trace_id` and `debate_id` and never combines state from different debates. `actor_id` and payload `agent_id` use the same value, such as `matching`.

`task_id` and `trace_id` identify the same workflow. One workflow has one `plan_id`. A retry creates a new `debate_id` such as `tr-01JAI001:attempt-2` while keeping the same `task_id`, `trace_id`, and `plan_id`. The Kafka key remains `trace_id`; `attempt` is included in the event and idempotency key so a retry is not deduplicated as the first attempt.

## REST API

### POST `/ai/requests`

Starts a recommendation workflow. The client sends the original natural-language request so the Parser can create the structured requirements before Round 0.

Request with `Authorization: Bearer <JWT>`:

```json
{
  "guest_id": "guest-1001",
  "request_text": "I need a two-night stay in San Jose from November 20 to November 22 for two people, under $250 per night, with parking, a kitchen, and a rating of at least 4.5.",
  "property_pool_ref": {
    "pool_id": "benchmark-200-properties-v1",
    "dataset_version": "2026-10-30",
    "selection_rule": "deterministic_filter_then_property_id"
  },
  "round_deadline_ms": 10000,
  "evaluation_config": "full_debate"
}
```

The client must send an `Idempotency-Key` header. A repeated key returns the original task instead of starting a second workflow. `evaluation_config` is one of `baseline`, `single_agent`, `debate_without_auditor`, or `full_debate`. It supports the required 50-request comparison. In `baseline` mode, the benchmark runner records the same request/configuration but uses Xu's fixed `baseline.py` logic offline; it does not start an agent debate or publish debate-round events.

The Chair records the expected agents when it receives the request:

| Configuration | Expected agents | Debate behavior |
|---|---|---|
| `baseline` | none | Uses the fixed benchmark baseline offline; no agent debate is started |
| `single_agent` | Matching | One Matching response; no challenge rounds |
| `debate_without_auditor` | Matching, Risk | Round 1 challenges Matching/Risk; no Auditor challenge |
| `full_debate` | Matching, Risk, Fairness Auditor | All three publish independent Round 0 opinions; Auditor receives a Round 1 challenge; all three vote in Round 2 |

The expected list is stored in `debates[trace_id]`. A round is complete only when every expected agent has replied or its deadline has produced a recorded timeout.

`round_deadline_ms` is the Chair-controlled deadline for each round. The Chair converts it to and persists an absolute `deadline_at` for Round 0, Round 1, and Round 2; agents cannot extend their own deadline.

Response `202 Accepted`:

```json
{
  "task_id": "tr-01JAI001",
  "trace_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "status": "started"
}
```

`task_id` is the public name for the workflow identifier and is equal to `trace_id`. Invalid input returns `400`; an unknown guest returns `404`; Kafka failure returns `503`. If the task was created but Kafka publication is temporarily unavailable, the task remains in `publish_pending` and is retried using the same task and idempotency key; the API does not create a second trace.

Before publishing `ai.requests`, the AI API validates `guest_id` through the User Service's internal REST interface. This synchronous lookup is the only user-existence dependency at request creation; the guest record is not copied into the AI debate state.

### GET `/ai/tasks/{task_id}`

Returns workflow status, agent status, and the plan summary.

```json
{
  "task_id": "tr-01JAI001",
  "trace_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "debate_id": "tr-01JAI001:attempt-1",
  "attempt": 1,
  "status": "awaiting_approval",
  "current_round": 2,
  "round_deadline_at": "2026-10-30T10:00:20Z",
  "agent_status": [
    {"agent_id": "parser", "status": "completed"},
    {"agent_id": "matching", "status": "completed"},
    {"agent_id": "risk", "status": "completed"},
    {"agent_id": "fairness", "status": "completed"}
  ],
  "plan": [
    {"step_id": "parse", "owner": "parser", "status": "completed"},
    {"step_id": "retrieve_candidates", "owner": "analysis-agents", "status": "completed"},
    {"step_id": "round_0", "owner": "chair", "status": "completed"},
    {"step_id": "round_1_2", "owner": "chair", "status": "completed"},
    {"step_id": "adjudicate", "owner": "chair", "status": "completed"},
    {"step_id": "explain", "owner": "explanation", "status": "completed"},
    {"step_id": "human_approval", "owner": "reviewer", "status": "pending"}
  ]
}
```

### GET `/ai/results/{task_id}`

Returns the persisted `ai.results` payload: final recommendation, `final_ranking`, alternatives, votes, dissent, `disagreement`, adjudication, measured `evidence`, generated `explanation`, and limitations. It returns `404` until a result is published.

### WebSocket `/ai/tasks/{task_id}/stream`

The client authenticates with `Authorization: Bearer <JWT>` during the WebSocket handshake. The server streams the shared event envelope for `ai.parsed`, `debate.round.0`, `debate.round.1`, `debate.round.2`, `ai.agent_timeout`, and `ai.results`. `ai.agent_timeout` is a WebSocket/dashboard notification derived from persisted timeout state; it is not a separate Kafka topic. The server sends only events with the requested `trace_id`. After a disconnect, the client calls the task and result endpoints to recover state.

### POST `/ai/results/{task_id}/approve`

Records a human approval. Approval does not itself create a booking, send a message, or change availability.

Request:

```json
{
  "decision": "approved",
  "selected_property_id": "property-2001",
  "comment": "Meets the requested requirements"
}
```

### POST `/ai/results/{task_id}/edit`

Records a human-edited recommendation and the final selected property. The edited result must preserve the original trace and identify what changed.

```json
{
  "decision": "edited",
  "selected_property_id": "property-2007",
  "reason": "Guest preferred a lower price"
}
```

### POST `/ai/results/{task_id}/reject`

Records a human rejection.

```json
{
  "decision": "rejected",
  "reason": "No property meets the budget"
}
```

Only an authenticated human can approve, edit, or reject. The service takes the reviewer identity and role from the validated JWT subject and never trusts a `reviewer_id` from the request body. An agent attempting an approval action receives `403`; a second decision receives `409`. The project may allow the authenticated guest or host to make the decision, but the identity always comes from authentication.

## Kafka Topics and Direction

| Topic | Producer | Consumer | Key | Purpose |
|---|---|---|---|---|
| `ai.requests` | AI API | Parser and Chair | `trace_id` | Deliver the original request and start the workflow |
| `ai.parsed` | Parser | Chair and analysis agents | `trace_id` | Deliver the structured requirements and property-pool reference |
| `debate.round.0` | Each analysis agent | Chair | `trace_id` | Each agent publishes its independent first opinion |
| `debate.round.1` | Chair | Agents and Fairness Auditor | `trace_id` | Ask targeted challenge questions |
| `debate.round.2` | Each agent | Chair | `trace_id` | Each agent publishes its answer and vote |
| `ai.results` | Chair | AI API, WebSocket gateway, analytics | `trace_id` | Publish recommendation, votes, dissent, and adjudication |
| `ai.approvals` | AI API | Audit and analytics | `trace_id` | Persist approved, edited, or rejected decision |
| `ai.dlq` | Failed consumer | Operations | original key | Preserve permanently failed messages |

The Parser consumes `ai.requests` first and publishes `ai.parsed`. Analysis agents subscribe only to `ai.parsed`, not to both topics, so they do not need a cross-topic join or an in-memory cache of two messages. The `property_pool_ref` is passed through unchanged. Each agent queries the same fixed 200-property pool with the same normalized filters. The query uses deterministic filtering and ordering, so agents can independently obtain the same candidate list without a new Chair-to-agent topic. The Parser does not query properties and is not a Round 0 debating agent.

Every consumer validates the envelope, uses bounded retries, and deduplicates with `idempotency_key`. Permanently invalid messages go to `ai.dlq` with the original event and failure reason.

## `ai.requests` Payload

```json
{
  "task_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "guest_id": "guest-1001",
  "request_text": "I need a two-night stay in San Jose for two people, under $250 per night, with parking and a kitchen.",
  "property_pool_ref": {
    "pool_id": "benchmark-200-properties-v1",
    "dataset_version": "2026-10-30",
    "selection_rule": "deterministic_filter_then_property_id"
  },
  "evaluation_config": "full_debate"
}
```

## `ai.parsed` Payload

```json
{
  "task_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "property_pool_ref": {
    "pool_id": "benchmark-200-properties-v1",
    "dataset_version": "2026-10-30",
    "selection_rule": "deterministic_filter_then_property_id"
  },
  "request": {
    "guest_id": "guest-1001",
    "check_in": "2026-11-20",
    "check_out": "2026-11-22",
    "guest_count": 2,
    "location": "San Jose",
    "max_nightly_price": 250.0,
    "required_amenities": ["parking", "kitchen"],
    "minimum_rating": 4.5
  },
  "parse_confidence": 0.96,
  "missing_fields": []
}
```

The Parser output contains no property candidates. Each analysis agent combines this normalized request with the unchanged `property_pool_ref`, queries the fixed pool, sorts results by the declared selection rule, and computes a `candidate_set_hash`. The agent includes the resulting candidate IDs and hash in its Round 0 response.

## `debate.round.0` Payload

Each analysis agent publishes one message to the Chair. Agents do not consume other agents' Round 0 outputs. The detailed agent-output contract is finalized with E in `docs/api/agent_payloads.md`.

```json
{
  "round": 0,
  "debate_id": "tr-01JAI001:attempt-1",
  "attempt": 1,
  "plan_id": "plan-01JAI001",
  "agent_id": "matching",
  "status": "completed",
  "candidate_property_ids": ["property-2001", "property-2007", "property-2012"],
  "candidate_set_hash": "sha256:abc123...",
  "scoring_version": "matcher-v1",
  "prompt_version": "matching-prompt-v1",
  "model": "none",
  "ranked_properties": [
    {
      "property_id": "property-2001",
      "rank": 1,
      "score": 0.91,
      "score_breakdown": {
        "budget": 0.95,
        "location": 0.88,
        "amenities": 1.0,
        "rating": 0.82,
        "availability": 1.0
      },
      "reasons": ["within budget", "parking", "rating 4.8"],
      "evidence_ids": ["property-2001", "availability-2001-20261120"]
    }
  ],
  "confidence": 0.84,
  "evidence_ids": ["property-2001", "availability-2001-20261120"],
  "reflection": {
    "cycle": 1,
    "checks": {
      "constraint": {"result": "pass", "note": "dates, budget, capacity, amenities satisfied for top 3"},
      "evidence": {"result": "pass", "note": "all reasons map to evidence_ids"},
      "risk": {"result": "pass", "note": "no risk flags in top 3"},
      "conflict": {"result": "na", "note": "no other agent output visible in Round 0"},
      "action": {"result": "pass", "note": "no external action proposed"}
    },
    "decision": "keep",
    "changed_top_1": false,
    "before_top_1": "property-2001",
    "after_top_1": "property-2001"
  },
  "latency_ms": 182,
  "error": null
}
```

The example shows one ranked entry for brevity. A real Round 0 response contains **at least 10** `ranked_properties` (or every candidate if fewer than 10 exist), ordered by `score` descending with contiguous `rank` values starting at 1. This is required for precision@10 and recall@10 in all four configurations.

`scoring_version` is required in every Round 0 response (for example `matcher-v1`, `risk-rules-v1`, `fairness-v1`). It identifies which versioned scoring rule produced the ranking so a self-improvement change such as `matcher-v2` can be compared against `matcher-v1` on the same 50 requests (§7.5.4).

`status` is `completed`, `timeout`, or `error`. A timeout or error includes a non-null `error` object. `score` and `confidence` range from `0` to `1`, and ranked properties must come from the candidate list.

The `reflection` object, the agent-specific fields (`risk_flags` for Risk, `audit_findings` for the Fairness Auditor), and the ten harness validation rules are defined in `docs/api/agent_payloads.md` §3, §5 and §7. That file is the source of truth for field-level detail inside agent payloads; this file defines the envelope, topics, direction, deadlines and Chair behavior.

The example score uses the versioned weights `budget=0.30`, `location=0.25`, `amenities=0.20`, `rating=0.15`, and `availability=0.10`. The weights are stored with the model/configuration version and must be included in the trace when changed.

## `debate.round.1` Payload

The Chair publishes targeted questions after comparing Round 0. The Fairness Auditor must receive at least one challenge in every full debate.

```json
{
  "round": 1,
  "debate_id": "tr-01JAI001:attempt-1",
  "attempt": 1,
  "plan_id": "plan-01JAI001",
  "trigger": "score_disagreement",
  "original_property_id": "property-2001",
  "challenger_property_id": "property-2007",
  "questions": [
    {
      "question_id": "q-001",
      "target_agent_id": "risk",
      "property_id": "property-2001",
      "question": "Check whether availability is current for the requested dates.",
      "required_evidence": ["availability", "event_trace"]
    },
    {
      "question_id": "q-002",
      "target_agent_id": "fairness",
      "property_id": "property-2001",
      "question": "Audit whether the ranking disadvantages properties with less historical data.",
      "required_evidence": ["ranking_scores", "property_history"]
    }
  ],
  "deadline_ms": 10000,
  "deadline_at": "2026-10-30T10:00:20Z"
}
```

`original_property_id` is the Chair's current top-1 property before the challenge (the Matching agent's Round 0 top-1). `challenger_property_id` is the competing top-1 from the agent that disagreed. A Round 2 vote of `keep_original` or `switch_to_challenger` therefore refers to exactly one of these two property IDs.

## `debate.round.2` Payload

Each expected agent publishes its own response and vote to the Chair. An agent that received no targeted question still publishes Round 2 with an empty `responses` array and a vote. The Chair, not the agents, calculates totals and makes the final adjudication.

```json
{
  "round": 2,
  "debate_id": "tr-01JAI001:attempt-1",
  "attempt": 1,
  "plan_id": "plan-01JAI001",
  "agent_id": "risk",
  "responses": [
    {
      "question_id": "q-001",
      "answer": "Availability was refreshed 2 seconds ago.",
      "evidence_ids": ["availability-2001-20261120"]
    }
  ],
  "vote": {
    "choice": "keep_original",
    "property_id": "property-2001",
    "reason": "The original recommendation satisfies the dates and availability evidence.",
    "evidence_ids": ["availability-2001-20261120"]
  },
  "revised_ranking": null,
  "confidence": 0.88,
  "deadline_at": "2026-10-30T10:00:30Z"
}
```

The allowed vote choices are `keep_original`, `switch_to_challenger`, and `abstain`. Every vote must include a reason. Unless the choice is `abstain`, `vote.property_id` must equal the Round 1 `original_property_id` or `challenger_property_id`, and `vote.evidence_ids` must reference at least one stored record or Round 0/1 message (§7.2). `revised_ranking` is set only when the challenge changed the agent's ranking (see `agent_payloads.md` §4).

## `ai.results` Payload

```json
{
  "debate_id": "tr-01JAI001:attempt-1",
  "plan_id": "plan-01JAI001",
  "status": "awaiting_approval",
  "recommendation": {
    "property_id": "property-2001",
    "score": 0.91,
    "reasons": ["within budget", "parking", "rating 4.8"],
    "confidence": 0.84
  },
  "final_ranking": [
    "property-2001", "property-2044", "property-2007", "property-2012", "property-2019",
    "property-2023", "property-2031", "property-2038", "property-2052", "property-2060"
  ],
  "alternatives": [
    {"property_id": "property-2007", "score": 0.76, "rejection_reason": "higher nightly price"}
  ],
  "vote_summary": {"keep_original": 1, "switch_to_challenger": 1, "abstain": 1},
  "dissent": [
    {"agent_id": "fairness", "reason": "Candidate has limited historical data", "severity": "medium"}
  ],
  "disagreement": {
    "top1_distinct": 2,
    "top10_overlap": 0.54,
    "max_score_gap": 0.18,
    "challenge_required": true
  },
  "adjudication": {
    "method": "rule_forced",
    "deadlock": true,
    "decision_rule": "safety_then_majority",
    "tie_break_applied": "lower_risk_penalty",
    "risk_penalty": {"property-2001": 0.05, "property-2007": 0.12}
  },
  "evidence": {
    "scoring_versions": {"matching": "matcher-v1", "risk": "risk-rules-v1", "fairness": "fairness-v1"},
    "score_breakdown": {"budget": 0.95, "location": 0.88, "amenities": 1.0, "rating": 0.82, "availability": 1.0},
    "evidence_ids": ["property-2001", "availability-2001-20261120", "review-8812"]
  },
  "explanation": {
    "text": "Property 2001 is within your $250 budget, has parking and a kitchen, and is rated 4.8. Property 2007 was also considered but costs more per night. One reviewer agent noted that 2001 has limited review history, so please check recent reviews before booking.",
    "source": "llm",
    "model": "explanation-prompt-v1",
    "generated": true
  },
  "limitations": [],
  "approval_required": true
}
```

- `final_ranking` contains at least 10 property IDs in order (or every candidate if fewer than 10). It is the list used to compute precision@10, recall@10 and the mean rank of the human top choice for every configuration.
- `disagreement` is computed by the Chair from the Round 0 responses. Field definitions and the `challenge_required` threshold are in `agent_payloads.md` §6.
- `evidence` holds measured values only: scores, scoring versions and IDs of stored records. `explanation` holds generated plain-language text only. The explanation is never used as input to scoring, voting or adjudication (§13.6).

### Adjudication and tie-break rule

`adjudication.method` is one of:

| Method | When |
|---|---|
| `single_agent` | `single_agent` configuration; no vote is held and the Matching ranking is used |
| `consensus` | every non-abstaining vote chooses the same property |
| `majority` | one property has strictly more non-abstaining votes than the other; dissenting votes are listed in `dissent` |
| `rule_forced` | votes are tied, or no valid vote was cast; `deadlock` is `true` |

The Chair applies `safety_then_majority` in this order:

1. **Safety gate.** A property that fails a hard constraint (dates, budget, capacity, required amenities) in the Chair's code check, or that has a `high` severity Risk flag, cannot be the final recommendation if any candidate without those problems exists. If every option fails, the Chair records this in `limitations`.
2. **Majority.** Among the remaining options, the property with more non-abstaining votes wins.
3. **Tie-break.** On a tie — for example a 1–1 split in `debate_without_auditor`, or 1–1–1 with an abstention in `full_debate` — the Chair chooses the property with the **lower total risk penalty** from the Risk agent's `risk_flags`. If still tied, it chooses the higher Round 0 Matching score. If still tied, it keeps `original_property_id`.
4. **No valid votes.** If every agent abstained or timed out, the Chair keeps `original_property_id`, sets `deadlock: true`, and adds a limitation.

The rule is deterministic, so a tied debate in the 50-request benchmark produces the same result on every rerun and does not need a person present. `tie_break_applied` names the step that decided the outcome. A forced decision always includes the dissent note, and `approval_required` stays `true`, so a human still approves or rejects every result.

## Plan, Timeout, and Safety Rules

- The Parser is the first plan step: it converts `request_text` into structured requirements and records parse confidence and missing fields.
- Each analysis agent retrieves candidates from the fixed pool named by `property_pool_ref`, using the normalized request and deterministic filtering/order. Every Round 0 response includes `candidate_property_ids` and `candidate_set_hash`.
- The Chair compares the candidate IDs and hashes across Round 0 responses. A mismatch is an invalid round result and triggers retry or safe stop; the Chair never combines rankings made from different candidate sets.
- Every workflow has one `plan_id` linked to one `trace_id`. Each step records input, expected output, responsible agent, status, and timestamps.
- The Chair stores the expected agent list from `evaluation_config` and maintains separate state for every `trace_id`.
- A dependent step starts only after its prerequisite completes. A failed step is retried, skipped with an explanation, or stopped safely.
- The Chair applies `round_deadline_ms` to every round and persists the absolute `deadline_at` timestamp in MongoDB. After restart it recalculates the remaining time from `deadline_at` instead of relying on an in-memory timer.
- An agent may report `status: timeout` when its internal LLM call exceeds its deadline. If no message arrives, the Chair records a separate `chair_recorded_timeout` for that agent after `deadline_at`.
- A late message from an earlier attempt or an already-advanced round is persisted as `late_or_stale`, but it is not used to recalculate the round, change the expected-agent count, or affect the final vote.
- A retry increments `attempt` and creates a new `debate_id` and idempotency namespace. Messages from an earlier attempt remain auditable but cannot update the current attempt.
- Chair state is recoverable: offsets and events remain in Kafka, and the Chair resumes by reading from the last committed offset. The Chair commits an offset only after persisting the message and updating durable debate state; otherwise the message is replayed safely.
- If a permitted partial result is published after a timeout, `limitations` identifies the missing agent and `approval_required` remains `true`.
- **Explanation step.** After adjudication and before publishing `ai.results`, the Chair calls the Explanation skill as the `explain` plan step. It receives only the already-decided recommendation, alternatives, dissent, limitations and `evidence`; it cannot change the ranking, the vote or the decision. Explanation does not consume `ai.results`, so there is no loop: `ai.results` is published once, with the explanation already inside it.
- The `explain` step has its own deadline (`explain_deadline_ms`, default 5000). If the LLM call times out or fails, the Chair fills `explanation.text` from a fixed template built from `evidence` (budget, amenities, rating, top risk), sets `explanation.source` to `template_fallback`, adds `explanation_fallback` to `limitations`, and still publishes the result. A slow explanation never blocks the debate (§13.10).
- Agents cannot approve recommendations or perform booking, availability, messaging, or other external actions.
- Only an authenticated human can approve, edit, or reject a result.
- Plans, events, responses, votes, dissent notes, errors, deadlines, and approval decisions are persisted by `trace_id` in MongoDB `debates` and `event_traces` collections. The Chair owns the debate-state record; the platform analytics service reads the event-trace records for dashboards.

## Common Error Body

```json
{
  "error": {
    "code": "INVALID_AGENT_OUTPUT",
    "message": "ranked_properties contains an unknown property_id",
    "trace_id": "tr-01JAI001",
    "retryable": false,
    "details": {"agent_id": "matching", "round": 0}
  }
}
```

The API uses `400` for invalid input, `401` for missing authentication, `403` for prohibited agent actions, `404` for unknown resources, `409` for duplicate or conflicting decisions, `422` for invalid agent output, `429` for rate limits, and `503` for unavailable dependencies.

The final error field names and status conventions must be checked against `docs/api/00_conventions.md` after C completes that shared document.
