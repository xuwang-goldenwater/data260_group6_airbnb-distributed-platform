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
  "actor_id": "matching-agent",
  "entity": {
    "entity_type": "debate",
    "entity_id": "deb-01JAI001"
  },
  "payload": {},
  "idempotency_key": "tr-01JAI001:matching:round-0",
  "event_id": "ev-01JAI001",
  "producer": "ai-service",
  "schema_version": "1.0"
}
```

`trace_id` is the Kafka message key for debate events. Messages with the same key go to the same partition, so Kafka preserves their order within that topic and partition. This does not isolate different traces: different keys can share a partition. Isolation is enforced by the Chair, which groups messages by `trace_id` and never combines state from different debates. `agent_id` identifies the sender; it is not the Kafka key.

## REST API

### POST `/ai/requests`

Starts a recommendation workflow. The client sends the original natural-language request so the Parser can create the structured requirements before Round 0.

Request:

```json
{
  "guest_id": "guest-1001",
  "request_text": "I need a two-night stay in San Jose for two people, under $250 per night, with parking and a kitchen.",
  "property_pool_ref": {
    "pool_id": "benchmark-200-properties-v1",
    "dataset_version": "2026-11-01",
    "selection_rule": "deterministic_filter_then_property_id"
  },
  "evaluation_config": "full_debate"
}
```

`evaluation_config` is one of `baseline`, `single_agent`, `debate_without_auditor`, or `full_debate`. It supports the required 50-request comparison. In `baseline` mode, the benchmark runner records the same request/configuration but uses Xu's fixed `baseline.py` logic offline; it does not start an agent debate or publish debate-round events.

The Chair records the expected agents when it receives the request:

| Configuration | Expected agents | Debate behavior |
|---|---|---|
| `baseline` | none | Uses the fixed benchmark baseline offline; no agent debate is started |
| `single_agent` | Matching | One Matching response; no challenge rounds |
| `debate_without_auditor` | Matching, Risk | Round 1 challenges Matching/Risk; no Auditor challenge |
| `full_debate` | Matching, Risk, Fairness Auditor | Full Round 0, Round 1, and Round 2; Auditor must receive a challenge |

The expected list is stored in `debates[trace_id]`. A round is complete only when every expected agent has replied or its deadline has produced a recorded timeout.

Response `202 Accepted`:

```json
{
  "task_id": "tr-01JAI001",
  "trace_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "status": "started"
}
```

`task_id` is the public name for the workflow identifier and is equal to `trace_id`. Invalid input returns `400`; an unknown guest returns `404`; Kafka failure returns `503`.

### GET `/ai/tasks/{task_id}`

Returns workflow status, agent status, and the plan summary.

```json
{
  "task_id": "tr-01JAI001",
  "trace_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "status": "awaiting_approval",
  "current_round": 2,
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
    {"step_id": "round_1_2", "owner": "chair", "status": "in_progress"},
    {"step_id": "human_approval", "owner": "reviewer", "status": "pending"}
  ]
}
```

### GET `/ai/results/{task_id}`

Returns the persisted final recommendation, alternatives, votes, dissent, decision method, and limitations. It returns `404` until a result is published.

### WebSocket `/ai/tasks/{task_id}/stream`

Streams the shared event envelope for `ai.parsed`, `debate.round.0`, `debate.round.1`, `debate.round.2`, `ai.agent_timeout`, and `ai.results`. `ai.agent_timeout` is a WebSocket/dashboard notification derived from persisted timeout state; it is not a separate Kafka topic. The server sends only events with the requested `trace_id`. After a disconnect, the client calls the task and result endpoints to recover state.

### POST `/ai/results/{task_id}/approve`

Records a human approval. Approval does not itself create a booking, send a message, or change availability.

Request:

```json
{
  "reviewer_id": "guest-1001",
  "decision": "approved",
  "selected_property_id": "property-2001",
  "comment": "Meets the requested requirements"
}
```

### POST `/ai/results/{task_id}/edit`

Records a human-edited recommendation and the final selected property. The edited result must preserve the original trace and identify what changed.

```json
{
  "reviewer_id": "guest-1001",
  "decision": "edited",
  "selected_property_id": "property-2007",
  "reason": "Guest preferred a lower price"
}
```

### POST `/ai/results/{task_id}/reject`

Records a human rejection.

```json
{
  "reviewer_id": "guest-1001",
  "decision": "rejected",
  "reason": "No property meets the budget"
}
```

Only an authenticated human can approve, edit, or reject. An agent attempting an approval action receives `403`; a second decision receives `409`.

## Kafka Topics and Direction

| Topic | Producer | Consumer | Key | Purpose |
|---|---|---|---|---|
| `ai.requests` | AI API | Chair and all analysis agents | `trace_id` | Deliver the original request and start the workflow |
| `ai.parsed` | Parser | Chair and analysis agents | `trace_id` | Deliver the structured requirements |
| `debate.round.0` | Each analysis agent | Chair | `trace_id` | Each agent publishes its independent first opinion |
| `debate.round.1` | Chair | Agents and Fairness Auditor | `trace_id` | Ask targeted challenge questions |
| `debate.round.2` | Each agent | Chair | `trace_id` | Each agent publishes its answer and vote |
| `ai.results` | Chair | AI API, client, analytics | `trace_id` | Publish recommendation, votes, dissent, and adjudication |
| `ai.approvals` | AI API | Audit and analytics | `trace_id` | Persist approved, edited, or rejected decision |
| `ai.dlq` | Failed consumer | Operations | original key | Preserve permanently failed messages |

The Parser consumes `ai.requests` first and publishes `ai.parsed`. The `property_pool_ref` is passed through unchanged. After receiving `ai.requests` and `ai.parsed`, each analysis agent queries the same fixed 200-property pool with the same normalized filters. The query uses deterministic filtering and ordering, so agents can independently obtain the same candidate list without a new Chair-to-agent topic. The Parser does not query properties and is not a Round 0 debating agent.

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
    "dataset_version": "2026-11-01",
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
    "dataset_version": "2026-11-01",
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
  "debate_id": "deb-01JAI001",
  "plan_id": "plan-01JAI001",
  "agent_id": "matching",
  "status": "completed",
  "candidate_property_ids": ["property-2001", "property-2007", "property-2012"],
  "candidate_set_hash": "sha256:abc123...",
  "ranked_properties": [
    {
      "property_id": "property-2001",
      "score": 0.91,
      "score_breakdown": {
        "budget": 0.95,
        "location": 0.88,
        "amenities": 1.0,
        "rating": 0.82
      },
      "reasons": ["within budget", "parking", "rating 4.8"]
    }
  ],
  "confidence": 0.84,
  "evidence_ids": ["property-2001", "availability-2001-20261120"],
  "reflection": {
    "status": "passed",
    "concerns": []
  },
  "risks": [],
  "latency_ms": 182,
  "error": null
}
```

`status` is `completed`, `timeout`, or `error`. A timeout or error includes a non-null `error` object. `score` and `confidence` range from `0` to `1`, and ranked properties must come from the candidate list.

## `debate.round.1` Payload

The Chair publishes targeted questions after comparing Round 0. The Fairness Auditor must receive at least one challenge in every full debate.

```json
{
  "round": 1,
  "debate_id": "deb-01JAI001",
  "plan_id": "plan-01JAI001",
  "trigger": "score_disagreement",
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
  "deadline_ms": 10000
}
```

## `debate.round.2` Payload

Each agent publishes its own response and vote to the Chair. The Chair, not the agents, calculates totals and makes the final adjudication.

```json
{
  "round": 2,
  "debate_id": "deb-01JAI001",
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
    "reason": "The original recommendation satisfies the dates and availability evidence."
  },
  "confidence": 0.88
}
```

The allowed vote choices are `keep_original`, `switch_to_challenger`, and `abstain`. Every vote must include a reason.

## `ai.results` Payload

```json
{
  "debate_id": "deb-01JAI001",
  "plan_id": "plan-01JAI001",
  "status": "awaiting_approval",
  "recommendation": {
    "property_id": "property-2001",
    "score": 0.91,
    "reasons": ["within budget", "parking", "rating 4.8"],
    "confidence": 0.84
  },
  "alternatives": [
    {"property_id": "property-2007", "score": 0.76, "rejection_reason": "higher nightly price"}
  ],
  "vote_summary": {"keep_original": 3, "switch_to_challenger": 1, "abstain": 0},
  "dissent": [
    {"agent_id": "fairness", "reason": "Candidate has limited historical data", "severity": "medium"}
  ],
  "adjudication": {
    "method": "consensus",
    "deadlock": false,
    "decision_rule": "majority_vote"
  },
  "limitations": [],
  "approval_required": true
}
```

`adjudication.method` is `consensus` when agents converge and `rule_forced` when the Chair applies the documented decision rule after disagreement or deadlock. A forced decision must include the rule and dissent. This makes the required deadlock case visible and reproducible.

## Plan, Timeout, and Safety Rules

- The Parser is the first plan step: it converts `request_text` into structured requirements and records parse confidence and missing fields.
- Each analysis agent retrieves candidates from the fixed pool named by `property_pool_ref`, using the normalized request and deterministic filtering/order. Every Round 0 response includes `candidate_property_ids` and `candidate_set_hash`.
- The Chair compares the candidate IDs and hashes across Round 0 responses. A mismatch is an invalid round result and triggers retry or safe stop; the Chair never combines rankings made from different candidate sets.
- Every workflow has one `plan_id` linked to one `trace_id`. Each step records input, expected output, responsible agent, status, and timestamps.
- The Chair stores the expected agent list from `evaluation_config` and maintains separate state for every `trace_id`.
- A dependent step starts only after its prerequisite completes. A failed step is retried, skipped with an explanation, or stopped safely.
- The Chair applies a deadline to every round and records the missing agent and failure reason.
- A late message for a round that has already advanced is not used to recalculate that round. The Chair persists it as a late event, records the reason, and prevents it from affecting the next round or final vote.
- Chair state is recoverable: offsets and events remain in Kafka, and the Chair republishes or reprocesses uncommitted messages after restart. The Chair commits an offset only after persisting the message and updating durable debate state; otherwise the message is replayed safely. Durable state and idempotency records prevent duplicate votes or results.
- If a permitted partial result is published after a timeout, `limitations` identifies the missing agent and `approval_required` remains `true`.
- Agents cannot approve recommendations or perform booking, availability, messaging, or other external actions.
- Only an authenticated human can approve, edit, or reject a result.
- Plans, events, responses, votes, dissent notes, errors, and approval decisions are persisted by `trace_id`.

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
