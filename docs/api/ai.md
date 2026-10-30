# AI Service API

Owner: Haozhe Wang
Role: D Agent Orchestration
Status: draft for team review

## Scope

The AI service accepts a guest recommendation request, creates a trace, coordinates independent agents through Kafka, streams debate progress to the client, and publishes a recommendation for human approval. Agents do not call one another directly over HTTP.

All Kafka messages use the shared envelope from `docs/api/00_conventions.md`:

```json
{
  "event_id": "ev-01JAI001",
  "event_type": "debate.round.0",
  "trace_id": "tr-01JAI001",
  "occurred_at": "2026-10-30T10:00:00Z",
  "producer": "ai-service",
  "schema_version": "1.0",
  "payload": {}
}
```

`trace_id` is the Kafka message key for every debate event. This keeps concurrent debates isolated.

## REST API

### POST `/ai/requests`

Starts a recommendation workflow and publishes an `ai.requests` event.

Request:

```json
{
  "guest_id": "guest-1001",
  "check_in": "2026-11-20",
  "check_out": "2026-11-22",
  "guest_count": 2,
  "location": "San Jose",
  "max_nightly_price": 250.0,
  "property_type": "entire_home",
  "amenities": ["parking", "kitchen"],
  "minimum_rating": 4.5,
  "request_text": "Quiet place near downtown"
}
```

Response `202 Accepted`:

```json
{
  "trace_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "status": "started"
}
```

Validation: required fields are `guest_id`, `check_in`, `check_out`, `guest_count`, and `location`. `check_out` must be later than `check_in`; `guest_count` and `max_nightly_price` must be positive. Invalid input returns `400`; an unknown guest returns `404`; Kafka failure returns `503`.

### GET `/ai/requests/{trace_id}`

Returns the current workflow state.

```json
{
  "trace_id": "tr-01JAI001",
  "plan_id": "plan-01JAI001",
  "status": "awaiting_approval",
  "current_round": 2,
  "agent_status": [
    {"agent_id": "matching", "status": "completed"},
    {"agent_id": "risk", "status": "completed"},
    {"agent_id": "fairness", "status": "completed"}
  ],
  "result_id": "res-01JAI001",
  "updated_at": "2026-10-30T10:00:08Z"
}
```

Returns `401` for missing authentication or `404` when the trace does not exist.

### GET `/ai/requests/{trace_id}/events`

Returns persisted events in event-time order. This supports page reload and recovery after a WebSocket disconnect.

```json
{
  "trace_id": "tr-01JAI001",
  "events": [
    {
      "event_id": "ev-001",
      "event_type": "debate.round.0",
      "occurred_at": "2026-10-30T10:00:02Z"
    }
  ]
}
```

### WebSocket `/ai/requests/{trace_id}/stream`

The client subscribes with:

```json
{"action": "subscribe", "trace_id": "tr-01JAI001"}
```

The server sends the shared event envelope for `ai.status`, `debate.round.0`, `debate.round.1`, `debate.round.2`, `ai.agent_timeout`, and `ai.results`. It sends only events with the requested `trace_id`. A disconnected client calls the events endpoint to recover missed messages.

### POST `/ai/requests/{trace_id}/approve`

Records a human approval. Approval does not directly create a booking, send a message, or change availability.

Request:

```json
{
  "reviewer_id": "guest-1001",
  "decision": "approved",
  "selected_property_id": "property-2001",
  "comment": "Meets the requested requirements"
}
```

Response `200 OK`:

```json
{
  "trace_id": "tr-01JAI001",
  "approval_id": "apr-01JAI001",
  "decision": "approved",
  "status": "approved"
}
```

Only an authenticated human can approve. An agent attempting this action receives `403`. A second decision for the same trace receives `409`.

### POST `/ai/requests/{trace_id}/reject`

Records a human rejection.

Request:

```json
{
  "reviewer_id": "guest-1001",
  "reason": "No property meets the budget"
}
```

Response `200 OK`:

```json
{
  "trace_id": "tr-01JAI001",
  "approval_id": "apr-01JAI002",
  "decision": "rejected",
  "status": "rejected"
}
```

## Kafka Topics

| Topic | Producer | Consumer | Key | Purpose |
|---|---|---|---|---|
| `ai.requests` | AI API | Chair/harness | `trace_id` | Start a recommendation workflow |
| `debate.round.0` | Chair | Independent agents | `trace_id` | Collect independent first opinions |
| `debate.round.1` | Chair | Agents and auditor | `trace_id` | Ask targeted challenge questions |
| `debate.round.2` | Chair | Agents and auditor | `trace_id` | Collect answers and votes |
| `ai.results` | Chair | AI API and analytics | `trace_id` | Publish recommendation and dissent |
| `ai.approvals` | AI API | Audit/analytics | `trace_id` | Persist human decision |
| `ai.dlq` | Failed consumer | Operations | original key | Preserve permanently failed events |

Every consumer validates the envelope, uses bounded retries, and records an idempotency key based on `event_id` and `trace_id`. Permanently invalid messages go to `ai.dlq` with the original event and failure reason.

## `ai.requests` Payload

```json
{
  "plan_id": "plan-01JAI001",
  "guest_id": "guest-1001",
  "check_in": "2026-11-20",
  "check_out": "2026-11-22",
  "guest_count": 2,
  "location": "San Jose",
  "max_nightly_price": 250.0,
  "amenities": ["parking", "kitchen"],
  "minimum_rating": 4.5
}
```

## `debate.round.0` Payload

Round 0 gives each independent agent the same request and candidate properties. Agents must not see another agent's answer before publishing their own first opinion.

```json
{
  "round": 0,
  "debate_id": "deb-01JAI001",
  "plan_id": "plan-01JAI001",
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
  "candidate_property_ids": ["property-2001", "property-2007", "property-2012"],
  "target_agent_ids": ["parser", "matching", "risk", "fairness"],
  "deadline_ms": 10000,
  "response_topic": "debate.round.0.responses"
}
```

Required fields are `round`, `debate_id`, `plan_id`, `request`, `candidate_property_ids`, `target_agent_ids`, and `deadline_ms`. `candidate_property_ids` must be non-empty and `deadline_ms` must be positive.

## Round 0 Agent Response Payload

```json
{
  "round": 0,
  "debate_id": "deb-01JAI001",
  "agent_id": "matching",
  "status": "completed",
  "ranked_properties": [
    {
      "property_id": "property-2001",
      "score": 0.91,
      "reasons": ["within budget", "parking", "rating 4.8"]
    }
  ],
  "confidence": 0.84,
  "evidence_ids": ["property-2001", "availability-2001-20261120"],
  "risks": [],
  "latency_ms": 182,
  "model_version": "rules-v1",
  "error": null
}
```

`status` is `completed`, `timeout`, or `error`. A timeout or error must include a non-null `error` object. `score` and `confidence` range from `0` to `1`, and ranked properties must come from the requested candidate list.

## `debate.round.1` Payload

The Chair publishes targeted questions after comparing Round 0 opinions.

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
    }
  ],
  "deadline_ms": 10000
}
```

## `debate.round.2` Payload

```json
{
  "round": 2,
  "debate_id": "deb-01JAI001",
  "responses": [
    {
      "question_id": "q-001",
      "agent_id": "risk",
      "answer": "Availability was refreshed 2 seconds ago.",
      "evidence_ids": ["availability-2001-20261120"],
      "vote": "accept",
      "confidence": 0.88
    }
  ],
  "votes": {"accept": 3, "reject": 1, "abstain": 0},
  "dissent": [
    {
      "agent_id": "fairness",
      "reason": "Candidate has limited historical data",
      "severity": "medium"
    }
  ],
  "deadline_ms": 10000
}
```

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
    {
      "property_id": "property-2007",
      "score": 0.76,
      "rejection_reason": "higher nightly price"
    }
  ],
  "votes": {"accept": 3, "reject": 1, "abstain": 0},
  "dissent": [
    {
      "agent_id": "fairness",
      "reason": "Candidate has limited historical data",
      "severity": "medium"
    }
  ],
  "limitations": [],
  "approval_required": true
}
```

If an agent times out and the decision rule allows a partial result, the Chair may publish `ai.results`, but `limitations` must identify the missing agent and `approval_required` remains `true`.

## Plan, Timeout, and Safety Rules

- Every workflow has one `plan_id` linked to one `trace_id`.
- Each plan step records its input, expected output, responsible agent, status, and timestamps.
- A dependent step starts only after its prerequisite completes. A failed step is retried, skipped with an explanation, or stopped safely.
- The Chair applies a deadline to every round and records the missing agent and failure reason.
- Agents cannot approve recommendations or perform booking, availability, messaging, or other external actions.
- Only an authenticated human can approve or reject a result.
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
