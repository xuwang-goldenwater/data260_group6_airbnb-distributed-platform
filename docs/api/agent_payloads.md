# Agent Payloads

Owner: Xu Wang
Role: E Agent Quality & Evaluation
Status: draft for team review

## Scope

This file defines what each agent puts inside `payload` of the debate events. The envelope, topics, keys, deadlines, retries and REST endpoints are defined by D in `docs/api/ai.md`; nothing here changes them. Field names below extend D's Round 0 / Round 2 examples and keep every field D already defined.

| Agent | Owner | Consumes | Publishes |
|---|---|---|---|
| Parser | E | `ai.requests` | `ai.parsed` |
| Matching | E | `ai.parsed`, `debate.round.1` | `debate.round.0`, `debate.round.2` |
| Review & Risk | E | `ai.parsed`, `debate.round.1` | `debate.round.0`, `debate.round.2` |
| Fairness Auditor | E | `ai.parsed`, `debate.round.1` | `debate.round.0`, `debate.round.2` |
| Explanation | D | `ai.results` (internal to Chair) | `explanation` field of `ai.results` |

Design rule: **scores are computed by deterministic code; the LLM only parses the request, writes reasons, and answers challenges.** This keeps rankings reproducible from stored messages (§13.5) and keeps measured evidence separate from generated text (§13.6).

## 1. `ai.parsed` — Parser additions

Adds optional fields to D's `request` object. Missing values stay `null` and are listed in `missing_fields`.

| Field | Type | Required | Rule |
|---|---|---|---|
| `property_type` | string \| null | no | one of `apartment`, `house`, `condo`, `cabin`, `room` |
| `trip_purpose` | string \| null | no | one of `business`, `family`, `couple`, `group`, `remote_work`; used to group benchmark results |
| `bedrooms_min` | int \| null | no | ≥ 0 |
| `nice_to_have_amenities` | string[] | no | scored, never used as a hard filter |

## 2. Common Round 0 fields (all analysis agents)

Every agent's Round 0 payload contains D's fields plus the ones marked **new**.

| Field | Type | Rule |
|---|---|---|
| `agent_id` | string | `matching`, `risk` or `fairness`; equals envelope `actor_id` |
| `status` | string | `completed`, `timeout`, `error` |
| `candidate_property_ids` | string[] | deterministic candidate list from the fixed pool |
| `candidate_set_hash` | string | `sha256:` of the sorted candidate IDs; must match across agents |
| `ranked_properties` | object[] | **at least 10 entries** (or all candidates if fewer); ordered by `score` desc; ties broken by `property_id` |
| `confidence` | number | 0–1 |
| `evidence_ids` | string[] | every ID must exist in MySQL/Mongo (property, availability, review) |
| `reflection` | object | see §5 |
| `scoring_version` | string | **new** — e.g. `matcher-v1`, `risk-rules-v1`, `fairness-v1` |
| `prompt_version` | string | **new** — e.g. `matching-prompt-v1` |
| `model` | string | **new** — model ID used for text; `none` if no LLM call |
| `llm_cache_hit` | bool | **new** — true if the text came from the input-hash cache |
| `latency_ms` | int | ≥ 0 |
| `error` | object \| null | non-null when `status` ≠ `completed` |

Each `ranked_properties[]` entry:

| Field | Type | Rule |
|---|---|---|
| `property_id` | string | must be in `candidate_property_ids` |
| `rank` | int | **new** — 1-based, contiguous |
| `score` | number | 0–1 |
| `score_breakdown` | object | factor → 0–1; factors depend on the agent (below) |
| `reasons` | string[] | 1–3 short reasons (generated text) |
| `evidence_ids` | string[] | **new** — IDs supporting this property's score |

## 3. Agent-specific Round 0 content

### 3.1 Matching

`score_breakdown` keys and weights (from `ai.md`, versioned as `matcher-v1`): `budget 0.30`, `location 0.25`, `amenities 0.20`, `rating 0.15`, `availability 0.10`. Weights are echoed in the payload:

```json
"weights": {"budget": 0.30, "location": 0.25, "amenities": 0.20, "rating": 0.15, "availability": 0.10}
```

### 3.2 Review & Risk

Ranks by `score = matcher_score × (1 − risk_penalty)`. Adds `risk_flags` per property:

```json
"risk_flags": [
  {
    "property_id": "property-2001",
    "type": "repeated_cancellations",
    "severity": "high",
    "penalty": 0.25,
    "evidence_ids": ["review-8812", "reservation-5531"],
    "detail": "3 host cancellations in the last 90 days"
  }
]
```

`type` is one of `repeated_cancellations`, `cleanliness_complaints`, `missing_amenity_reports`, `unusual_price`, `slow_host_response`, `stale_availability`. `severity` is `low` / `medium` / `high` with penalties 0.05 / 0.12 / 0.25 (versioned as `risk-rules-v1`). Review evidence comes from the Review service (`reviews` collection).

### 3.3 Fairness Auditor

Ranks with the matching score after removing signals that should not count, and reports what it found:

```json
"audit_findings": [
  {
    "signal": "review_count",
    "affected_property_ids": ["property-2044"],
    "objection": "property-2044 meets every requirement but ranks 14th only because it has 2 reviews",
    "suggested_action": "move_into_top_10",
    "rank_without_signal": 4
  }
]
```

`signal` is one of `price_level`, `neighborhood_popularity`, `review_count`, `host_history_length`, `property_type`. An empty list is allowed and means "no unfair signal found".

## 4. Round 2 additions

D's Round 2 payload plus:

| Field | Type | Rule |
|---|---|---|
| `vote.choice` | string | `keep_original`, `switch_to_challenger`, `abstain` |
| `vote.property_id` | string \| null | **new** — the property voted for; `null` for `abstain` |
| `vote.reason` | string | required, non-empty |
| `vote.evidence_ids` | string[] | **new** — at least one ID from a stored Round 0/1 message or record (§7.2: votes must refer to stored evidence) |
| `revised_ranking` | object[] \| null | **new** — set only if the challenge changed this agent's ranking; same shape as `ranked_properties` |

Round 1 needs `original_property_id` next to `challenger_property_id` so `keep_original` is unambiguous (requested from D).

## 5. Reflection object (§7.5.3)

One initial check and at most one revision per agent per round (`max_cycles = 2`).

```json
"reflection": {
  "cycle": 1,
  "checks": {
    "constraint": {"result": "pass", "note": "dates, budget, capacity, amenities satisfied for top 3"},
    "evidence":   {"result": "pass", "note": "all reasons map to evidence_ids"},
    "risk":       {"result": "warn", "note": "property-2001 has 2 cancellation reviews"},
    "conflict":   {"result": "na",   "note": "no other agent output visible in Round 0"},
    "action":     {"result": "pass", "note": "no external action proposed"}
  },
  "decision": "keep",
  "changed_top_1": false,
  "before_top_1": "property-2001",
  "after_top_1": "property-2001"
}
```

- `result` is `pass`, `warn`, `fail` or `na`. Any `fail` forces `decision` = `revise` or `reject`.
- `decision` is `keep`, `revise` or `reject`. `reject` sets `status: error` with `error.code = REFLECTION_REJECTED`.
- The `constraint` and `evidence` checks are computed by code, not by the LLM.
- `before_top_1` / `after_top_1` let the evaluation measure whether reflection changed accuracy (§7.5.3).
- `conflict` is `na` in Round 0 (agents are independent) and is filled in Round 2.

## 6. Disagreement and final ranking (computed by the Chair, published in `ai.results`)

Requested additions to D's `ai.results`:

```json
"final_ranking": ["property-2001", "property-2044", "property-2007", "..."],
"disagreement": {
  "top1_distinct": 2,
  "top10_overlap": 0.54,
  "max_score_gap": 0.18,
  "challenge_required": true
}
```

- `final_ranking`: at least 10 property IDs, ordered. Used for precision@10, recall@10 and mean rank of the human top choice.
- `top1_distinct`: number of different top-1 properties across Round 0 agents (1 = full agreement).
- `top10_overlap`: mean pairwise Jaccard similarity of the agents' top-10 sets (0–1).
- `max_score_gap`: largest difference between agents' scores for the same property in anyone's top 3.
- `challenge_required` is `true` when `top1_distinct > 1` **or** `top10_overlap < 0.6`. In `full_debate` the Fairness Auditor is challenged regardless (§7.2).

## 7. Validation rules (harness, §7.5.1)

The Chair validates every agent payload before using it. A failed check returns error code `INVALID_AGENT_OUTPUT` (`422` on REST), the message is retried once, then moved to `ai.dlq` with the original `trace_id` and reason.

| # | Check | Failure code detail |
|---|---|---|
| 1 | All required fields present with correct types | `missing_field` / `bad_type` |
| 2 | `trace_id`, `debate_id`, `attempt` match the current debate | `wrong_debate` |
| 3 | `agent_id` is in the expected-agent list for this config | `unexpected_agent` |
| 4 | Every `property_id` is in `candidate_property_ids` | `unknown_property` |
| 5 | `candidate_set_hash` equals the Chair's hash | `candidate_mismatch` |
| 6 | `score`, `confidence`, breakdown values in 0–1; `rank` contiguous from 1 | `out_of_range` |
| 7 | `ranked_properties` has ≥ 10 entries or all candidates | `ranking_too_short` |
| 8 | Every `evidence_ids` entry resolves to a stored record | `unknown_evidence` |
| 9 | Vote has a reason and, unless `abstain`, a `property_id` | `invalid_vote` |
| 10 | No field requests an external action (booking, message, availability) | `prohibited_action` |

The demo (§7.5.5) sends one payload that breaks check 4 and shows it rejected.

## 8. Evaluation records

### 8.1 Benchmark run row (`benchmarks/results/<config>.csv`)

| Column | Meaning |
|---|---|
| `request_id` | ID from `benchmarks/requests.yaml` |
| `config` | `baseline`, `single_agent`, `debate_without_auditor`, `full_debate` |
| `trace_id` | empty for `baseline` |
| `scoring_version` | e.g. `matcher-v1` |
| `final_ranking` | top-10 property IDs, `;`-separated |
| `precision_at_10`, `recall_at_10` | against `labels_final.csv` |
| `human_top_rank` | rank of the human top choice in `final_ranking` (11 if absent) |
| `latency_ms` | request to `ai.results` |
| `auditor_changed_outcome` | true if top-1 differs from the run without the auditor |

### 8.2 Human feedback record (Mongo `feedback`, §7.3.2, §7.5.4)

```json
{
  "trace_id": "tr-01JAI001",
  "recommended_property_id": "property-2001",
  "decision": "edited",
  "selected_property_id": "property-2007",
  "reason": "Guest preferred a lower price",
  "reason_category": "price",
  "scoring_version": "matcher-v1",
  "reviewer_role": "guest",
  "created_at": "2026-11-24T18:00:00Z"
}
```

`reason_category` is one of `price`, `location`, `amenities`, `risk`, `fairness`, `explanation`, `other`. A proposed change (e.g. `matcher-v2`) cites the feedback records it is based on, is run on the same 50 requests, and is promoted only after a team member approves it; `matcher-v1` stays available for rollback.
