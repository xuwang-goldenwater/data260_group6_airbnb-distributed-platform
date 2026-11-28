# Property search cache measurement

Local HTTP benchmark: 100 measured requests per mode, concurrency 4,
after warm-up. Both modes returned identical results.
This benchmark covers undated listing search and excludes Booking latency.

| Metric | Cache enabled | Cache disabled |
|---|---:|---:|
| Mean latency (ms) | 12.1693 | 12.0373 |
| p95 latency (ms) | 17.1548 | 15.7394 |
| p99 latency (ms) | 17.9339 | 18.1925 |
| Throughput (requests/s) | 318.3406 | 320.6727 |

Enabled: 100 HIT responses. Disabled: 100 BYPASS responses.

No speedup was observed in this run. The small local dataset and Redis/version-check
overhead may explain the similar performance; this run does not establish
statistical significance or performance at larger scale.

Correctness: all 96 tests passed. Live smoke verified cache hits, live Booking
checks on dated hits, and Kafka-triggered invalidation. The invalidation smoke
used a synthetic hint and did not modify reservations or listings.

Raw measurements: search_cache.json.
