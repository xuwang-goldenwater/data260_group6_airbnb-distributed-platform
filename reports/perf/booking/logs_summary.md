# Log summary (raw logs are kept locally in reports/perf/booking/logs/, not committed)

| log file | lines | records processed | transient retries | dlq writes | redeliveries (duplicate) | offset commits | errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| dlq/worker-1_log.txt | 10 | 0 | 0 | 1 | 0 | 1 | 1 |
| dlq/worker-2_log.txt | 26 | 0 | 0 | 10 | 0 | 10 | 10 |
| latency/api_log.txt | 1743 | 0 | 0 | 0 | 0 | 0 | 0 |
| latency/worker-1_log.txt | 1512 | 376 | 0 | 0 | 0 | 376 | 0 |
| latency/worker-2_log.txt | 822 | 204 | 0 | 0 | 0 | 204 | 0 |
| restart/api_log.txt | 363 | 0 | 0 | 0 | 0 | 0 | 0 |
| restart/worker-1_log.txt | 312 | 76 | 0 | 0 | 0 | 76 | 0 |
| restart/worker-2_after_restart_log.txt | 121 | 29 | 0 | 0 | 1 | 29 | 0 |
| restart/worker-2_before_kill_log.txt | 63 | 15 | 0 | 0 | 0 | 15 | 0 |
| retry/api_log.txt | 57 | 0 | 0 | 0 | 0 | 0 | 0 |
| retry/worker_faults0_trial0_log.txt | 9 | 1 | 0 | 0 | 0 | 1 | 0 |
| retry/worker_faults0_trial1_log.txt | 9 | 1 | 0 | 0 | 0 | 1 | 0 |
| retry/worker_faults0_trial2_log.txt | 9 | 1 | 0 | 0 | 0 | 1 | 0 |
| retry/worker_faults1_trial0_log.txt | 10 | 1 | 1 | 0 | 0 | 1 | 0 |
| retry/worker_faults1_trial1_log.txt | 10 | 1 | 1 | 0 | 0 | 1 | 0 |
| retry/worker_faults1_trial2_log.txt | 10 | 1 | 1 | 0 | 0 | 1 | 0 |
| retry/worker_faults2_trial0_log.txt | 11 | 1 | 2 | 0 | 0 | 1 | 0 |
| retry/worker_faults2_trial1_log.txt | 11 | 1 | 2 | 0 | 0 | 1 | 0 |
| retry/worker_faults2_trial2_log.txt | 11 | 1 | 2 | 0 | 0 | 1 | 0 |
| retry/worker_faults3_trial0_log.txt | 12 | 1 | 3 | 0 | 0 | 1 | 0 |
| retry/worker_faults3_trial1_log.txt | 12 | 1 | 3 | 0 | 0 | 1 | 0 |
| retry/worker_faults3_trial2_log.txt | 12 | 1 | 3 | 0 | 0 | 1 | 0 |
| retry/worker_faults4_trial0_log.txt | 10 | 0 | 3 | 1 | 0 | 1 | 1 |
| retry/worker_faults4_trial1_log.txt | 10 | 0 | 3 | 1 | 0 | 1 | 1 |
| retry/worker_faults4_trial2_log.txt | 10 | 0 | 3 | 1 | 0 | 1 | 1 |
