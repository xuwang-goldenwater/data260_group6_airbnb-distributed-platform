"""Measure two real Property processes: cache enabled vs disabled. Never invent results."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
import time

import httpx


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cached-url", default="http://127.0.0.1:8003")
    p.add_argument("--uncached-url", default="http://127.0.0.1:8013")
    p.add_argument("--requests", type=int, default=100)
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--output", default="reports/perf/property/search_cache.json")
    args = p.parse_args()
    if not 1 <= args.concurrency <= 50 or not 10 <= args.requests <= 10000:
        p.error("Use concurrency 1-50 and requests 10-10000")
    body = {"location": "San Jose", "guest_count": 2, "required_amenities": ["kitchen", "parking"],
            "maximum_nightly_price": 250.0, "sort": "price_asc", "page_size": 50}
    report = {"timestamp": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
              "concurrency": args.concurrency, "requests_per_mode": args.requests, "filters": body,
              "scope": "Undated public listing search; HTTP latency including validation, SQL revision check and cache work. Booking latency is excluded.",
              "modes": {}}
    reference = None
    # Alternate order in two rounds to reduce simple first/second mode bias.
    aggregate = {"enabled": {"times": [], "outcomes": Counter(), "elapsed": 0.0},
                 "disabled": {"times": [], "outcomes": Counter(), "elapsed": 0.0}}
    with httpx.Client(timeout=20, limits=httpx.Limits(max_connections=50, max_keepalive_connections=50)) as client:
        for url, expected in ((args.cached_url, "HIT"), (args.uncached_url, "BYPASS")):
            for _ in range(3):
                r = client.post(url + "/properties/search", json=body); r.raise_for_status()
            if r.headers.get("x-search-cache") != expected:
                raise ValueError("Check that the two URLs run enabled/disabled cache modes respectively")
            if reference is None: reference = r.json()
            elif reference != r.json(): raise ValueError("Modes returned different property results")
        def measure(url):
            started = time.perf_counter()
            r = client.post(url + "/properties/search", json=body)
            elapsed = (time.perf_counter() - started) * 1000
            r.raise_for_status()
            if r.json() != reference: raise ValueError("Dataset changed during measurement; rerun in a quiet environment")
            return elapsed, r.headers.get("x-search-cache", "UNKNOWN")
        for round_no, count in enumerate((args.requests // 2, args.requests - args.requests // 2)):
            order = [("enabled", args.cached_url), ("disabled", args.uncached_url)]
            if round_no: order.reverse()
            for name, url in order:
                start = time.perf_counter()
                with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                    results = list(pool.map(lambda _: measure(url), range(count)))
                aggregate[name]["elapsed"] += time.perf_counter() - start
                aggregate[name]["times"].extend(v[0] for v in results)
                aggregate[name]["outcomes"].update(v[1] for v in results)
    for name, values in aggregate.items():
        times = sorted(values["times"])
        report["modes"][name] = {"count": len(times), "latency_ms_mean": sum(times) / len(times),
                                 "latency_ms_p50": times[math.ceil(.50*len(times))-1],
                                 "latency_ms_p95": times[math.ceil(.95*len(times))-1],
                                 "latency_ms_p99": times[math.ceil(.99*len(times))-1],
                                 "throughput_rps": len(times) / values["elapsed"],
                                 "cache_outcomes": dict(values["outcomes"]), "latencies_ms": times}
    output = Path(args.output); output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": "MEASURED", "output": str(output),
                      "modes": {k: {f:v for f,v in d.items() if f != "latencies_ms"} for k,d in report["modes"].items()}}, indent=2))


if __name__ == "__main__":
    main()
