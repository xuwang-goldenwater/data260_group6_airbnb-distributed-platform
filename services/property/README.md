# Property Service (Role A)

Setup, endpoint contracts, JWT configuration, tests, smoke checks and current
scope are in [`../user/README.md`](../user/README.md).

From the repository root:

```bash
PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn property_api.app:app --host 127.0.0.1 --port 8003
```

MySQL supplies listing/profile data; Booking's HTTP API supplies date availability.
The service does not regenerate seed, reserve dates, or silently fall back to JSON.

## Rating projection

Consume `review.created` to maintain rating/count safely. See
[`RATINGS.md`](RATINGS.md) for baseline initialization, tests and worker commands.

## Search cache

Optional Redis listing-search caching and the separate event invalidator are
documented in [`CACHE.md`](CACHE.md), including live checks and measurements.
