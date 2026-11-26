from ulid import ULID


def reservation_id() -> str:
    return f"res-{ULID()}"


def trace_id() -> str:
    return f"tr-{ULID()}"


def event_id() -> str:
    return f"ev-{ULID()}"
