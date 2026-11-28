"""Make `booking_core`, `booking_api` and `booking_worker` importable when running scripts directly."""
import sys
from pathlib import Path

_SERVICES = Path(__file__).resolve().parents[2]
for _p in (_SERVICES.parent, _SERVICES / "booking-api", _SERVICES / "booking-worker"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
