"""Create Role A's ignored local config; reuse Booking JWT settings when present."""
import os
from pathlib import Path
import secrets
import sys

ROOT = Path(__file__).resolve().parents[3]
target = ROOT / "services/user/.env"
booking = ROOT / "services/booking-api/.env"


def load(path):
    values = {}
    if path.is_file():
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("\"'")
    return values


if target.exists():
    if not (os.getenv("JWT_SECRET") or load(target).get("JWT_SECRET")):
        raise SystemExit("Existing services/user/.env has no JWT_SECRET. Set it without replacing other settings.")
    print("Existing services/user/.env retained.")
    sys.exit(0)

values = load(ROOT / "services/user/.env.example")
existing = load(booking)
keys = ("MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE", "JWT_SECRET", "JWT_ISSUER")
for key in keys:
    if key in os.environ:
        values[key] = os.environ[key]
    elif key in existing:
        values[key] = existing[key]
values["JWT_SECRET"] = values.get("JWT_SECRET") or secrets.token_hex(32)
if any("\n" in v or "\r" in v for v in values.values()):
    raise SystemExit("Local environment values must be single-line strings")
with target.open("x") as file:
    file.write("# Local-only Role A configuration. Never commit this file.\n")
    file.write("\n".join(key + "=" + value for key, value in values.items()) + "\n")
target.chmod(0o600)
print("Created services/user/.env; no secrets printed.")
print("Before live Booking integration, ensure Booking and AI use the same JWT_SECRET and JWT_ISSUER.")
