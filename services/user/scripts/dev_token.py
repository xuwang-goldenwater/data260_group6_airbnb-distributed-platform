"""Local development helper; this is not a public login/token-issuing endpoint."""
import argparse
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import jwt
from listings_core.config import Settings

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("subject")
parser.add_argument("--role", choices=["guest", "host", "admin", "service"], default="guest")
args = parser.parse_args()
settings = Settings.from_env()
if not settings.jwt_secret:
    raise SystemExit("Set JWT_SECRET in the environment or services/user/.env first")
claims = {"sub": args.subject, "role": args.role, "iat": int(time.time()), "exp": int(time.time()) + 3600}
if settings.jwt_issuer:
    claims["iss"] = settings.jwt_issuer
print(jwt.encode(claims, settings.jwt_secret, algorithm="HS256"))
