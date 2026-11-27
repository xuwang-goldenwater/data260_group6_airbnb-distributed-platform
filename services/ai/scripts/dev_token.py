"""Print a short-lived JWT for local demos (uses the same JWT_SECRET as the running AI API).

    JWT_SECRET=dev-secret-change-me-0123456789abcdef python services/ai/scripts/dev_token.py guest-demo guest
    JWT_SECRET=dev-secret-change-me-0123456789abcdef python services/ai/scripts/dev_token.py agent-risk agent     # -> 403 on approve
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_core.auth import make_token  # noqa: E402
from ai_core.config import settings  # noqa: E402

if __name__ == "__main__":
    sub = sys.argv[1] if len(sys.argv) > 1 else "guest-demo"
    role = sys.argv[2] if len(sys.argv) > 2 else "guest"
    s = settings()
    if not s.jwt_secret:
        sys.exit("set JWT_SECRET first (the same value the AI API uses)")
    print(make_token(sub, role, s))
