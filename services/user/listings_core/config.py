from dataclasses import dataclass
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class Settings:
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "airbnb"
    mysql_password: str = ""
    mysql_database: str = "airbnb"
    jwt_secret: str = ""
    jwt_issuer: str = ""
    booking_url: str = "http://127.0.0.1:8002"
    timeout_s: float = 3.0
    seed_dir: Path = ROOT / "data/seed/generated"

    @classmethod
    def from_env(cls):
        # Explicitly scoped .env file. Exported environment always wins.
        path = Path(os.environ.get("LISTINGS_ENV_FILE", ROOT / "services/user/.env"))
        if path.is_file():
            for line in path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
        return cls(mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
                   mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
                   mysql_user=os.getenv("MYSQL_USER", "airbnb"),
                   mysql_password=os.getenv("MYSQL_PASSWORD", ""),
                   mysql_database=os.getenv("MYSQL_DATABASE", "airbnb"),
                   jwt_secret=os.getenv("JWT_SECRET", ""), jwt_issuer=os.getenv("JWT_ISSUER", ""),
                   booking_url=os.getenv("BOOKING_SERVICE_URL", "http://127.0.0.1:8002").rstrip("/"),
                   timeout_s=float(os.getenv("HTTP_TIMEOUT_S", "3")),
                   seed_dir=Path(os.getenv("SEED_DIR", str(ROOT / "data/seed/generated"))))
