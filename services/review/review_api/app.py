"""Review service HTTP API (docs/api/review.md), FastAPI, port 8004.

Endpoints (all need Authorization: Bearer <JWT>):
  POST /reviews/create      guest, header Idempotency-Key required
  POST /reviews/get
  POST /reviews/byProperty
  POST /reviews/respond     host of the property
  GET  /health              no auth
Configuration (same variable names as Booking and AI):
  JWT_SECRET, JWT_ALGORITHM (HS256), JWT_ISSUER
  MONGO_URI (mongodb://localhost:27017), MONGO_DB (airbnb)
  KAFKA_BOOTSTRAP (localhost:9092)
  BOOKING_URL (http://localhost:8002)   Booking internal API
  REVIEW_STORE = mongo | memory          memory = no database (local demo)
  REVIEW_KAFKA = on | off                off = events are logged, not sent

Run (from services/review):
    uvicorn review_api.app:app --port 8004
"""
import json
import logging
import os
import re
import secrets
import threading
import time
import urllib.error
import urllib.request

import jwt
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from review_core.service import ApiError, DependencyUnavailable, ReviewService
from review_core.store import MemoryReviewStore, MongoReviewStore

log = logging.getLogger("review")
TRACE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,40}$")
FLUSH_EVERY_S = 30


def env(name, default=""):
    return os.environ.get(name, default).strip()


# ---------------------------------------------------------------- auth
def actor_from(authorization):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise ApiError(401, "UNAUTHORIZED", "Missing bearer token.")
    secret = env("JWT_SECRET")
    if not secret:
        raise ApiError(503, "DEPENDENCY_UNAVAILABLE", "JWT verification is not configured.", retryable=True)
    try:
        claims = jwt.decode(authorization.split(" ", 1)[1].strip(), secret,
                            algorithms=[env("JWT_ALGORITHM", "HS256")],
                            issuer=env("JWT_ISSUER") or None, options={"require": ["sub", "exp"]})
    except jwt.ExpiredSignatureError:
        raise ApiError(401, "UNAUTHORIZED", "Token expired.") from None
    except jwt.PyJWTError:
        raise ApiError(401, "UNAUTHORIZED", "Invalid token.") from None
    sub = str(claims["sub"])
    return {"sub": sub, "role": claims.get("role") or ("host" if sub.startswith("host-") else "guest")}


def service_token():
    now = int(time.time())
    claims = {"sub": "review-service", "role": "service", "iat": now, "exp": now + 300}
    if env("JWT_ISSUER"):
        claims["iss"] = env("JWT_ISSUER")
    return jwt.encode(claims, env("JWT_SECRET"), algorithm=env("JWT_ALGORITHM", "HS256"))


# ---------------------------------------------------------------- dependencies
class BookingClient:
    """GET /internal/reservations/{id} on the Booking service (service-role JWT)."""

    def __init__(self, base_url):
        self.base = base_url.rstrip("/")

    def get_reservation(self, reservation_id):
        req = urllib.request.Request(f"{self.base}/internal/reservations/{reservation_id}",
                                     headers={"Authorization": f"Bearer {service_token()}"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise DependencyUnavailable(f"booking returned {e.code}") from None
        except (urllib.error.URLError, TimeoutError) as e:
            raise DependencyUnavailable(str(e)) from None


class KafkaPublisher:
    def __init__(self, bootstrap):
        from confluent_kafka import Producer
        self.producer = Producer({"bootstrap.servers": bootstrap, "client.id": "review-service",
                                  "acks": "all", "enable.idempotence": True, "message.timeout.ms": 10000})

    def publish(self, topic, key, envelope):
        errors = []
        self.producer.produce(topic, key=key, value=json.dumps(envelope).encode(),
                              on_delivery=lambda err, msg: errors.append(err) if err else None)
        if self.producer.flush(10) > 0 or errors:
            raise RuntimeError(f"kafka delivery failed: {errors or 'timeout'}")


class LogPublisher:
    def publish(self, topic, key, envelope):
        log.info("event %s key=%s %s", topic, key, envelope["idempotency_key"])


def default_service():
    if env("REVIEW_STORE", "mongo") == "memory":
        store = MemoryReviewStore()
    else:
        from pymongo import MongoClient
        store = MongoReviewStore(MongoClient(env("MONGO_URI", "mongodb://localhost:27017"),
                                             serverSelectionTimeoutMS=3000)[env("MONGO_DB", "airbnb")])
        store.ensure_indexes()
    publisher = (KafkaPublisher(env("KAFKA_BOOTSTRAP", "localhost:9092"))
                 if env("REVIEW_KAFKA", "on") == "on" else LogPublisher())
    return ReviewService(store, BookingClient(env("BOOKING_URL", "http://localhost:8002")), publisher)


# ---------------------------------------------------------------- app
def create_app(service=None, background_flush=True):
    app = FastAPI(title="Review service", version="1.0")
    state = {"svc": service}

    def svc():
        if state["svc"] is None:
            state["svc"] = default_service()
        return state["svc"]

    if background_flush:
        def loop():
            while True:
                time.sleep(FLUSH_EVERY_S)
                try:
                    n = svc().flush_pending()
                    if n:
                        log.info("republished %d pending review events", n)
                except Exception as e:          # keep retrying; never crash the API
                    log.warning("flush_pending failed: %s", e)
        threading.Thread(target=loop, daemon=True).start()

    @app.middleware("http")
    async def trace(request: Request, call_next):
        incoming = request.headers.get("x-trace-id", "")
        request.state.trace_id = incoming if TRACE_RE.match(incoming) else f"tr-{secrets.token_hex(8)}"
        response = await call_next(request)
        response.headers["X-Trace-Id"] = request.state.trace_id
        return response

    @app.exception_handler(ApiError)
    async def api_error(request: Request, e: ApiError):
        return JSONResponse(e.body(request.state.trace_id), status_code=e.status)

    async def body_of(request):
        try:
            body = await request.json()
        except ValueError:
            raise ApiError(400, "VALIDATION_ERROR", "Body must be JSON.") from None
        if not isinstance(body, dict):
            raise ApiError(400, "VALIDATION_ERROR", "Body must be a JSON object.")
        return body

    @app.post("/reviews/create")
    async def create(request: Request, authorization: str | None = Header(None),
                     idempotency_key: str | None = Header(None)):
        actor = actor_from(authorization)
        status, review = svc().create(actor, await body_of(request), idempotency_key, request.state.trace_id)
        return JSONResponse(review, status_code=status)

    @app.post("/reviews/get")
    async def get(request: Request, authorization: str | None = Header(None)):
        actor_from(authorization)
        return svc().get(await body_of(request))

    @app.post("/reviews/byProperty")
    async def by_property(request: Request, authorization: str | None = Header(None)):
        actor_from(authorization)
        return svc().by_property(await body_of(request))

    @app.post("/reviews/respond")
    async def respond(request: Request, authorization: str | None = Header(None)):
        actor = actor_from(authorization)
        return svc().respond(actor, await body_of(request), request.state.trace_id)

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "review"}

    return app


app = create_app(background_flush=os.environ.get("REVIEW_BACKGROUND_FLUSH", "on") == "on")
