"""Booking API (Role B). Run: uvicorn booking_api.app:app --port 8002 (see services/booking-api/README.md)."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import pymysql
from fastapi import Depends, FastAPI, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from booking_core import ids
from booking_core.auth import Actor, decode_token, optional_user, require_service, require_user
from booking_core.clients import AnalyticsClient, UserClient, make_property_client
from booking_core.config import Settings
from booking_core.db import Database
from booking_core.errors import ApiError, DependencyUnavailable
from booking_core.kafka_producer import EventProducer, KafkaEventProducer
from booking_core.outbox import OutboxPublisher
from booking_core.service import BookingService

from .ratelimit import RateLimiter
from .ws import Client, ConnectionManager, StatusConsumer

log = logging.getLogger("booking.api")
_TRACE_RE = re.compile(r"^[A-Za-z0-9._:-]{1,40}$")


async def json_body(request: Request) -> Any:
    """Parse the JSON body regardless of Content-Type; empty body → None; malformed → 400 VALIDATION_ERROR."""
    raw = await request.body()
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except ValueError:
        raise ApiError(400, "VALIDATION_ERROR", "Malformed JSON body.") from None


def _configure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [booking-api] %(name)s %(message)s")


class CompletionJob(threading.Thread):
    def __init__(self, svc: BookingService, interval_s: float):
        super().__init__(name="completion-job", daemon=True)
        self._svc, self._interval = svc, interval_s
        self._stop_evt = threading.Event()

    def run(self) -> None:
        while not self._stop_evt.wait(self._interval):
            try:
                self._svc.complete_stays()
            except Exception:
                log.exception("completion job failed")

    def stop(self) -> None:
        self._stop_evt.set()


def create_app(settings: Settings | None = None, *, db: Database | None = None,
               producer: EventProducer | None = None, property_client=None, start_background: bool = True) -> FastAPI:
    _configure_logging()
    settings = settings or Settings.from_env()
    if not settings.jwt_secret:
        raise RuntimeError("JWT_SECRET is not set (copy services/booking-api/.env.example to .env)")
    manager = ConnectionManager()
    state: dict[str, Any] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        _db = db or Database(settings)
        _producer = producer or KafkaEventProducer(settings, client_id="booking-api",
                                                   spread_partitions=settings.test_spread_partitions)
        svc = BookingService(settings, _db, _producer, property_client or make_property_client(settings),
                             UserClient(settings), AnalyticsClient(settings))
        state.update(svc=svc, db=_db, producer=_producer, limiter=RateLimiter(settings.rate_limit_per_minute))
        manager.loop = asyncio.get_running_loop()
        threads = []
        if start_background:
            threads.append(OutboxPublisher(_db, _producer, interval_s=settings.outbox_interval_s,
                                           min_age_s=settings.outbox_min_age_s))
            if settings.completion_interval_s > 0:
                threads.append(CompletionJob(svc, settings.completion_interval_s))
            if settings.ws_enabled:
                threads.append(StatusConsumer(settings, manager, _producer))
        for t in threads:
            t.start()
        log.info("booking-api ready precheck=%s property_source=%s spread_partitions=%s",
                 settings.precheck_enabled, settings.property_source, settings.test_spread_partitions)
        yield
        for t in threads:
            t.stop()
        for t in threads:
            t.join(timeout=5)   # lets the gateway consumer leave its group cleanly (no rebalance wait on restart)
        if producer is None:
            _producer.close()

    app = FastAPI(title="Booking & Availability API (Role B)", version="1.0", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
                       expose_headers=["X-Trace-Id"])
    app.state.booking = state
    app.state.ws_manager = manager

    @app.middleware("http")
    async def trace_middleware(request: Request, call_next):
        incoming = request.headers.get("x-trace-id", "")
        request.state.trace_id = incoming if _TRACE_RE.match(incoming) else ids.trace_id()
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Trace-Id"] = request.state.trace_id
        if request.url.path.startswith(("/bookings", "/dashboards", "/internal")):
            log.info("%s %s -> %s %.1fms trace_id=%s", request.method, request.url.path, response.status_code,
                     (time.perf_counter() - started) * 1000, request.state.trace_id)
        return response

    def _trace(request: Request) -> str:
        return getattr(request.state, "trace_id", None) or ids.trace_id()

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError):
        return JSONResponse(exc.body(_trace(request)), status_code=exc.status, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def bad_request(request: Request, exc: RequestValidationError):
        err = ApiError(400, "VALIDATION_ERROR", "Malformed JSON body.",
                       details={"errors": [e.get("msg") for e in exc.errors()][:5]})
        return JSONResponse(err.body(_trace(request)), status_code=400)

    @app.exception_handler(DependencyUnavailable)
    async def dep_unavailable(request: Request, exc: DependencyUnavailable):
        err = ApiError(503, "DEPENDENCY_UNAVAILABLE", f"{exc.dependency} is unavailable.", retryable=True)
        return JSONResponse(err.body(_trace(request)), status_code=503)

    @app.exception_handler(pymysql.err.OperationalError)
    @app.exception_handler(pymysql.err.InterfaceError)
    async def mysql_down(request: Request, exc: Exception):
        log.warning("mysql error: %s", exc)
        err = ApiError(503, "DEPENDENCY_UNAVAILABLE", "mysql is unavailable.", retryable=True)
        return JSONResponse(err.body(_trace(request)), status_code=503)

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        log.exception("unhandled error trace_id=%s", _trace(request))
        err = ApiError(500, "INTERNAL_ERROR", "Unexpected error.", retryable=False)
        return JSONResponse(err.body(_trace(request)), status_code=500)

    def svc() -> BookingService:
        return state["svc"]

    def user(authorization: str | None) -> Actor:
        return require_user(settings, authorization)

    def respond(result) -> JSONResponse:
        return JSONResponse(result.body, status_code=result.status,
                            headers={"Idempotent-Replayed": "true"} if result.replayed else None)

    # ------------------------------------------------------------------------- bookings
    @app.post("/bookings/create", status_code=202)
    def create_booking(request: Request, body: Any = Depends(json_body), authorization: str | None = Header(None),
                       idempotency_key: str | None = Header(None)):
        actor = user(authorization)
        state["limiter"].check(actor.sub)
        return respond(svc().create(actor, body, idempotency_key, _trace(request)))

    @app.post("/bookings/get")
    def get_booking(body: Any = Depends(json_body), authorization: str | None = Header(None)):
        return svc().get(user(authorization), body)

    @app.post("/bookings/byGuest")
    def by_guest(body: Any = Depends(json_body), authorization: str | None = Header(None)):
        return svc().by_guest(user(authorization), body)

    @app.post("/bookings/byHost")
    def by_host(body: Any = Depends(json_body), authorization: str | None = Header(None)):
        return svc().by_host(user(authorization), body)

    @app.post("/bookings/approve")
    def approve(request: Request, body: Any = Depends(json_body), authorization: str | None = Header(None),
                idempotency_key: str | None = Header(None)):
        return respond(svc().approve(user(authorization), body, idempotency_key, _trace(request)))

    @app.post("/bookings/decline")
    def decline(request: Request, body: Any = Depends(json_body), authorization: str | None = Header(None),
                idempotency_key: str | None = Header(None)):
        return respond(svc().decline(user(authorization), body, idempotency_key, _trace(request)))

    @app.post("/bookings/cancel")
    def cancel(request: Request, body: Any = Depends(json_body), authorization: str | None = Header(None),
               idempotency_key: str | None = Header(None)):
        return respond(svc().cancel(user(authorization), body, idempotency_key, _trace(request)))

    @app.post("/bookings/calendar")
    def calendar(body: Any = Depends(json_body), authorization: str | None = Header(None)):
        optional_user(settings, authorization)   # public read; a bad token is still rejected
        return svc().calendar(body)

    # ------------------------------------------------------------------------- internal (service auth)
    def service(authorization: str | None, x_service_token: str | None) -> None:
        require_service(settings, authorization, x_service_token)

    @app.get("/internal/reservations/{reservation_id}")
    def internal_reservation(reservation_id: str, authorization: str | None = Header(None),
                             x_service_token: str | None = Header(None)):
        service(authorization, x_service_token)
        return svc().internal_reservation(reservation_id)

    @app.post("/internal/availability/check")
    def internal_availability(body: Any = Depends(json_body), authorization: str | None = Header(None),
                              x_service_token: str | None = Header(None)):
        service(authorization, x_service_token)
        return svc().internal_availability(body)

    @app.get("/internal/properties/{property_id}/active-reservations")
    def internal_property_active(property_id: str, authorization: str | None = Header(None),
                                 x_service_token: str | None = Header(None)):
        service(authorization, x_service_token)
        return svc().internal_active("property_id", property_id)

    @app.get("/internal/guests/{guest_id}/active-reservations")
    def internal_guest_active(guest_id: str, authorization: str | None = Header(None),
                              x_service_token: str | None = Header(None)):
        service(authorization, x_service_token)
        return svc().internal_active("guest_id", guest_id)

    # ------------------------------------------------------------------------- WebSocket
    @app.websocket("/bookings/stream")
    async def stream(ws: WebSocket):
        # Browsers cannot set headers on a WebSocket, so `?token=` is accepted as well as Authorization.
        auth = ws.headers.get("authorization")
        token = auth.split(" ", 1)[1].strip() if auth and auth.lower().startswith("bearer ") else ws.query_params.get("token")
        try:
            if not token:
                raise ApiError(401, "UNAUTHORIZED", "Missing bearer token.")
            actor = decode_token(settings, token)
        except ApiError:
            await ws.close(code=4401)
            return
        await ws.accept()
        client = Client(ws=ws, sub=actor.sub)
        manager.add(client)
        await ws.send_text(json.dumps({"type": "connected", "sub": actor.sub}))
        try:
            while True:
                raw = await ws.receive_text()
                try:
                    msg = json.loads(raw)
                except ValueError:
                    await ws.send_text(json.dumps({"type": "error", "code": "VALIDATION_ERROR"}))
                    continue
                pid = msg.get("property_id") if isinstance(msg, dict) else None
                action = msg.get("action") if isinstance(msg, dict) else None
                if action in ("subscribe_calendar", "unsubscribe_calendar") and isinstance(pid, str) and pid:
                    (client.calendars.add if action == "subscribe_calendar" else client.calendars.discard)(pid)
                    await ws.send_text(json.dumps({"type": "ack", "action": action, "property_id": pid}))
                else:
                    await ws.send_text(json.dumps({"type": "error", "code": "VALIDATION_ERROR",
                                                   "message": "Unknown action."}))
        except WebSocketDisconnect:
            pass
        finally:
            manager.remove(client)

    # ------------------------------------------------------------------------- ops / dev
    @app.get("/health")
    def health():
        ok = True
        try:
            state["db"].ping()
        except Exception:
            ok = False
        return JSONResponse({"status": "ok" if ok else "degraded", "mysql": ok, "ws_clients": manager.count,
                             "ws_pushed": manager.pushed}, status_code=200 if ok else 503)

    return app


def __getattr__(name: str):
    # `uvicorn booking_api.app:app` builds the app lazily so importing this module in tests has no side effects.
    if name == "app":
        globals()["app"] = create_app()
        return globals()["app"]
    raise AttributeError(name)
