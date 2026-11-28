from typing import Annotated

from fastapi import Depends, Path, Request

from listings_core.config import Settings
from listings_core.http import Actor, ApiError, authenticate, make_app, require_profile_access, unavailable
from listings_core.models import GuestID, HostID
from listings_core.repository import MySQLRepository


def create_app(settings=None, repository=None):
    cfg = settings or Settings.from_env()
    repo = repository if repository is not None else MySQLRepository(cfg)
    app = make_app("User Service — Role A")

    def current(request: Request):
        return authenticate(request, cfg)

    def machine(request: Request):
        return authenticate(request, cfg, service_only=True)

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "user"}

    @app.get("/ready")
    def ready():
        if not cfg.jwt_secret:
            raise unavailable("JWT verification configuration")
        if not repo.ping():
            raise unavailable("MySQL")
        return {"status": "ready", "database": "mysql"}

    @app.post("/guests/get")
    def guest(body: GuestID, actor: Actor = Depends(current)):
        require_profile_access(actor, body.guest_id, "guest")
        result = repo.guest(body.guest_id)
        if result is None:
            raise ApiError(404, "GUEST_NOT_FOUND", "The requested guest does not exist.")
        return result

    @app.post("/hosts/get")
    def host(body: HostID, actor: Actor = Depends(current)):
        require_profile_access(actor, body.host_id, "host")
        result = repo.host(body.host_id)
        if result is None:
            raise ApiError(404, "HOST_NOT_FOUND", "The requested host does not exist.")
        return result

    @app.get("/internal/guests/{guest_id}")
    def internal_guest(guest_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")],
                       actor: Actor = Depends(machine)):
        if repo.guest(guest_id) is None:
            raise ApiError(404, "GUEST_NOT_FOUND", "The requested guest does not exist.")
        return {"guest_id": guest_id, "exists": True, "active": True}

    return app


app = create_app()
