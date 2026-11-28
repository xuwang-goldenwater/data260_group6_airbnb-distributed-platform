from typing import Annotated

from fastapi import Depends, Path, Request
from fastapi.responses import JSONResponse

from listings_core.config import Settings
from listings_core.http import Actor, ApiError, authenticate, make_app, require_profile_access, unavailable
from listings_core.models import GuestID, HostID
from listings_core.repository import MySQLRepository
from listings_core.profile_models import GuestUpdate, HostUpdate, GuestSearch
from listings_core.profile_service import ProfileService
from listings_core.write_clients import WriteDependencies


def create_app(settings=None, repository=None, write_dependencies=None):
    cfg = settings or Settings.from_env()
    repo = repository if repository is not None else MySQLRepository(cfg)
    app = make_app("User Service — Role A")
    profiles = ProfileService(repo, write_dependencies or WriteDependencies(cfg))

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

    def response(result):
        payload, replay = result
        return JSONResponse(payload, headers={"Idempotent-Replay": "true" if replay else "false"})

    @app.post("/guests/update")
    def update_guest(body: GuestUpdate, request: Request, actor: Actor = Depends(current)):
        return response(profiles.update("guest", body, actor, request.headers.get("idempotency-key"), request.state.trace_id))

    @app.post("/hosts/update")
    def update_host(body: HostUpdate, request: Request, actor: Actor = Depends(current)):
        return response(profiles.update("host", body, actor, request.headers.get("idempotency-key"), request.state.trace_id))

    @app.post("/guests/delete")
    def delete_guest(body: GuestID, request: Request, actor: Actor = Depends(current)):
        return response(profiles.delete_guest(body, actor, request.headers.get("idempotency-key"), request.state.trace_id))

    @app.post("/guests/search")
    def search_guests(body: GuestSearch, actor: Actor = Depends(current)):
        return profiles.search(body, actor)

    return app


app = create_app()
