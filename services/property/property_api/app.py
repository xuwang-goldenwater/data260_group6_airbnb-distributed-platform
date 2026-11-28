from typing import Annotated

from fastapi import Depends, Path, Request
from fastapi.responses import JSONResponse

from listings_core.booking import BookingClient
from listings_core.config import Settings
from listings_core.http import Actor, authenticate, make_app, unavailable
from listings_core.models import Availability, Candidates, PropertyID, Search
from listings_core.pool import FrozenPool
from listings_core.property_service import PropertyService
from listings_core.repository import MySQLRepository
from listings_core.property_writes import PropertyWrites
from listings_core.write_clients import WriteDependencies
from listings_core.write_models import CreateProperty, UpdateProperty, ArchiveProperty


def create_app(settings=None, repository=None, booking=None, write_dependencies=None):
    cfg = settings or Settings.from_env()
    repo = repository if repository is not None else MySQLRepository(cfg)
    availability = booking if booking is not None else BookingClient(cfg)
    service = PropertyService(repo, availability, FrozenPool(cfg.seed_dir))
    writes = PropertyWrites(repo, write_dependencies or WriteDependencies(cfg))
    app = make_app("Property Service — Role A")

    def optional_actor(request: Request):
        return authenticate(request, cfg, optional=True)

    def machine(request: Request):
        return authenticate(request, cfg, service_only=True)

    def host_actor(request: Request):
        return authenticate(request, cfg)

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "property"}

    @app.get("/ready")
    def ready():
        if not cfg.jwt_secret:
            raise unavailable("JWT verification configuration")
        if not repo.ping():
            raise unavailable("MySQL")
        return {"status": "ready", "database": "mysql"}

    @app.post("/properties/get")
    def get(body: PropertyID, actor: Actor | None = Depends(optional_actor)):
        return service.get(body.property_id, actor)

    @app.post("/properties/search")
    def search(body: Search, request: Request, actor: Actor | None = Depends(optional_actor)):
        return service.search(body, request.state.trace_id)

    @app.post("/properties/availability")
    def check(body: Availability, request: Request, actor: Actor | None = Depends(optional_actor)):
        return service.availability(body, request.state.trace_id)

    def apply_write(operation, body, request, actor):
        result, replay = writes.execute(operation, body, actor, request.headers.get("idempotency-key"), request.state.trace_id)
        return JSONResponse(result, status_code=201 if operation == "create" and not replay else 200,
                            headers={"Idempotent-Replay": "true" if replay else "false"})

    @app.post("/properties/create")
    def create(body: CreateProperty, request: Request, actor: Actor = Depends(host_actor)):
        return apply_write("create", body, request, actor)

    @app.post("/properties/update")
    def update(body: UpdateProperty, request: Request, actor: Actor = Depends(host_actor)):
        return apply_write("update", body, request, actor)

    @app.post("/properties/delete")
    def archive(body: ArchiveProperty, request: Request, actor: Actor = Depends(host_actor)):
        return apply_write("delete", body, request, actor)

    @app.post("/internal/properties/candidates")
    def candidates(body: Candidates, request: Request, actor: Actor = Depends(machine)):
        return service.candidates(body, request.state.trace_id)

    @app.get("/internal/hosts/{host_id}/properties")
    def by_host(host_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")],
                actor: Actor = Depends(machine)):
        rows = repo.properties("p.host_id = %s", (host_id,))
        return {"items": rows, "total": len(rows)}

    return app


app = create_app()
