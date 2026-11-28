from datetime import timezone
import uuid

import jsonschema

from .config import ROOT

SCHEMA = None


def make_event(topic, prop, actor, trace_id, now, changed_fields):
    global SCHEMA
    if SCHEMA is None:
        import json
        SCHEMA = json.loads((ROOT / "shared/envelope.schema.json").read_text())
    stamp = now.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    payload = {"property_id": prop["property_id"], "host_id": prop["host_id"],
               "version": prop["version"], "changed_fields": sorted(changed_fields),
               "updated_datetime": stamp}
    event = {"event_type": topic, "trace_id": trace_id, "timestamp": stamp,
             "actor_id": actor.sub, "entity": {"entity_type": "property", "entity_id": prop["property_id"]},
             "payload": payload, "idempotency_key": f"{topic}:{prop['property_id']}:{prop['version']}",
             "attempt": 1, "event_id": "ev-" + uuid.uuid4().hex,
             "producer": "property-service", "schema_version": "1.0"}
    jsonschema.Draft202012Validator(SCHEMA, format_checker=jsonschema.FormatChecker()).validate(event)
    return event
