import hashlib
import json

from .http import ApiError, unavailable


class FrozenPool:
    """Seed files specify pool identity only; response values always come from MySQL."""
    def __init__(self, directory):
        self.directory = directory

    def load(self):
        try:
            manifest = json.loads((self.directory / "manifest.json").read_bytes())
            raw = (self.directory / "properties.json").read_bytes()
            if hashlib.sha256(raw).hexdigest() != manifest["sha256"]["properties.json"]:
                raise ValueError("Pool checksum mismatch")
            records = json.loads(raw)
            properties = {p["property_id"]: p for p in records}
            if len(properties) != len(records) or len(records) != manifest["counts"]["properties"]:
                raise ValueError("Pool count mismatch")
            return manifest, properties
        except (OSError, ValueError, KeyError, TypeError):
            raise unavailable("Frozen benchmark pool metadata") from None

    def current(self, repo, request):
        manifest, frozen = self.load()
        if request.pool_id != manifest["pool_id"] or request.dataset_version != manifest["dataset_version"]:
            raise ApiError(409, "DATASET_VERSION_CONFLICT", "Use the current manifest pool_id and dataset_version.")
        rows = repo.pool(sorted(frozen))
        if len(rows) != len(frozen):
            raise ApiError(409, "DATASET_VERSION_CONFLICT", "MySQL is missing frozen-pool properties.")
        for row in rows:
            expected = dict(frozen[row["property_id"]])
            expected["amenities"] = sorted(expected["amenities"])
            if row != expected:
                raise ApiError(409, "DATASET_VERSION_CONFLICT", "MySQL listings differ from the frozen benchmark snapshot.")
        return rows
