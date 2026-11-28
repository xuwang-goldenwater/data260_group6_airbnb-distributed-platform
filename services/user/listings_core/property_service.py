from .http import ApiError


class PropertyService:
    def __init__(self, repository, booking, pool):
        self.repo, self.booking, self.pool = repository, booking, pool

    def get(self, property_id, actor=None):
        prop = self.repo.property(property_id)
        owns = bool(prop and actor and (actor.role in {"admin", "service"}
                    or actor.role == "host" and actor.sub == prop["host_id"]))
        if prop is None or prop["status"] != "active" and not owns:
            raise ApiError(404, "PROPERTY_NOT_FOUND", "The requested property does not exist.")
        return prop

    def validate_amenities(self, filters):
        if filters.required_amenities and not set(filters.required_amenities) <= self.repo.amenities():
            raise ApiError(400, "VALIDATION_ERROR", "Unknown required amenity.")

    def availability(self, body, trace_id):
        prop = self.get(body.property_id)
        if body.guest_count > prop["capacity"]:
            raise ApiError(422, "CAPACITY_EXCEEDED", "Guest count exceeds property capacity.")
        free, checked = self.booking.check([body.property_id], body.check_in, body.check_out, trace_id)
        return {"property_id": body.property_id, "check_in": body.check_in.isoformat(),
                "check_out": body.check_out.isoformat(), "guest_count": body.guest_count,
                "available": free[body.property_id], "checked_datetime": checked,
                "source": "booking_service"}

    def search(self, body, trace_id):
        self.validate_amenities(body)
        rows = self.repo.search(body)
        checked = None
        if body.check_in:
            free, checked = self.booking.check([p["property_id"] for p in rows], body.check_in,
                                               body.check_out, trace_id)
            rows = [dict(p, available=free[p["property_id"]]) for p in rows]
            if body.available_only:
                rows = [p for p in rows if p["available"]]
        else:
            rows = [dict(p, available=None) for p in rows]
        if body.sort == "price_asc":
            rows.sort(key=lambda p: (p["nightly_price"], p["property_id"]))
        elif body.sort == "price_desc":
            rows.sort(key=lambda p: (-p["nightly_price"], p["property_id"]))
        elif body.sort in {"rating_desc", "relevance"}:
            rows.sort(key=lambda p: (-p["rating"], p["property_id"]))
        else:
            rows.sort(key=lambda p: p["property_id"])
        start = (body.page - 1) * body.page_size
        return {"items": rows[start:start + body.page_size], "page": body.page,
                "page_size": body.page_size, "total": len(rows), "sort": body.sort,
                "availability_checked_datetime": checked}

    def candidates(self, body, trace_id):
        filters = body.filters
        self.validate_amenities(filters)
        rows = self.pool.current(self.repo, body)
        selected = []
        for p in rows:
            if p["status"] != "active" or p["capacity"] < filters.guest_count or p["bedrooms"] < filters.minimum_bedrooms:
                continue
            if filters.location and p["location"]["city"].casefold() != filters.location.casefold():
                continue
            if p["nightly_price"] < filters.minimum_nightly_price:
                continue
            if filters.maximum_nightly_price is not None and p["nightly_price"] > filters.maximum_nightly_price:
                continue
            if filters.property_types and p["property_type"] not in filters.property_types:
                continue
            if not set(filters.required_amenities) <= set(p["amenities"]):
                continue
            # Benchmark's declared convention: no reviews means unknown, not low rating.
            if filters.minimum_rating is not None and p["review_count"] and p["rating"] < filters.minimum_rating:
                continue
            selected.append(p)
        selected.sort(key=lambda p: p["property_id"])
        free, checked = self.booking.check([p["property_id"] for p in selected], filters.check_in,
                                           filters.check_out, trace_id)
        selected = [dict(p, available=True) for p in selected if free[p["property_id"]]]
        return {"pool_id": body.pool_id, "dataset_version": body.dataset_version,
                "selection_rule": body.selection_rule,
                "candidate_property_ids": [p["property_id"] for p in selected],
                "properties": selected, "availability_checked_datetime": checked}
