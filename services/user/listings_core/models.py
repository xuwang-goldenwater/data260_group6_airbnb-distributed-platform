from datetime import date
import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StringConstraints, field_validator, model_validator

ID = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")]
Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Money = Annotated[float, Field(ge=0, le=99999999.99, allow_inf_nan=False, strict=True)]
PropertyType = Literal["apartment", "house", "condo", "cabin", "room"]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GuestID(Model):
    guest_id: ID


class HostID(Model):
    host_id: ID


class PropertyID(Model):
    property_id: ID


class DateRange(Model):
    check_in: date | None = None
    check_out: date | None = None

    @field_validator("check_in", "check_out", mode="before")
    @classmethod
    def iso_date(cls, value):
        if value is not None and (not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)):
            raise ValueError("Use a YYYY-MM-DD date string")
        return value

    @model_validator(mode="after")
    def date_pair(self):
        if (self.check_in is None) != (self.check_out is None):
            raise ValueError("Provide check_in and check_out together")
        if self.check_in and not 0 < (self.check_out - self.check_in).days <= 365:
            raise ValueError("Stay must be 1–365 nights with checkout after check-in")
        return self


class Filters(DateRange):
    location: Name | None = None
    minimum_nightly_price: Money = 0
    maximum_nightly_price: Money | None = None
    property_types: list[PropertyType] = Field(default_factory=list, max_length=5)
    guest_count: Annotated[StrictInt, Field(ge=1, le=30)] = 1
    minimum_bedrooms: Annotated[StrictInt, Field(ge=0, le=20)] = 0
    required_amenities: list[Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,99}$")]] = Field(default_factory=list, max_length=50)
    minimum_rating: Annotated[float, Field(ge=0, le=5, allow_inf_nan=False, strict=True)] | None = None

    @model_validator(mode="after")
    def filter_bounds(self):
        if self.maximum_nightly_price is not None and self.maximum_nightly_price < self.minimum_nightly_price:
            raise ValueError("Maximum nightly price must be at least the minimum")
        self.property_types = list(dict.fromkeys(self.property_types))
        self.required_amenities = list(dict.fromkeys(self.required_amenities))
        return self


class Search(Filters):
    available_only: StrictBool = False
    sort: Literal["relevance", "price_asc", "price_desc", "rating_desc", "property_id_asc"] = "property_id_asc"
    page: Annotated[StrictInt, Field(ge=1, le=1000000)] = 1
    page_size: Annotated[StrictInt, Field(ge=1, le=50)] = 20

    @model_validator(mode="after")
    def availability_needs_dates(self):
        if self.available_only and not self.check_in:
            raise ValueError("available_only requires both dates")
        return self


class Availability(DateRange):
    property_id: ID
    guest_count: Annotated[StrictInt, Field(ge=1, le=30)] = 1

    @model_validator(mode="after")
    def required_dates(self):
        if not self.check_in:
            raise ValueError("Both dates are required")
        return self


class Candidates(Model):
    pool_id: Annotated[str, Field(min_length=1, max_length=100)]
    dataset_version: Annotated[str, Field(min_length=1, max_length=100)]
    selection_rule: Literal["deterministic_filter_then_property_id"]
    filters: Filters

    @model_validator(mode="after")
    def required_dates(self):
        if not self.filters.check_in:
            raise ValueError("Candidate selection requires both dates")
        return self
