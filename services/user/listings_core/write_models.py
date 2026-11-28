from typing import Annotated, Literal

from pydantic import Field, StrictInt, StringConstraints, model_validator

from .models import ID, Model, PropertyType

Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]
Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=5, max_length=150)]
Description = Annotated[str, StringConstraints(min_length=20, max_length=5000)]
Capacity = Annotated[StrictInt, Field(ge=1, le=30)]
Bedrooms = Annotated[StrictInt, Field(ge=0, le=20)]
Bathrooms = Annotated[float, Field(ge=0.5, le=20, allow_inf_nan=False, strict=True, multiple_of=0.01)]
Price = Annotated[float, Field(gt=0, le=99999999.99, allow_inf_nan=False, strict=True, multiple_of=0.01)]
Fee = Annotated[float, Field(ge=0, le=99999999.99, allow_inf_nan=False, strict=True, multiple_of=0.01)]
Amenities = Annotated[list[Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,99}$")]], Field(max_length=50)]
Version = Annotated[StrictInt, Field(ge=1, le=9223372036854775806)]


class Location(Model):
    address_line_1: Text
    city: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
    state: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
    postal_code: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=32)]
    country: Annotated[str, StringConstraints(pattern=r"^[A-Z]{2}$")]
    latitude: Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False, strict=True, multiple_of=0.000001)]
    longitude: Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False, strict=True, multiple_of=0.000001)]


class CreateProperty(Model):
    title: Title
    description: Description
    property_type: PropertyType
    location: Location
    capacity: Capacity
    bedrooms: Bedrooms
    bathrooms: Bathrooms
    amenities: Amenities = Field(default_factory=list)
    nightly_price: Price
    cleaning_fee: Fee = 0.0

    @model_validator(mode="after")
    def unique_amenities(self):
        if len(self.amenities) != len(set(self.amenities)):
            raise ValueError("Amenities must be unique")
        return self


class Changes(Model):
    title: Title | None = None
    description: Description | None = None
    property_type: PropertyType | None = None
    location: Location | None = None
    capacity: Capacity | None = None
    bedrooms: Bedrooms | None = None
    bathrooms: Bathrooms | None = None
    amenities: Amenities | None = None
    nightly_price: Price | None = None
    cleaning_fee: Fee | None = None
    availability: Literal["available", "partially_available", "unavailable"] | None = None
    status: Literal["draft", "active", "suspended", "archived"] | None = None

    @model_validator(mode="before")
    @classmethod
    def present_values(self, value):
        if not isinstance(value, dict) or not value or any(v is None for v in value.values()):
            raise ValueError("Provide at least one change; null values are not allowed")
        amenities = value.get("amenities")
        if isinstance(amenities, list) and all(isinstance(a, str) for a in amenities) and len(amenities) != len(set(amenities)):
            raise ValueError("Amenities must be unique")
        return value


class UpdateProperty(Model):
    property_id: ID
    expected_version: Version
    changes: Changes


class ArchiveProperty(Model):
    property_id: ID
    expected_version: Version
