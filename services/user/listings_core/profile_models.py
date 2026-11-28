from typing import Annotated
from urllib.parse import urlsplit

from pydantic import EmailStr, Field, StrictBool, StrictInt, StringConstraints, field_validator, model_validator

from .models import ID, Model, PropertyType

ShortName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
Phone = Annotated[str, StringConstraints(pattern=r"^\+[1-9][0-9]{1,14}$")]
Photo = Annotated[str, StringConstraints(max_length=2048)]
Amenity = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,99}$")]


class Preferences(Model):
    property_types: list[PropertyType] = Field(default_factory=list, max_length=5)
    required_amenities: list[Amenity] = Field(default_factory=list, max_length=50)
    max_nightly_price: Annotated[float, Field(gt=0, le=99999999.99, strict=True, allow_inf_nan=False)] | None = None

    @model_validator(mode="after")
    def unique_lists(self):
        if len(self.property_types) != len(set(self.property_types)) or len(self.required_amenities) != len(set(self.required_amenities)):
            raise ValueError("Preference lists must contain unique values")
        return self


class EditableProfile(Model):
    email: EmailStr | None = None
    phone: Phone | None = None
    profile_photo_url: Photo | None = None

    @field_validator("email")
    @classmethod
    def bounded_email(cls, value):
        if value is not None and len(value) > 254:
            raise ValueError("Email is at most 254 characters")
        return str(value) if value is not None else None

    @field_validator("profile_photo_url")
    @classmethod
    def https_photo(cls, value):
        if value is None:
            return value
        url = urlsplit(value)
        _ = url.port  # Reject invalid or out-of-range port numbers.
        if url.scheme != "https" or not url.hostname or url.username or url.password or any(c.isspace() for c in value):
            raise ValueError("Photo must be an HTTPS URL without embedded credentials")
        return value

    @model_validator(mode="after")
    def required_edits(self):
        supplied = self.model_fields_set - {"guest_id", "host_id"}
        if not supplied:
            raise ValueError("Provide at least one editable field")
        for field in supplied - {"phone", "location", "profile_photo_url", "preferences"}:
            if getattr(self, field) is None:
                raise ValueError(field + " cannot be null")
        return self


class GuestUpdate(EditableProfile):
    guest_id: ID
    first_name: ShortName | None = None
    last_name: ShortName | None = None
    location: Annotated[str, StringConstraints(strip_whitespace=True, max_length=200)] | None = None
    preferences: Preferences | None = None


class HostUpdate(EditableProfile):
    host_id: ID
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=150)] | None = None


class SaveProperty(Model):
    property_id: ID
    saved: StrictBool


class GuestSearch(Model):
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)] | None = None
    email: EmailStr | None = None
    location: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)] | None = None
    page: Annotated[StrictInt, Field(ge=1, le=1000000)] = 1
    page_size: Annotated[StrictInt, Field(ge=1, le=50)] = 20
