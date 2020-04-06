from typing import Optional
from pydantic import BaseModel
from bson import ObjectId


class GenericHTTPError(BaseModel):
    detail: str


class GenericAccpted(BaseModel):
    detail: str


class ObjectIdStr(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(str(v)):
            return ValueError(f"Not a valid ObjectId: {v}")
        return ObjectId(str(v))


class DBModel(BaseModel):
    _id: Optional[ObjectIdStr] = ...

    class Config:
        json_encoders = {ObjectId: lambda x: str(x)}


generic_accepted = {"description": "Request accepted", "model": GenericAccpted}
generic_http_error = {"description": "Application error", "model": GenericHTTPError}
