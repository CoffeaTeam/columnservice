import logging
from typing import List, Dict, Union, Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from ..common import generic_http_error, DBModel, ObjectIdStr
from .services import services


logger = logging.getLogger(__name__)


class Column(BaseModel):
    name: str
    dtype: str
    dimension: int
    doc: str
    generator: str
    packoptions: Dict[str, Union[int, str]]
    """See https://python-blosc.readthedocs.io/en/latest/reference.html#blosc.pack_array"""  # noqa


class ColumnSet(DBModel):
    name: str
    base: Optional[ObjectIdStr] = ...
    columns: List[Column]


router = APIRouter()


@router.get("/columnsets", response_model=List[ColumnSet])
async def get_columnsets():
    out = await services.db.columnsets.find().to_list(length=None)
    for columnset in out:
        if columnset["base"] is None:
            columnset["base"] = columnset["_id"]
    return out


@router.get(
    "/columnsets/{name}", response_model=ColumnSet, responses={404: generic_http_error}
)
async def get_columnset(name: str):
    """Retrieve columnset from database

    Raises HTTPException if no columnset exists"""
    columnset = await services.db.columnsets.find_one({"name": name})
    if columnset is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND, detail="Columnset not found"
        )
    if columnset["base"] is None:
        columnset["base"] = columnset["_id"]
    return columnset


@router.post(
    "/columnsets", response_model=ColumnSet, responses={409: generic_http_error}
)
async def create_columnset(columnset: ColumnSet):
    result = await services.db.columnsets.find_one(
        {"name": columnset.name}, projection=[]
    )
    if result is not None:
        raise HTTPException(
            status_code=HTTP_409_CONFLICT, detail="Columnset already exists"
        )
    columnset = columnset.dict()
    await services.db.columnsets.insert_one(columnset)
    return columnset


@router.delete("/columnsets/{name}")
async def delete_columnset(name: str):
    await services.db.columnsets.delete_one({"name": name})
