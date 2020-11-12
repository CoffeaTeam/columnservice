import logging
import hashlib
import json
from typing import List, Dict, Optional
from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from pymongo import ReturnDocument
from .common import generic_http_error, DBModel, ObjectIdStr
from .services import services


logger = logging.getLogger(__name__)


class ColumnSet(DBModel):
    name: str
    base: Optional[ObjectIdStr] = ...
    columns: Dict


async def extract_columnset(tree: dict):
    """Separate tree metadata into file-specific and common portions

    Operates in-place on the input tree, which should be a dictionary
    with structure as produced in columnservice.client.get_file_metadata
    """
    columns = tree.pop("base_form")
    columnset = {
        "name": tree["name"]
        + "-"
        + tree["form_hash"][:7],  # TODO: sanitize name for URL?
        "hash": tree["form_hash"],
        "columns": columns,
        "base": None,
    }
    out = await services.db.columnsets.find_one_and_update(
        columnset,
        {"$unset": {"noop": None}},
        {},  # return only _id
        return_document=ReturnDocument.AFTER,
        upsert=True,
    )
    tree["columnset_id"] = out["_id"]


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
    columnset = await services.group_query(
        f"/columnsets/{name}", services.db.columnsets.find_one, {"name": name}
    )
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
    # TODO: handle concurrent calls: upsert? or fail and let client retry?
    result = await services.db.columnsets.find_one(
        {"name": columnset.name}, projection=[]
    )
    if result is not None:
        raise HTTPException(
            status_code=HTTP_409_CONFLICT, detail="Columnset already exists"
        )
    columnset = columnset.dict()
    columnset["hash"] = hashlib.sha256(
        json.dumps({"columns": columnset["columns"]}).encode()
    ).hexdigest()
    await services.db.columnsets.insert_one(columnset)
    return columnset


@router.delete("/columnsets/{name}")
async def delete_columnset(name: str):
    await services.db.columnsets.delete_one({"name": name})
