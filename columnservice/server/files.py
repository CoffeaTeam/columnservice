import asyncio
import logging
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_400_BAD_REQUEST
from .services import services
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from ..client import get_file_metadata
from .common import ObjectIdStr, DBModel


logger = logging.getLogger(__name__)


class Tree(BaseModel):
    name: str
    numentries: int
    clusters: List[int]
    columnset_id: Optional[ObjectIdStr] = ...
    columnset_hash: str


class File(DBModel):
    lfn: str
    available: bool
    error: str = None
    uuid: str = None
    trees: List[Tree]


router = APIRouter()


async def _extract_columnset(tree: dict):
    """Separate output of get_file_metadata into file-specific and common portions

    Operates in-place on the input tree"""
    columns = tree.pop("columnset")
    columnset = {
        "name": tree["name"]
        + "-"
        + tree["columnset_hash"][:7],  # TODO: sanitize name for URL?
        "hash": tree["columnset_hash"],
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


async def _update_file_metadata(file):
    try:
        metadata = await services.dask.submit(get_file_metadata, file["lfn"])
    except Exception as ex:
        file["available"] = False
        file["error"] = str(ex)
        return
    await asyncio.gather(*[_extract_columnset(t) for t in metadata["trees"]])
    file.update(metadata)
    file["available"] = True


@router.post("/files", response_model=File)
async def create_lfn(lfn: str):
    file = await services.db.files.find_one({"lfn": lfn})
    if file is None:
        file = {"lfn": lfn}
        await _update_file_metadata(file)
        await services.db.files.insert_one(file)
    elif not file["available"]:
        await _update_file_metadata(file)
        if file["available"]:
            update = {
                "$set": {
                    "available": True,
                    "error": None,
                    "uuid": file["uuid"],
                    "trees": file["trees"],
                }
            }
            result = await services.db.files.update_one({"_id": file["_id"]}, update)
            if result.modified_count != 1:
                raise HTTPException(
                    HTTP_400_BAD_REQUEST, f"Failed to update file {file['lfn']}"
                )
    return file
