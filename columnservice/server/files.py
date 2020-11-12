import asyncio
import logging
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND
from .services import services
from .common import ObjectIdStr, DBModel, generic_http_error
from .columnsets import extract_columnset


logger = logging.getLogger(__name__)


class Tree(BaseModel):
    name: str
    num_entries: int
    common_entry_offsets: List[int]
    columnset_id: Optional[ObjectIdStr] = ...


class File(DBModel):
    lfn: str
    available: bool
    error: str = None
    uuid: str = None
    trees: List[Tree] = None


router = APIRouter()


async def _update_file_metadata(file):
    try:
        metadata = await services.dask.submit(
            services.filemanager.get_file_metadata, file["lfn"]
        )
    except Exception as ex:
        file["available"] = False
        file["error"] = str(ex)
        logger.error(f"Exception while updating metadata for {file}: {ex}")
        return
    await asyncio.gather(*[extract_columnset(t) for t in metadata["trees"]])
    file.update(metadata)
    file["available"] = True


@router.post("/files", response_model=File, responses={400: generic_http_error})
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


@router.get("/files/lfn", responses={404: generic_http_error})
async def get_lfn(uuid: str):
    file = await services.db.files.find_one({"uuid": uuid})
    if file is None or not file["available"]:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Failed to find file for uuid {uuid}")
    return file["lfn"]
