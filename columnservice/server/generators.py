import logging
from typing import List, Dict
from fastapi import APIRouter, HTTPException
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from .common import generic_http_error, DBModel
from .services import services


logger = logging.getLogger(__name__)


class ColumnGenerator(DBModel):
    name: str
    function_key: str
    input_columns: List[Dict]
    available: bool = True


router = APIRouter()


@router.get("/generators", response_model=List[ColumnGenerator])
async def get_generators():
    return await services.db.generators.find().to_list(length=None)


@router.get(
    "/generators/{name}",
    response_model=ColumnGenerator,
    responses={404: generic_http_error},
)
async def get_generator(name: str):
    """Retrieve generator from database

    Raises HTTPException if no generator exists"""
    generator = await services.group_query(
        f"/generators/{name}", services.db.generators.find_one, {"name": name}
    )
    if generator is None:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND, detail="Generator not found"
        )
    return generator


@router.post(
    "/generators", response_model=ColumnGenerator, responses={409: generic_http_error}
)
async def create_generator(generator: ColumnGenerator):
    result = await services.db.generators.find_one(
        {"name": generator.name}, projection=[]
    )
    if result is not None:
        raise HTTPException(
            status_code=HTTP_409_CONFLICT, detail="Generator already exists"
        )
    generator = generator.dict()
    await services.db.generators.insert_one(generator)
    return generator


@router.delete("/generators/{name}")
async def delete_generator(name: str):
    await services.db.generators.delete_one({"name": name})
