import asyncio
import math
import logging
from enum import Enum
from typing import List, Union
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, BackgroundTasks
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from ..common import generic_http_error, DBModel
from .services import services
from .columnsets import ColumnSet, get_columnset
from .files import File, create_lfn


logger = logging.getLogger(__name__)


class DatasetSource(str, Enum):
    dbs_global = "dbs-global"
    # dbs_phys03 = "dbs-phys03"
    user = "user"


class DatasetType(str, Enum):
    data = "data"
    mc = "mc"


class Dataset(DBModel):
    name: str
    source: DatasetSource
    type: DatasetType
    nfiles: int
    pathexpr: str = None
    prep_id: str = None


class NewDataset(BaseModel):
    name: str
    source: DatasetSource
    type: DatasetType
    pathexpr: Union[str, List[str]]


class Partition(BaseModel):
    lfn: str
    uuid: str
    tree_name: str
    start: int
    stop: int
    columnset: str


async def index_files(dataset, fileset: List[str]):
    logger.info(f"Starting index_files for dataset {dataset['name']}")
    fileset = await asyncio.gather(*[create_lfn(lfn) for lfn in fileset])
    update = {
        "$set": {
            "fileset": [file["_id"] for file in fileset],
            "columnsets": list(
                set(tree["columnset_id"] for file in fileset for tree in file["trees"])
            ),
        }
    }
    result = await services.db.datasets.update_one({"_id": dataset["_id"]}, update)
    if result.modified_count != 1:
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Failed to update dataset {dataset['name']} with file info",
        )
    logger.info(f"Finished index_files for dataset {dataset['name']}")


def partition(clusters: List[int], target_size: int, max_size: int, lfn: str = None):
    start, stop = 0, 1
    while start < stop < len(clusters):
        while (
            stop < len(clusters) - 1 and clusters[stop] - clusters[start] < target_size
        ):
            stop += 1
        size = clusters[stop] - clusters[start]
        if size > max_size:
            logger.warning(
                f"Large cluster found in LFN {lfn} size {size} start {clusters[start]} stop {clusters[stop]}"  # noqa
            )
            n = max(round(size / target_size), 1)
            step = math.ceil(size / n)
            for index in range(n):
                i, j = step * index, min(size, step * (index + 1))
                yield (clusters[start] + i, clusters[start] + j)
            assert clusters[start] + j == clusters[stop]
        else:
            yield (clusters[start], clusters[stop])
        start, stop = stop, stop + 1


router = APIRouter()


@router.get("/datasets", response_model=List[Dataset])
async def get_datasets():
    return await services.db.datasets.find().to_list(length=None)


@router.get(
    "/datasets/{name}", response_model=Dataset, responses={404: generic_http_error}
)
async def get_dataset(name: str):
    """Retrieve dataset from database

    Raises HTTPException if no dataset exists"""
    result = await services.db.datasets.find_one({"name": name})
    if result is None:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Dataset not found")
    return result


@router.post(
    "/datasets",
    response_model=Dataset,
    responses={404: generic_http_error, 400: generic_http_error},
)
async def create_dataset(new_dataset: NewDataset, tasks: BackgroundTasks = None):
    """Create a new dataset"""
    result = await services.db.datasets.find_one(
        {"name": new_dataset.name}, projection=[]
    )
    if result is not None:
        raise HTTPException(
            status_code=HTTP_409_CONFLICT, detail="Dataset already exists"
        )
    if new_dataset.source == DatasetSource.dbs_global:
        if type(new_dataset.pathexpr) is not str:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="DBS-derived datasets should not enumerate the files",
            )
        dbsinfo = await services.dmwm.dbs.jsonmethod(
            "datasets", dataset=new_dataset.pathexpr, detail=True
        )
        if len(dbsinfo) != 1:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND, detail="Dataset not found"
            )
        dbsinfo = dbsinfo[0]
        if dbsinfo["primary_ds_type"] != new_dataset.type:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Mismatch between specified dataset type and DBS dataset type",
            )
        fileset = [
            f["logical_file_name"]
            for f in await services.dmwm.dbs.jsonmethod(
                "files", dataset=new_dataset.pathexpr
            )
        ]
        dataset = Dataset(
            name=new_dataset.name,
            source=new_dataset.source,
            type=new_dataset.type,
            nfiles=len(fileset),
            pathexpr=new_dataset.pathexpr,
            prep_id=dbsinfo["prep_id"],
        )
    elif new_dataset.source == DatasetSource.user:
        if type(new_dataset.pathexpr) is str or any(
            "*" in path for path in new_dataset.pathexpr
        ):
            # glob?
            raise NotImplementedError
        fileset = new_dataset.pathexpr
        dataset = Dataset(
            name=new_dataset.name,
            source=new_dataset.source,
            type=new_dataset.type,
            nfiles=len(fileset),
        )
    dataset = dataset.dict()
    await services.db.datasets.insert_one(dataset)
    tasks.add_task(index_files, dataset, fileset)
    return dataset


@router.delete("/datasets/{name}")
async def delete_dataset(name: str):
    await services.db.datasets.delete_one({"name": name})


@router.get("/datasets/{dataset_name}/files", response_model=List[File])
async def get_files(dataset_name: str):
    dataset = await get_dataset(dataset_name)
    if "fileset" not in dataset:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail="Fileset not found in dataset (likely still building)",
        )
    return await services.db.files.find({"_id": {r"$in": dataset["fileset"]}}).to_list(
        length=None
    )


@router.put(
    "/datasets/{dataset_name}/files",
    responses={400: generic_http_error, 404: generic_http_error},
)
async def update_dataset_files(dataset_name: str, fileset: List[str]):
    dataset = await get_dataset(dataset_name)
    await index_files(dataset, fileset)


@router.get("/datasets/{dataset_name}/columnsets", response_model=List[ColumnSet])
async def get_dataset_columnsets(dataset_name: str):
    dataset = await get_dataset(dataset_name)
    out = await services.db.columnsets.find(
        {"_id": {r"$in": dataset["columnsets"]}}
    ).to_list(length=None)
    for columnset in out:
        if columnset["base"] is None:
            columnset["base"] = columnset["_id"]
    return out


@router.get(
    "/datasets/{dataset_name}/columnsets/{columnset_name}/partitions",
    response_model=List[Partition],
    responses={404: generic_http_error},
)
async def get_partitions(
    dataset_name: str,
    columnset_name: str,
    target_size: int = 100000,
    max_size: int = 300000,
    limit: int = None,
):
    dataset, columnset = await asyncio.gather(
        get_dataset(dataset_name), get_columnset(columnset_name)
    )
    partitions = []
    if "fileset" not in dataset:
        raise HTTPException(
            status_code=HTTP_404_NOT_FOUND,
            detail="Fileset not found in dataset (likely still building)",
        )
    valid_files = {
        "_id": {r"$in": dataset["fileset"]},
        "available": True,
        "trees.columnset_id": columnset["base"],
    }
    async for file in services.db.files.find(valid_files):
        tree = [t for t in file["trees"] if t["columnset_id"] == columnset["base"]][0]
        for start, stop in partition(
            tree["clusters"], target_size, max_size, file["lfn"]
        ):
            partitions.append(
                {
                    "lfn": file["lfn"],
                    "uuid": file["uuid"],
                    "tree_name": tree["name"],
                    "start": start,
                    "stop": stop,
                    "columnset": columnset_name,
                }
            )
            if limit is not None and len(partitions) == limit:
                return partitions
    return partitions
