import logging
import asyncio
import math
import os
from typing import List
from fastapi import FastAPI, HTTPException, status, BackgroundTasks
from motor.motor_asyncio import AsyncIOMotorClient
from dmwmclient import Client as DMWMClient

# from dmwmclient.restclient import locate_proxycert
from distributed import Client as DaskClient
from .models import DatasetSource, DatasetType
from .filereader import get_file_metadata


logger = logging.getLogger(__name__)
api = FastAPI()


class Clients:
    def __init__(self):
        self.mongo = None
        self.db = None
        self.dmwm = None
        self.dask = None

    async def start(self):
        muser = os.environ["MONGODB_USERNAME"]
        mpass = os.environ["MONGODB_PASSWORD"]
        mhost = os.environ["MONGODB_HOSTNAME"]
        mdatabase = os.environ["MONGODB_DATABASE"]
        dscheduler = os.environ["DASK_SCHEDULER"]
        self.mongo = AsyncIOMotorClient(f"mongodb://{muser}:{mpass}@{mhost}")
        self.db = self.mongo[mdatabase]
        logger.info(
            "Existing collections: %r" % (await self.db.list_collection_names())
        )
        self.dmwm = DMWMClient()  # usercert=locate_proxycert())
        self.dask = await DaskClient(dscheduler, asynchronous=True)

    async def stop(self):
        self.db = None
        self.mongo = None
        self.dmwm = None
        await self.dask.close()
        self.dask = None


clients = Clients()


@api.on_event("startup")
async def startup():
    await clients.start()


@api.on_event("shutdown")
async def shutdown():
    await clients.stop()


@api.get("/")
async def root():
    return {"hello": "world"}


@api.get("/datasets")
async def get_datasets():
    return await clients.db.datasets.find().to_list(length=None)


@api.post("/datasets")
async def create_dataset(
    name: str,
    dbsname: str = None,
    fileset: List[str] = None,
    dataset_type: DatasetType = None,
    tasks: BackgroundTasks = None,
):
    """Create a new dataset

    If dbsname is set, interpret it as a DBS global dataset name
    Otherwise, interpret the fileset as an explicit list of LFNs
    """
    result = await clients.db.datasets.find_one({"name": name}, projection=[])
    if result is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Dataset already exists"
        )
    dataset = {"name": name}
    if dbsname is not None:
        dataset["source"] = DatasetSource.dbs_global
        dbsinfo = await clients.dmwm.dbs.jsonmethod(
            "datasets", dataset=dbsname, detail=True
        )
        if len(dbsinfo) != 1:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
            )
        dbsinfo = dbsinfo[0]
        dataset["dbsname"] = dbsname
        dataset["prep_id"] = dbsinfo["prep_id"]
        dataset["dataset_type"] = dbsinfo["primary_ds_type"]
        fileset = [
            f["logical_file_name"]
            for f in await clients.dmwm.dbs.jsonmethod("files", dataset=dbsname)
        ]
    elif fileset is not None:
        dataset["source"] = DatasetSource.user
        if dataset_type is None:
            raise HTTPException(
                status_code=status.HTTP_400_INVALID,
                detail="No dataset_type specified for user data",
            )
        dataset["dataset_type"] = dataset_type
    dataset["nfiles"] = len(fileset)
    await clients.db.datasets.insert_one(dataset)
    tasks.add_task(_create_fileset, fileset, dataset)
    return dataset


async def _get_dataset(name: str, just_id: bool = False):
    result = await clients.db.datasets.find_one(
        {"name": name}, projection=[] if just_id else None
    )
    if result is not None:
        return result
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
    )


@api.get("/datasets/{name}")
async def get_dataset(name: str):
    return await _get_dataset(name)


@api.delete("/datasets/{name}")
async def delete_dataset(name: str):
    dataset = await _get_dataset(name, just_id=True)
    await clients.db.datasets.delete_one({"_id": dataset["_id"]})


async def _extract_columnset(tree: dict):
    """Separate output of get_file_metadata into file-specific and common portions

    Operates in-place on the input tree"""
    columnset = {
        "name": tree["name"],  # TODO: sanitize for URL
        "hash": tree["columnset_hash"],
        "columns": tree.pop("columnset"),
        "base": None,
    }
    result = await clients.db.columnsets.find_one(
        {"hash": columnset["hash"]}, projection=[]
    )
    if result is not None:
        tree["columnset_id"] = result["_id"]
        return
    logging.info(f"Creating new columnset from file: {columnset['hash']}")
    await clients.db.columnsets.insert_one(columnset)
    tree["columnset_id"] = columnset["_id"]


async def _get_or_create_lfn(lfn: str):
    file = {"lfn": lfn}
    result = await clients.db.files.find_one(file, projection=[])
    if result is not None:
        return result
    try:
        metadata = await clients.dask.submit(get_file_metadata, lfn)
        await asyncio.gather(*[_extract_columnset(t) for t in metadata["trees"]])
        file.update(metadata)
        file["available"] = True
    except IOError as ex:
        file["available"] = False
        file["error"] = str(ex)
    await clients.db.files.insert_one(file)
    return file


async def _create_fileset(fileset: List[str], dataset: dict):
    logger.info(
        f"Starting background task _create_fileset for dataset {dataset['name']}"
    )
    fileset = await asyncio.gather(*[_get_or_create_lfn(lfn) for lfn in fileset])
    update = {"$set": {"fileset": [file["_id"] for file in fileset]}}
    logger.info(f"Finished _create_fileset for dataset {dataset['name']}")
    result = await clients.db.datasets.update_one({"_id": dataset["_id"]}, update)
    if result.modified_count != 1:
        raise RuntimeError(f"Failed to update dataset {dataset['name']} with file info")


@api.get("/datasets/{dataset_name}/files")
async def get_files(dataset_name: str):
    dataset = await _get_dataset(dataset_name)
    if "fileset" not in dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Fileset not found in dataset (likely still building)",
        )
    return await clients.db.files.find({"_id": {r"$in": dataset["fileset"]}}).to_list(
        length=None
    )


def _partition(clusters: List[int], target_size: int, max_size: int, lfn: str = None):
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


@api.get("/datasets/{dataset_name}/columnsets/{columnset_name}/partitions")
async def get_partitions(
    dataset_name: str,
    columnset_name: str,
    target_size: int = 100000,
    max_size: int = 300000,
):
    dataset = await _get_dataset(dataset_name)
    columnset = await clients.db.columnsets.find_one({"name": columnset_name})
    if columnset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Columnset not found"
        )
    base_columnset = columnset["base"]
    if base_columnset is None:
        base_columnset = columnset["_id"]
    partitions = []
    if "fileset" not in dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Fileset not found in dataset (likely still building)",
        )
    valid_files = {
        "_id": {r"$in": dataset["fileset"]},
        "available": True,
        "trees.columnset_id": base_columnset,
    }
    async for file in clients.db.files.find(valid_files):
        tree = [t for t in file["trees"] if t["columnset_id"] == base_columnset][0]
        for start, stop in _partition(
            tree["clusters"], target_size, max_size, file["lfn"]
        ):
            partitions.append(
                {
                    "lfn": file["lfn"],
                    "uuid": file["uuid"],
                    "tree_name": tree["name"],
                    "start": start,
                    "stop": stop,
                }
            )
    return partitions


@api.get("/columnsets")
async def get_columnsets():
    return await clients.db.columnsets.find().to_list(None)


@api.get("/columnsets/{columnset_name}")
async def get_columnset(columnset_name):
    return await clients.db.columnsets.find_one({"name": columnset_name})
