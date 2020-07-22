import logging
import os
from fastapi import FastAPI

from columnservice.server import (
    auth,
    datasets,
    columnsets,
    generators,
    files,
)
from columnservice.server.services import services


logger = logging.getLogger(__name__)


app = FastAPI()
app.include_router(auth.router)
app.include_router(datasets.router)
app.include_router(columnsets.router)
app.include_router(generators.router)
app.include_router(files.router)


@app.on_event("startup")
async def startup():
    await services.start()


@app.on_event("shutdown")
async def shutdown():
    await services.stop()


@app.get("/")
async def root():
    return {"hello": "world"}


@app.get("/clientconfig")
async def get_config():
    # TODO: specify return type?
    if "FILESTORE" in os.environ:
        storage = {
            "type": "filesystem",
            "args": {"path": os.environ["FILESTORE"]},
        }
    else:
        storage = {
            "type": "minio-buffered",
            "buffersize": int(1e7),
            "bucket": os.environ["COLUMNSERVICE_BUCKET"],
            "args": {
                "endpoint": os.environ["MINIO_HOSTNAME"],
                "access_key": os.environ["MINIO_ACCESS_KEY"],
                "secret_key": os.environ["MINIO_SECRET_KEY"],
                "secure": False,
            },
        }

    return {
        "storage": storage,
        "file_catalog": [
            {"algo": "prefix", "prefix": "root://coffea@cmsxrootd-site.fnal.gov/"},
            {"algo": "prefix", "prefix": "root://coffea@cmsxrootd.fnal.gov/"},
            {"algo": "prefix", "prefix": "root://coffea@cms-xrd-global.cern.ch/"},
        ],
        "xrootdsource_metadata": {
            "timeout": 10,
            "chunkbytes": 32 * 1024,
            "limitbytes": 1024 ** 2,
            "parallel": False,
        },
        "xrootdsource": {
            "timeout": 60,
            "chunkbytes": 65536,
            "limitbytes": 16 * 1024 ** 2,
            "parallel": False,
        },
    }
