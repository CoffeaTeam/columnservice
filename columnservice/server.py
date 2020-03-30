import logging
import os
from fastapi import FastAPI

from .components import datasets, columnsets, generators
from .components.services import services


logger = logging.getLogger(__name__)


app = FastAPI()
app.include_router(datasets.router)
app.include_router(columnsets.router)
app.include_router(generators.router)


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
    return {
        "storage": {
            # "type": "filesystem",
            # "args": {"path": "/Users/ncsmith/storage"},
            "type": "minio",
            "bucket": os.environ["COLUMNSERVICE_BUCKET"],
            "args": {
                "endpoint": os.environ["MINIO_HOSTNAME"],
                "access_key": os.environ["MINIO_ACCESS_KEY"],
                "secret_key": os.environ["MINIO_SECRET_KEY"],
                "secure": False,
            },
        },
        "file_catalog": [
            {"algo": "prefix", "prefix": "root://coffea@cmsxrootd-site.fnal.gov/"},
            {"algo": "prefix", "prefix": "root://coffea@cmsxrootd.fnal.gov/"},
            {"algo": "prefix", "prefix": "root://coffea@cms-xrd-global.cern.ch/"},
        ],
        "xrootdsource_metadata": {
            "timeout": 20,
            "chunkbytes": 32 * 1024,
            "limitbytes": 1024 ** 2,
            "parallel": False,
        },
        "xrootdsource": {
            "timeout": 60,
            "chunkbytes": 512 * 1024,
            "limitbytes": 10 * 1024 ** 2,
            "parallel": False,
        },
    }
