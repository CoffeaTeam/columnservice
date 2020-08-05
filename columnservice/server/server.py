import logging
from fastapi import FastAPI

from columnservice.version import __version__
from columnservice.server import (
    auth,
    datasets,
    columnsets,
    generators,
    files,
    config,
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
    return config.client_config


@app.get("/version")
async def version():
    return __version__
