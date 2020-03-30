import asyncio
import logging
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dmwmclient.restclient import locate_proxycert
from dmwmclient import Client as DMWMClient
from distributed import Client as DaskClient
from minio import Minio


logger = logging.getLogger(__name__)


class Services:
    def __init__(self):
        self.mongo = None
        self.db = None
        self.dmwm = None
        self.dask = None
        self.minio = None
        self._executor = None

    async def run_pool(self, func, *args):
        return await asyncio.get_event_loop().run_in_executor(
            self._executor, func, *args
        )

    async def start_mongo(self):
        muser = os.environ["MONGODB_USERNAME"]
        mpass = os.environ["MONGODB_PASSWORD"]
        mhost = os.environ["MONGODB_HOSTNAME"]
        mdatabase = os.environ["MONGODB_DATABASE"]
        mconn = f"mongodb://{muser}:{mpass}@{mhost}/{mdatabase}"
        # await asyncio.sleep(2)  # race with mongo?
        self.mongo = AsyncIOMotorClient(mconn)
        self.db = self.mongo[mdatabase]
        logger.info(
            "Existing collections: %r" % (await self.db.list_collection_names())
        )
        # TODO: make collections? 'datasets', 'files', 'columnsets', 'generators'

    async def start_dmwm(self):
        self.dmwm = DMWMClient(usercert=locate_proxycert())

    async def start_dask(self):
        self.dask = await DaskClient(os.environ["DASK_SCHEDULER"], asynchronous=True)

    async def start_minio(self):
        self.minio = Minio(
            endpoint=os.environ["MINIO_HOSTNAME"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )
        bucket = os.environ["COLUMNSERVICE_BUCKET"]
        if not await self.run_pool(self.minio.bucket_exists, bucket):
            await self.run_pool(self.minio.make_bucket, bucket)

    async def start(self):
        await asyncio.gather(
            self.start_mongo(), self.start_dmwm(), self.start_dask(), self.start_minio()
        )

    async def stop(self):
        self.db = None
        self.mongo = None
        self.dmwm = None
        await self.dask.close()
        self.dask = None
        self.minio = None


services = Services()
