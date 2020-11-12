import asyncio
import logging
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dmwmclient.restclient import locate_proxycert
from dmwmclient import Client as DMWMClient
from distributed import Client as DaskClient
from distributed.security import Security
from minio import Minio
from columnservice.client.filemanager import FileManager
from columnservice.server import config


logger = logging.getLogger(__name__)


class Services:
    def __init__(self):
        self.mongo = None
        self.db = None
        self.dmwm = None
        self.dask = None
        self.minio = None
        self._executor = None
        self._queries = {}
        self.filemanager = FileManager(config.filemanager)

    async def run_pool(self, func, *args):
        return await asyncio.get_event_loop().run_in_executor(
            self._executor, func, *args
        )

    async def group_query(self, key, func, *args, **kwargs):
        """Group common pennding queries together

        func should be a callable that returns a coroutine
        """
        try:
            return await self._queries[key]
        except KeyError:
            self._queries[key] = asyncio.ensure_future(func(*args, **kwargs))
            await self._queries[key]
            return await self._queries.pop(key)

    async def start_mongo(self):
        muser = os.environ["MONGODB_USERNAME"]
        mpass = os.environ["MONGODB_PASSWORD"]
        mhost = os.environ["MONGODB_HOSTNAME"]
        mdatabase = os.environ["MONGODB_DATABASE"]
        mconn = f"mongodb://{muser}:{mpass}@{mhost}/{mdatabase}"
        # await asyncio.sleep(2)  # race with mongo?
        self.mongo = AsyncIOMotorClient(mconn)
        self.db = self.mongo[mdatabase]
        collections = await self.db.list_collection_names()
        logger.info("Existing collections: %r" % collections)
        if "columnsets" not in collections:
            await self.db.columnsets.create_index("hash", unique=True)
        # if "files" not in collections:
        #     await self.db.files.create_index("uuid", unique=True)
        # TODO: make collections? 'datasets', 'files', 'columnsets', 'generators'

    async def start_dmwm(self):
        self.dmwm = DMWMClient(usercert=locate_proxycert())

    async def start_dask(self):
        tls_path = os.environ.get("TLS_PATH", None)
        if tls_path is None:
            self.dask = await DaskClient(
                os.environ["DASK_SCHEDULER"], asynchronous=True
            )
        else:
            sec = Security(
                tls_ca_file=os.path.join(tls_path, "ca.crt"),
                tls_client_cert=os.path.join(tls_path, "hostcert.pem"),
                require_encryption=True,
            )
            self.dask = await DaskClient(
                os.environ["DASK_SCHEDULER"], asynchronous=True, security=sec
            )

    async def start_minio(self):
        self.minio = Minio(
            endpoint=os.environ["MINIO_HOSTNAME"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )
        bucket = os.environ["COLUMNSERVICE_BUCKET"]
        if await self.run_pool(self.minio.bucket_exists, bucket):
            logger.info(f"Existing bucket: {bucket}")
        else:
            logger.info(f"Creating new bucket: {bucket}")
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
