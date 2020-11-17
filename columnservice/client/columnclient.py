import os
import logging
import httpx
import distributed
import distributed.security
import dask.delayed
import dask.bag
from itertools import accumulate
from columnservice.client.mapping import setup_mapping
from columnservice.client.filemanager import FileManager
from coffea.nanoevents import NanoEventsFactory
from coffea.nanoevents.mapping import CachedMapping, UprootSourceMapping
from coffea.nanoevents.util import tuple_to_key
from columnservice.client.daskawkwardarray import DaskAwkwardArray


logger = logging.getLogger(__name__)


class Dataset:
    def __init__(self, name, cc):
        self._name = name
        self._cc = cc

    @property
    def name(self):
        return self._name

    @property
    def columnsets(self):
        result = self._cc.api.get(f"/datasets/{self.name}/columnsets")
        if result.status_code != 200:
            detail = result.json()["detail"]
            raise RuntimeError(f"{result.status_code}: {detail}")
        return {cs: Columnset(cs, self._cc) for cs in result.json()}

    def _partitions(self, columnset, schemaclass, limit):
        if isinstance(columnset, str):
            columnset = Columnset(columnset, self._cc)
        elif not isinstance(columnset, Columnset):
            raise ValueError(
                f"columnset should be a string or Columnset type, not {columnset}"
            )
        result = self._cc.api.get(
            f"/datasets/{self.name}/columnsets/{columnset.name}/partitions",
            params={"limit": limit},
        )
        if result.status_code != 200:
            detail = result.json()["detail"]
            raise RuntimeError(f"{result.status_code}: {detail}")
        schema = schemaclass(columnset.form)
        parts = result.json()

        # TODO reduce on server side
        parts = [
            [
                part["uuid"],
                part["tree_name"],
                part["start"],
                part["stop"],
            ]
            for part in parts
        ]

        def builder(item):
            return Partition(
                {
                    "uuid": item[0],
                    "tree_name": item[1],
                    "start": item[2],
                    "stop": item[3],
                },
                schema,
                self._cc,
            ).events()

        return parts, builder

    def iter_partitions(self, columnset, schemaclass, limit=None):
        parts, builder = self._partitions(columnset, schemaclass, limit)
        return map(builder, parts)

    def bag(self, columnset, schemaclass, limit=None):
        parts, builder = self._partitions(columnset, schemaclass, limit)
        return dask.bag.from_delayed(map(dask.delayed(builder), parts))

    def daskarray(self, columnset, schemaclass, limit=None):
        parts, builder = self._partitions(columnset, schemaclass, limit)
        offsets = [0].extend(accumulate((part[3] - part[2] for part in parts)))
        return DaskAwkwardArray.from_partitions(parts, builder, offsets)


class Columnset:
    def __init__(self, name, cc):
        self._name = name
        self._cc = cc

    @property
    def name(self):
        return self._name

    @property
    def form(self):
        result = self._cc.api.get(f"/columnsets/{self.name}")
        if result.status_code != 200:
            detail = result.json()["detail"]
            raise RuntimeError(f"{result.status_code}: {detail}")
        return result.json()["columns"]


class Partition:
    def __init__(self, data, schema, cc):
        self._data = data
        self._schema = schema
        self._cc = cc

    def events(self, runtime_cache=None):
        mapping = CachedMapping(self._cc.storage, UprootSourceMapping(self._cc))
        partition_tuple = (
            self._data["uuid"],
            self._data["tree_name"],
            "{0}-{1}".format(self._data["start"], self._data["stop"]),
        )
        factory = NanoEventsFactory(
            self._schema, mapping, tuple_to_key(partition_tuple), cache=runtime_cache
        )
        return factory.events()


class ColumnClient:
    def __init__(self, server_hostname, server_port=80):
        self._hostname = server_hostname
        self._port = server_port
        self._config = self.api.get("/clientconfig").json()

    def __getstate__(self):
        return {"hostname": self.hostname, "port": self.port, "config": self._config}

    def __setstate__(self, state):
        self._hostname = state["hostname"]
        self._port = state["port"]
        self._config = state["config"]

    @property
    def hostname(self):
        return self._hostname

    @property
    def port(self):
        return self._port

    @property
    def config(self):
        return self._config

    @property
    def api(self):
        if not hasattr(self, "_api"):
            self._api = httpx.Client(
                base_url=f"http://{self.hostname}:{self.port}",
                # TODO: timeout/retry
            )
        return self._api

    @property
    def storage(self):
        if not hasattr(self, "_storage"):
            self._storage = setup_mapping(self.config["storage"])
        return self._storage

    @property
    def filemanager(self):
        if not hasattr(self, "_filemanager"):
            self._filemanager = FileManager(self.config["filemanager"])
        return self._filemanager

    def get_dask(
        self,
        ca_path="dask_ca.crt",
        client_cert_path="dask_client_cert.pem",
        hostname=None,
        port=8786,
    ):
        with open(ca_path, "w") as fout:
            fout.write(self.config["tls_ca"])
        userproxy_path = os.environ.get(
            "X509_USER_PROXY", "/tmp/x509up_u%d" % os.getuid()
        )
        with open(userproxy_path, "rb") as fin:
            userproxy = fin.read()
        result = self.api.post("/clientkey", data={"proxycert": userproxy})
        if result.status_code == 401:
            raise RuntimeError("Authorization denied while retrieving dask certificate")
        elif result.status_code != 200:
            raise RuntimeError("Error while retrieving dask certificate")
        with open(client_cert_path, "w") as fout:
            fout.write(result.text)
        sec = distributed.security.Security(
            tls_ca_file=ca_path,
            tls_client_cert=client_cert_path,
            require_encryption=True,
        )
        if hostname is None:
            hostname = self.hostname
        url = f"tls://{hostname}:{port}"
        return distributed.Client(url, security=sec)

    def register_dataset(self, name, pathexpr, source="dbs-global", type="mc"):
        data = {
            "name": name,
            "source": source,
            "type": type,
            "pathexpr": pathexpr,
        }
        result = self.api.post("/datasets", json=data)
        if result.status_code != 202:
            detail = result.json()["detail"]
            raise RuntimeError(f"{result.status_code}: {detail}")
        return Dataset(name, self)

    def get_dataset(self, name):
        return Dataset(name, self)

    def open_uuid(self, uuid):
        result = self.api.get("/files/lfn", params={"uuid": uuid})
        if result.status_code != 200:
            detail = result.json()["detail"]
            raise RuntimeError(f"{result.status_code}: {detail}")
        lfn = result.json()
        return self.filemanager.open_file(lfn)
