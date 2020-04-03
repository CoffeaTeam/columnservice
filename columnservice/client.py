"""Client for columnservice API"""
import logging
import hashlib
import json
import os
from functools import lru_cache
from threading import Lock
from collections.abc import Mapping, MutableMapping
import numpy
import uproot
import httpx
import awkward
from minio.error import NoSuchKey
from io import BytesIO
import blosc
import cloudpickle
import lz4.frame as lz4f
from cachetools import LRUCache


def _default_server():
    try:
        return os.environ["COLUMNSERVICE_URL"]
    except KeyError:
        pass
    return "http://localhost:8000"


logger = logging.getLogger(__name__)


class FilesystemMutableMapping(MutableMapping):
    def __init__(self, path):
        """(ab)use a filesystem as a mutable mapping"""
        self._path = path

    def __getitem__(self, key):
        try:
            with open(os.path.join(self._path, key), "rb") as fin:
                return fin.read()
        except FileNotFoundError:
            raise KeyError

    def __setitem__(self, key, value):
        path = os.path.join(self._path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fout:
            fout.write(value)

    def __delitem__(self, key):
        os.remove(os.path.join(self._path, key))

    def __iter__(self):
        raise NotImplementedError("Too lazy to recursively ls")

    def __len__(self):
        raise NotImplementedError("No performant way to get directory count")


class ReadBuffer(MutableMapping):
    def __init__(self, cache, base):
        """Buffers reads from base via cache.

        Assumes cache is not threadsafe, and base is threadsafe.
        No cacheing is done on write, assuming caller already has what they need
        """
        self.lock = Lock()
        self.cache = cache
        self.base = base

    def __getitem__(self, key):
        try:
            with self.lock:
                return self.cache[key]
        except KeyError:
            out = self.base[key]
            with self.lock:
                self.cache[key] = out
            return out

    def __setitem__(self, key, value):
        self.base[key] = value
        with self.lock:
            self.cache.pop(key, None)

    def __delitem__(self, key):
        del self.base[key]
        with self.lock:
            self.cache.pop(key, None)

    def __iter__(self):
        return iter(self.base)

    def __len__(self):
        return len(self.base)


class S3MutableMapping(MutableMapping):
    def __init__(self, s3api, bucket):
        """Turn a minio/aws S3 API into a simple mutable mapping"""
        self._s3 = s3api
        self._bucket = bucket

    def __getitem__(self, key):
        try:
            return self._s3.get_object(self._bucket, key).data
        except NoSuchKey:
            raise KeyError

    def __setitem__(self, key, value):
        self._s3.put_object(self._bucket, key, BytesIO(value), len(value))

    def __delitem__(self, key):
        self._s3.remove_object(self._bucket, key)

    def __iter__(self):
        return (
            o.object_name for o in self._s3.list_objects(self._bucket, recursive=True)
        )

    def __len__(self):
        raise NotImplementedError("No performant way to count bucket size")


class NanoFilter:
    def __init__(self, filter_column):
        self.filter_column = filter_column

    @property
    def name(self):
        return "nanofilter-" + self.filter_column

    def dig(self, events, colname):
        prefix, *col = colname.split("_")
        col = "_".join(col)
        if col == "":
            if prefix.startswith("n"):
                return events[prefix[1:]].counts
            return events[prefix]
        return events[prefix][col]

    def __call__(self, col, input_columns):
        from coffea.nanoaod import NanoEvents

        events = NanoEvents.from_arrays(input_columns.arrays())

        if col["name"] == "_num":
            return numpy.array(self.dig(events, self.filter_column).sum())

        events = events[self.dig(events, self.filter_column)]
        out = self.dig(events, col["name"]).flatten()
        return out.astype(col["dtype"])  # kill any implicit casts


class ColumnClient:
    server_url = _default_server()
    _state = {}
    _initlock = Lock()

    def _initialize(self, config=None):
        if config is None:
            self._api = httpx.Client(
                base_url=ColumnClient.server_url,
                # TODO: timeout/retry
            )
            config = self._api.get("/clientconfig").json()
        self._storage = self._init_storage(config["storage"])
        self._file_catalog = config["file_catalog"]
        self._xrootdsource = config["xrootdsource"]
        self._xrootdsource_metadata = config["xrootdsource_metadata"]
        self._init = True

    def __init__(self, config=None):
        self.__dict__ = self._state  # borg
        with self._initlock:
            if not hasattr(self, "_init"):
                self._initialize(config)

    def reinitialize(self, config=None):
        with self._initlock:
            self._initialize(config)

    def _init_storage(self, config):
        if config["type"] == "filesystem":
            return FilesystemMutableMapping(**config["args"])
        elif config["type"] == "minio":
            from minio import Minio

            s3api = Minio(**config["args"])
            return S3MutableMapping(s3api, config["bucket"])
        elif config["type"] == "minio-buffered":
            from minio import Minio

            s3api = Minio(**config["args"])
            buffer = LRUCache(config["buffersize"], getsizeof=lambda v: len(v))
            base = S3MutableMapping(s3api, config["bucket"])
            return ReadBuffer(buffer, base)
        raise ValueError("Unrecognized storage type {config['type']}")

    @property
    def storage(self):
        return self._storage

    def _lfn2pfn(self, lfn: str, catalog_index: int):
        algo = self._file_catalog[catalog_index]
        if algo["algo"] == "prefix":
            return algo["prefix"] + lfn
        raise RuntimeError("Unrecognized LFN2PFN algorithm type")

    def open_file_metadata(self, lfn: str, fallback: int = 0):
        try:
            pfn = self._lfn2pfn(lfn, fallback)
            return uproot.open(pfn, xrootdsource=self._xrootdsource_metadata)
        except IOError as ex:
            if fallback == len(self.catalog) - 1:
                raise
            logger.info("Fallback due to IOError in FileOpener: " + str(ex))
            return self.open_file_metadata(lfn, fallback + 1)

    @classmethod
    @lru_cache(maxsize=4)
    def open_file(cls, lfn: str, fallback: int = 0):
        self = cls()
        try:
            pfn = self._lfn2pfn(lfn, fallback)
            return uproot.open(pfn, xrootdsource=self._xrootdsource)
        except IOError as ex:
            if fallback == len(self.catalog) - 1:
                raise
            logger.info("Fallback due to IOError in FileOpener: " + str(ex))
            return self.open_file(lfn, fallback + 1)

    @classmethod
    @lru_cache(maxsize=8)
    def columns(cls, columnset_name: str):
        self = cls()
        out = self._api.get("/columnsets/%s" % columnset_name).json()
        return out["columns"]

    @classmethod
    @lru_cache(maxsize=8)
    def generator(cls, generator_name: str):
        self = cls()
        out = self._api.get("/generators/%s" % generator_name).json()
        function = self.storage[out["function_key"]]
        out["function"] = cloudpickle.loads(lz4f.decompress(function))
        return out

    def register_generator(
        self, function, base_columnset, input_columns, output_columns
    ):
        """Probably better put as a member of a ColumnSet class"""
        if hasattr(function, "name"):
            generator_name = function.name
        else:
            generator_name = function.__name__
        function_data = lz4f.compress(cloudpickle.dumps(function))
        function_key = "generator/" + hashlib.sha256(function_data).hexdigest()

        columns = self._api.get("columnsets/" + base_columnset).json()
        if "_num" not in input_columns:
            input_columns.append("_num")
        generator = {
            "name": generator_name,
            "input_columns": [
                c for c in columns["columns"] if c["name"] in input_columns
            ],
            "function_key": function_key,
        }
        for col in output_columns:
            col = dict(col)
            col["generator"] = generator_name
            col["packoptions"] = {}
            columns["columns"].append(col)
        columns["name"] = base_columnset + "-" + generator_name

        res = self._api.post("generators", json=generator)
        if not res.status_code == 200:
            raise RuntimeError(res.json()["detail"])
        res = self._api.post("columnsets", json=columns)
        if not res.status_code == 200:
            raise RuntimeError(res.json()["detail"])
        self.storage[function_key] = function_data
        return columns["name"]

    def register_filter(self, input_columnset, filter_column):
        input_columns = [filter_column]
        output_columns = [
            {
                "name": "_num",
                "dtype": "uint32",
                "dimension": 0,
                "doc": "Number of filtered events",
            }
        ]
        for col in self.columns(input_columnset):
            if col["name"].startswith("_"):
                continue
            input_columns.append(col["name"])
            output_columns.append(col)

        return self.register_generator(
            NanoFilter(filter_column), input_columnset, input_columns, output_columns
        )


def get_file_metadata(file_lfn: str):
    file = ColumnClient().open_file_metadata(file_lfn)
    info = {"uuid": file._context.uuid.hex(), "trees": []}
    tnames = file.allkeys(
        filterclass=lambda cls: issubclass(cls, uproot.tree.TTreeMethods)
    )
    tnames = set(name.decode("ascii").split(";")[0] for name in tnames)
    for tname in tnames:
        tree = file[tname]
        columns = []
        for bname in tree.keys():
            bname = bname.decode("ascii")
            interpretation = uproot.interpret(tree[bname])
            dimension = 1
            while isinstance(interpretation, uproot.asjagged):
                interpretation = interpretation.content
                dimension += 1
            if not isinstance(interpretation.type, numpy.dtype):
                continue
            columns.append(
                {
                    "name": bname,
                    "dtype": str(interpretation.type),
                    "dimension": dimension,
                    "doc": tree[bname].title.decode("ascii"),
                    "generator": "file",
                    "packoptions": {},
                }
            )
        if len(columns) == 0:
            continue
        columns.append(
            {
                "name": "_num",
                "dtype": "uint32",
                "dimension": 0,
                "doc": "Number of events",
                "generator": "file",
                "packoptions": {},
            }
        )
        columnhash = hashlib.sha256(json.dumps({"columns": columns}).encode())
        info["trees"].append(
            {
                "name": tname,
                "numentries": tree.numentries,
                "clusters": [0] + list(c[1] for c in tree.clusters()),
                "columnset": columns,
                "columnset_hash": columnhash.hexdigest(),
            }
        )
    return info


class ColumnHelper(Mapping):
    def __init__(self, partition, columns=None):
        self._client = ColumnClient()
        self._partition = partition
        self._storage = self._client.storage
        if columns is None:
            columns = self._client.columns(self._partition["columnset"])
        self._columns = {c["name"]: c for c in columns}
        self._source = None
        self._keyprefix = "/".join(
            [
                self._partition["uuid"],
                self._partition["tree_name"],
                str(self._partition["start"]),
                str(self._partition["stop"]),
            ]
        )
        self._num = None

    def _key(self, col):
        return "/".join([self._keyprefix, col["generator"], col["name"]])

    @property
    def num(self):
        if self._num is None:
            self._num = int(self["_num"])
        return self._num

    def _file_generator(self, name):
        if name == "_num":
            return numpy.array(self._partition["stop"] - self._partition["start"])
        if self._source is None:
            self._source = self._client.open_file(self._partition["lfn"])[
                self._partition["tree_name"]
            ]
        return self._source[name].array(
            entrystart=self._partition["start"],
            entrystop=self._partition["stop"],
            flatten=True,
        )

    def _generate(self, col):
        generator = self._client.generator(col["generator"])
        return generator["function"](
            col, ColumnHelper(self._partition, generator["input_columns"])
        )

    def __getitem__(self, name: str):
        col = self._columns[name]
        key = self._key(col)
        try:
            data = self._storage[key]
            return blosc.unpack_array(data)
        except KeyError:
            if col["generator"] == "file":
                out = self._file_generator(name)
            else:
                out = self._generate(col)
            self._storage[key] = blosc.pack_array(out, **col["packoptions"])
            return out

    def __iter__(self):
        return iter(self._columns)

    def __len__(self):
        return len(self._columns)

    def virtual(self, name: str, cache: MutableMapping = None):
        col = self._columns[name]
        dtype = numpy.dtype(col["dtype"])
        if col["dimension"] == 2:
            virtualtype = awkward.type.ArrayType(float("inf"), dtype)
        elif col["dimension"] == 1:
            virtualtype = awkward.type.ArrayType(self.num, dtype)
        else:
            raise NotImplementedError
        return awkward.VirtualArray(
            self.__getitem__,
            (name,),
            {},
            type=virtualtype,
            persistentkey=self._key(col),
            cache=cache,
        )

    def arrays(self):
        return {k: self.virtual(k) for k in self if not k.startswith("_")}
