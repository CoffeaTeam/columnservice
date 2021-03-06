import io
import os
import threading
from collections import defaultdict
from collections.abc import MutableMapping
import zict


class Counters:
    def __init__(self):
        self.counters = defaultdict(int)
        self._lock = threading.Lock()

    def inc(self, name, val):
        with self._lock:
            self.counters[name] += val


counters = Counters()


class ThreadsafeMap(MutableMapping):
    def __init__(self, base):
        """A thread-safe mutable mapping

        All access is wrapped by a lock.
        """
        self.lock = threading.Lock()
        self.base = base

    def __getitem__(self, key):
        with self.lock:
            return self.base[key]

    def __setitem__(self, key, value):
        with self.lock:
            self.base[key] = value

    def __delitem__(self, key):
        with self.lock:
            del self.base[key]

    def __iter__(self):
        with self.lock:
            return iter(self.base)

    def __len__(self):
        with self.lock:
            return len(self.base)


class FilesystemMutableMapping(MutableMapping):
    def __init__(self, path):
        """(ab)use a filesystem as a mutable mapping"""
        self._path = path

    def __getitem__(self, key):
        try:
            with open(os.path.join(self._path, key), "rb") as fin:
                value = fin.read()
            if len(value) == 0:
                # Failure to write seen sometimes?
                raise KeyError
            counters.inc("filesystem_read", len(value))
            return value
        except FileNotFoundError:
            raise KeyError

    def __setitem__(self, key, value):
        path = os.path.join(self._path, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fout:
            fout.write(value)
        counters.inc("filesystem_write", len(value))

    def __delitem__(self, key):
        os.remove(os.path.join(self._path, key))

    def __iter__(self):
        raise NotImplementedError("Too lazy to recursively ls")

    def __len__(self):
        raise NotImplementedError("No performant way to get directory count")


class S3MutableMapping(MutableMapping):
    def __init__(self, s3api, bucket):
        """Turn a minio/aws S3 API into a simple mutable mapping"""
        self._s3 = s3api
        self._bucket = bucket

    def __getitem__(self, key):
        from minio.error import NoSuchKey

        try:
            value = self._s3.get_object(self._bucket, key).data
            counters.inc("s3_read", len(value))
            return value
        except NoSuchKey:
            raise KeyError

    def __setitem__(self, key, value):
        self._s3.put_object(self._bucket, key, io.BytesIO(value), len(value))
        counters.inc("s3_write", len(value))

    def __delitem__(self, key):
        self._s3.remove_object(self._bucket, key)

    def __iter__(self):
        return (
            o.object_name for o in self._s3.list_objects(self._bucket, recursive=True)
        )

    def __len__(self):
        raise NotImplementedError("No performant way to count bucket size")


class NullMapping(MutableMapping):
    def __init__(self):
        """Has nothing in it, always"""

    def __getitem__(self, key):
        raise KeyError

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        raise KeyError

    def __iter__(self):
        return iter([])

    def __len__(self):
        return 0


shared_mappings = {}
setup_lock = threading.Lock()


def setup_mapping(config):
    if "shared" not in config:
        return setup_mapping_impl(config)
    with setup_lock:
        mapname = config["shared"]
        try:
            return shared_mappings[mapname]
        except KeyError:
            mapping = ThreadsafeMap(setup_mapping_impl(config))
            shared_mappings[mapname] = mapping
            return mapping


def setup_mapping_impl(config):
    if config["type"] == "null":
        return NullMapping()
    elif config["type"] == "filesystem":
        return FilesystemMutableMapping(**config["args"])
    elif config["type"].startswith("minio"):
        from minio import Minio

        s3api = Minio(**config["args"])
        base = S3MutableMapping(s3api, config["bucket"])
        if config["type"] == "minio":
            return base
        elif config["type"] == "minio-blosc":
            import blosc

            return zict.Func(blosc.pack_array, blosc.unpack_array, base)
        elif config["type"] == "minio-pickle":
            import pickle

            return zict.Func(pickle.dumps, pickle.loads, base)
    raise ValueError(f"Unrecognized storage type {config['type']}")
