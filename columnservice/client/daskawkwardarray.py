import dask.base
import dask.utils
import dask.delayed
from dask.delayed import Delayed
import dask.multiprocessing
import dask.highlevelgraph
import dask.optimization
from operator import getitem, add
from functools import reduce, partial
import numpy.lib.mixins
import awkward1
import numbers


def map_partitions(func, *args, **kwargs):
    name = "%s-%s" % (
        dask.utils.funcname(func),
        dask.base.tokenize(func, "map-partitions", *args, **kwargs),
    )
    dsk = {}
    dependencies = []

    arrays = []
    args2 = []
    for a in args:
        if isinstance(a, DaskAwkwardArray):
            arrays.append(a)
            args2.append(a)
        elif isinstance(a, (Delayed,)):
            args2.append(a.key)
            dependencies.append(a)
        else:
            args2.append(a)

    offsets = arrays[0].partition_offsets
    if any(item.partition_offsets != offsets for item in arrays):
        raise ValueError("All DaskAwkwardArray must have the same partitioning")
    npartitions = len(offsets) - 1

    def build_args(n):
        return [(a.name, n) if isinstance(a, DaskAwkwardArray) else a for a in args2]

    if kwargs:
        dsk = {
            (name, n): (
                dask.utils.apply,
                func,
                build_args(n),
                (dict, (zip, list(kwargs), list(kwargs.values()))),
            )
            for n in range(npartitions)
        }
    else:
        dsk = {(name, n): (func,) + tuple(build_args(n)) for n in range(npartitions)}

    graph = dask.highlevelgraph.HighLevelGraph.from_collections(
        name, dsk, dependencies=arrays + dependencies
    )

    return DaskAwkwardArray(graph, name, offsets)


def strip_behavior(array):
    array.behavior = None
    return array


class DaskAwkwardArray(
    dask.base.DaskMethodsMixin, numpy.lib.mixins.NDArrayOperatorsMixin
):
    def __init__(self, dsk, name, partition_offsets):
        if not isinstance(dsk, dask.highlevelgraph.HighLevelGraph):
            dsk = dask.highlevelgraph.HighLevelGraph.from_collections(
                name, dsk, dependencies=[]
            )
        self._dsk = dsk
        self.name = name
        self.partition_offsets = partition_offsets

    def __dask_graph__(self):
        return self._dsk

    def __dask_keys__(self):
        return [(self.name, i) for i in range(self.npartitions)]

    def __dask_layers__(self):
        return (self.name,)

    def __dask_tokenize__(self):
        return self.name

    @staticmethod
    def __dask_optimize__(dsk, keys, **kwargs):
        dsk2, _ = dask.optimization.fuse_linear(dsk, keys)
        return dsk2

    __dask_scheduler__ = staticmethod(dask.multiprocessing.get)

    def __dask_postcompute__(self):
        return awkward1.concatenate, ()

    def __dask_postpersist__(self):
        return type(self), (self.name, self.partition_offsets)

    def __len__(self):
        return self.partition_offsets[-1]

    @property
    def npartitions(self):
        return len(self.partition_offsets) - 1

    def __str__(self):
        return "DaskAwkwardArray<%s, len=%d, parts=%d>" % (
            dask.utils.key_split(self.name),
            len(self),
            self.npartitions,
        )

    __repr__ = __str__

    @classmethod
    def from_partitions(cls, partitions, builder, offsets):
        # builder = dask.delayed(builder)

        name = "awkward-from-partitions-" + dask.base.tokenize(builder, *partitions)
        names = [(name, i) for i in range(len(partitions))]
        values = [(builder, part) for part in partitions]
        dsk = dict(zip(names, values))

        graph = dask.highlevelgraph.HighLevelGraph.from_collections(
            name, dsk, dependencies=[]
        )
        return cls(graph, name, offsets)

    def map_partitions(self, func, *args, **kwargs):
        return map_partitions(func, self, *args, **kwargs)

    _HANDLED_TYPES = (numbers.Number,)

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        if "out" in kwargs:
            raise ValueError("awkward dask ufuncs cannot use 'out'")

        if method != "__call__":
            raise ValueError("only ufunc call method is supported")

        for x in inputs:
            if not isinstance(x, self._HANDLED_TYPES + (DaskAwkwardArray,)):
                return NotImplemented

        result = map_partitions(ufunc, *inputs, **kwargs)
        return result

    def strip_behavior(self):
        return self.map_partitions(strip_behavior)

    def __getitem__(self, key):
        return map_partitions(getitem, self, key)

    def __getattr__(self, key):
        if key in dir(type(self)):
            return super(DaskAwkwardArray, self).__getattribute__(key)
        return map_partitions(getattr, self, key)

    def reduction(self, map_func, reduce_func=None):
        if reduce_func is None:
            reduce_func = partial(reduce, add)
        bag = dask.bag.Bag(self._dsk, self.name, self.npartitions)
        return bag.reduction(map_func, reduce_func).to_delayed()
