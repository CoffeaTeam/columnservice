import logging
import hashlib
import bson
import numpy
import uproot


logger = logging.getLogger(__name__)


class FileOpener:
    def __init__(self, filecatalog, timeout=20):
        self.filecatalog = filecatalog
        self.xrootdsource = {
            "timeout": timeout,
            "chunkbytes": 32 * 1024,
            "limitbytes": 1024 ** 2,
            "parallel": False,
        }

    def _lfn2pfn(self, lfn, catalog_index):
        algo = self.filecatalog[catalog_index]
        if algo["type"] == "prefix":
            return algo["prefix"] + lfn
        raise RuntimeError("Unrecognized LFN2PFN algorithm type")

    def open(self, lfn, fallback=0):
        try:
            pfn = self._lfn2pfn(lfn, fallback)
            return uproot.open(pfn, xrootdsource=self.xrootdsource)
        except IOError as ex:
            if fallback == len(self.filecatalog) - 1:
                raise
            logger.info("Fallback due to IOError in FileOpener: " + str(ex))
            return self.open(lfn, fallback + 1)


fileOpener = FileOpener(
    [
        {"type": "prefix", "prefix": "root://coffea@cmsxrootd-site.fnal.gov/"},
        {"type": "prefix", "prefix": "root://coffea@cmsxrootd.fnal.gov/"},
        {"type": "prefix", "prefix": "root://coffea@cms-xrd-global.cern.ch/"},
    ]
)


def get_file_metadata(file_lfn: str):
    file = fileOpener.open(file_lfn)
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
                    "generator": "default",
                }
            )
        if len(columns) == 0:
            continue
        columnhash = hashlib.sha256(bson.encode({"columns": columns}))
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


if __name__ == "__main__":
    lfn = "/store/mc/RunIIFall17NanoAODv6/ZH_HToBB_ZToQQ_M125_13TeV_powheg_pythia8/NANOAODSIM/PU2017_12Apr2018_Nano25Oct2019_102X_mc2017_realistic_v7-v1/260000/9E0D57B7-D1B8-EC4F-9CE6-6978F003F700.root"  # noqa
    from pprint import pprint

    pprint(get_file_metadata(lfn))
