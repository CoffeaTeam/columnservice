import json
import logging
import hashlib
import uproot
import numpy


logger = logging.getLogger(__name__)


class FileManager:
    def __init__(self, config):
        self.config = config

    def _lfn2pfn(self, lfn: str, catalog_index: int):
        algo = self.config["file_catalog"][catalog_index]
        if algo["algo"] == "prefix":
            return algo["prefix"] + lfn
        raise RuntimeError("Unrecognized LFN2PFN algorithm type")

    def _open_file(self, lfn: str, fallback: int = 0, for_metadata=False):
        try:
            pfn = self._lfn2pfn(lfn, fallback)
            return uproot.open(
                pfn,
                xrootdsource=self.config["xrootdsource_metadata"]
                if for_metadata
                else self.config["xrootdsource"],
            )
        except IOError as ex:
            if fallback == len(self.config["file_catalog"]) - 1:
                raise
            logger.info("Fallback due to IOError in FileOpener: " + str(ex))
            return self._open_file(lfn, fallback + 1)

    def open_file(self, lfn: str):
        return self._open_file(lfn)

    def get_file_metadata(self, file_lfn: str):
        file = self._open_file(file_lfn, for_metadata=True)
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
            columnhash = hashlib.sha256(
                json.dumps({"columns": columns}).encode()
            ).hexdigest()
            info["trees"].append(
                {
                    "name": tname,
                    "numentries": tree.numentries,
                    "clusters": [0] + list(c[1] for c in tree.clusters()),
                    "columnset": columns,
                    "columnset_hash": columnhash,
                }
            )
        return info
