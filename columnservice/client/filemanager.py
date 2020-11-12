import json
import logging
import hashlib
import uproot4
from coffea.nanoevents import NanoEventsFactory


logger = logging.getLogger(__name__)


class FileManager:
    _test_config = {
        "file_catalog": [
            {"algo": "identity"},
        ],
        "uproot_options": {
            "timeout": 30,
            "max_num_elements": None,
        },
    }

    def __init__(self, config):
        self.config = config

    def _lfn2pfn(self, lfn: str, catalog_index: int):
        entry = self.config["file_catalog"][catalog_index]
        if entry["algo"] == "identity":
            return lfn
        elif entry["algo"] == "prefix":
            return entry["prefix"] + lfn
        raise RuntimeError("Unrecognized LFN2PFN entry algorithm type")

    def _open_file(self, lfn: str, fallback: int = 0):
        try:
            pfn = self._lfn2pfn(lfn, fallback)
            return uproot4.open(pfn, self.config["uproot_options"])
        except IOError as ex:
            if fallback == len(self.config["file_catalog"]) - 1:
                raise
            logger.info("Fallback due to IOError in FileOpener: " + str(ex))
            return self._open_file(lfn, fallback + 1)

    def open_file(self, lfn: str):
        return self._open_file(lfn)

    def get_file_metadata(self, file_lfn: str):
        rootdir = self._open_file(file_lfn)
        info = {"uuid": rootdir.file.uuid.hex, "trees": []}
        tnames = rootdir.keys(
            recursive=True, filter_classname=lambda cls: cls == "TTree"
        )
        for tname in tnames:
            tree = rootdir[tname]
            base_form = NanoEventsFactory._extract_base_form(tree)
            form_hash = hashlib.sha256(json.dumps(base_form).encode()).hexdigest()
            info["trees"].append(
                {
                    "name": tname,
                    "num_entries": tree.num_entries,
                    "common_entry_offsets": tree.common_entry_offsets(),
                    "base_form": base_form,
                    "form_hash": form_hash,
                }
            )
        return info
