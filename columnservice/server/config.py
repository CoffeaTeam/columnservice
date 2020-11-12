import os
from columnservice.server.x509util import TLS_CA


if "FILESTORE" in os.environ:
    storage = {
        "type": "filesystem",
        "args": {"path": os.environ["FILESTORE"]},
    }
else:
    storage = {
        "type": "minio-blosc",
        "bucket": os.environ["COLUMNSERVICE_BUCKET"],
        "args": {
            "endpoint": os.environ["MINIO_HOSTNAME"],
            "access_key": os.environ["MINIO_ACCESS_KEY"],
            "secret_key": os.environ["MINIO_SECRET_KEY"],
            "secure": False,
        },
    }

filemanager = {
    "file_catalog": [
        {"algo": "prefix", "prefix": "root://coffea@cmsxrootd-site.fnal.gov/"},
        {"algo": "prefix", "prefix": "root://coffea@cmsxrootd.fnal.gov/"},
        {"algo": "prefix", "prefix": "root://coffea@cms-xrd-global.cern.ch/"},
    ],
    "uproot_options": {
        "timeout": 30,
        "max_num_elements": None,
    },
}

client_config = {
    "storage": storage,
    "filemanager": filemanager,
    "tls_ca": TLS_CA,
}
