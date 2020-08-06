import os
import logging
import httpx
import distributed
import distributed.security
from columnservice.client.mapping import setup_mapping
from columnservice.client.filemanager import FileManager


logger = logging.getLogger(__name__)


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
        self, ca_path="dask_ca.crt", client_cert_path="dask_client_cert.pem", port=8786,
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
        url = f"tls://{self.hostname}:{port}"
        return distributed.Client(url, security=sec)
