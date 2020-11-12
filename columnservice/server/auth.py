import asyncio
import logging
import os
from tempfile import NamedTemporaryFile
from io import BytesIO
from ssl import SSLError
from fastapi import APIRouter, HTTPException, File
from fastapi.responses import PlainTextResponse
from starlette.status import HTTP_401_UNAUTHORIZED
from httpx import NetworkError
from dmwmclient import RESTClient
from columnservice.server.x509util import create_user_cert
from columnservice.server.common import generic_http_error
from columnservice.server.services import services


logger = logging.getLogger(__name__)
USER_ALLOWLIST = os.environ.get("USER_ALLOWLIST", "").split(",")
CERT_LOCK = asyncio.Lock()
router = APIRouter()


@router.post(
    "/clientkey", responses={401: generic_http_error}, response_class=PlainTextResponse
)
async def get_clientkey(proxycert: bytes = File(...)):
    tmp = None
    try:
        tmp = await services.run_pool(NamedTemporaryFile)
        await services.run_pool(lambda: tmp.write(proxycert))
        client = RESTClient(usercert=tmp.name)
        userdata = await client.getjson(
            "https://cms-cric.cern.ch/api/accounts/user/query/?json&preset=whoami"
        )
        if len(userdata["result"]) == 0:
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "No account record in CRIC",
            )
        userdata = userdata["result"][0]
        username = userdata["login"]
        fullname = userdata["name"]
        if username not in USER_ALLOWLIST:
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "Sorry, your account is not authorized",
            )
        async with CERT_LOCK:
            out = await services.run_pool(
                lambda: create_user_cert(username, fullname, BytesIO())
            )
    except NetworkError:
        raise HTTPException(
            HTTP_401_UNAUTHORIZED,
            "Failed to authorize",
        )
    except SSLError:
        raise HTTPException(
            HTTP_401_UNAUTHORIZED,
            "Failed to authorize",
        )
    finally:
        if tmp:
            await services.run_pool(lambda: tmp.close())
    return out.getvalue()
