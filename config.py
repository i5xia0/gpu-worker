import asyncio
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import ClientSession

logging.basicConfig(
    level=os.getenv("WORKER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("comfyui-worker")


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip()


@dataclass(frozen=True)
class ObjectStoreConfig:
    endpoint: str = ""
    region: str = "auto"
    bucket: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    public_base_url: str = ""
    output_prefix: str = ""

    @property
    def uploads_enabled(self) -> bool:
        return all(
            [
                self.endpoint,
                self.bucket,
                self.access_key_id,
                self.secret_access_key,
            ]
        )

    def output_key(self, node_id: str, filename: str) -> str:
        parts = [self.output_prefix.strip("/"), node_id, filename]
        return "/".join(part for part in parts if part)

    def public_url(self, node_id: str, filename: str) -> str:
        if not self.public_base_url:
            return ""
        return f"{self.public_base_url.rstrip('/')}/{self.output_key(node_id, filename)}"


@dataclass
class Settings:
    host: str = _env("WORKER_HOST", "0.0.0.0")
    port: int = int(_env("WORKER_PORT", "8190"))
    comfyui_base_url: str = _env("COMFYUI_BASE_URL", "http://127.0.0.1:8188").rstrip("/")
    node_id: str = _env("WORKER_NODE_ID", "s3-g0")
    request_timeout_seconds: int = int(_env("WORKER_REQUEST_TIMEOUT_SECONDS", "300"))
    input_root: Path = Path(_env("WORKER_INPUT_ROOT", "/workspace/media"))
    output_root: Path = Path(_env("WORKER_OUTPUT_ROOT", "/workspace/media/s3-g0"))
    tmp_root: Path = Path(_env("WORKER_TMP_ROOT", "/tmp/comfyui-worker"))
    input_prefix: str = _env("WORKER_INPUT_PREFIX", "").strip("/")
    output_prefix: str = _env("WORKER_OUTPUT_PREFIX", "").strip("/")
    r2_endpoint: str = _env("R2_ENDPOINT")
    r2_region: str = _env("R2_REGION", "auto")
    r2_bucket: str = _env("R2_BUCKET")
    r2_access_key_id: str = _env("R2_ACCESS_KEY_ID")
    r2_secret_access_key: str = _env("R2_SECRET_ACCESS_KEY")
    r2_public_base_url: str = _env("R2_PUBLIC_BASE_URL").rstrip("/")

    @property
    def uploads_enabled(self) -> bool:
        return all(
            [
                self.r2_endpoint,
                self.r2_bucket,
                self.r2_access_key_id,
                self.r2_secret_access_key,
            ]
        )

    def output_key(self, filename: str) -> str:
        parts = [self.output_prefix, self.node_id, filename]
        return "/".join(part for part in parts if part)

    def public_url(self, filename: str) -> str:
        if not self.r2_public_base_url:
            return ""
        return f"{self.r2_public_base_url}/{self.output_key(filename)}"

    def default_object_store(self) -> ObjectStoreConfig:
        return ObjectStoreConfig(
            endpoint=self.r2_endpoint,
            region=self.r2_region,
            bucket=self.r2_bucket,
            access_key_id=self.r2_access_key_id,
            secret_access_key=self.r2_secret_access_key,
            public_base_url=self.r2_public_base_url,
            output_prefix=self.output_prefix,
        )


@dataclass
class JobRecord:
    prompt_id: str
    client_id: str
    temp_id: str
    temp_dir: Path
    object_store: ObjectStoreConfig
    s3_client: Any | None = None
    uploaded_urls: dict[str, str] = field(default_factory=dict)
    uploads_completed: bool = False


class WorkerState:
    def __init__(self, settings: Settings):
        self.settings = settings
        timeout = aiohttp.ClientTimeout(total=settings.request_timeout_seconds)
        self.http_session: ClientSession | None = None
        self.jobs: dict[str, JobRecord] = {}
        self.jobs_lock = asyncio.Lock()
        self.timeout = timeout

    async def startup(self) -> None:
        self.settings.input_root.mkdir(parents=True, exist_ok=True)
        self.settings.tmp_root.mkdir(parents=True, exist_ok=True)
        self.http_session = aiohttp.ClientSession(timeout=self.timeout)
        logger.info(
            "worker started: comfyui=%s uploads_enabled=%s",
            self.settings.comfyui_base_url,
            self.settings.uploads_enabled,
        )

    async def shutdown(self) -> None:
        if self.http_session is not None:
            await self.http_session.close()
            self.http_session = None
