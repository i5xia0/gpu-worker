import asyncio
import copy
import json
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import boto3
from aiohttp import ClientSession, WSMsgType, web
from botocore.config import Config as BotoConfig


logging.basicConfig(
    level=os.getenv("WORKER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("comfyui-worker")


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip()


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


@dataclass
class JobRecord:
    prompt_id: str
    client_id: str
    temp_id: str
    temp_dir: Path
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
        self.s3_client = self._build_s3_client()

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

    def _build_s3_client(self):
        if not self.settings.uploads_enabled:
            return None

        session = boto3.session.Session()
        return session.client(
            "s3",
            endpoint_url=self.settings.r2_endpoint,
            region_name=self.settings.r2_region,
            aws_access_key_id=self.settings.r2_access_key_id,
            aws_secret_access_key=self.settings.r2_secret_access_key,
            config=BotoConfig(signature_version="s3v4"),
        )


def _normalize_assets(raw_assets: Any) -> list[dict[str, Any]]:
    if raw_assets is None:
        return []
    if isinstance(raw_assets, list):
        return [asset for asset in raw_assets if isinstance(asset, dict)]
    if isinstance(raw_assets, dict):
        items: list[dict[str, Any]] = []
        for logical_name, value in raw_assets.items():
            if isinstance(value, dict):
                item = dict(value)
                item.setdefault("logical_name", logical_name)
                items.append(item)
        return items
    return []


def _sanitize_filename(name: str) -> str:
    base = os.path.basename(name.strip()) or f"asset-{uuid.uuid4().hex}"
    return base.replace("\\", "_")


def _replace_value(payload: Any, matcher, replacement: str) -> Any:
    if isinstance(payload, dict):
        return {key: _replace_value(value, matcher, replacement) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_replace_value(value, matcher, replacement) for value in payload]
    if isinstance(payload, str) and matcher(payload):
        return replacement
    return payload


_download_locks: dict[str, asyncio.Lock] = {}
_download_locks_guard = asyncio.Lock()


async def _get_file_lock(filename: str) -> asyncio.Lock:
    async with _download_locks_guard:
        if filename not in _download_locks:
            _download_locks[filename] = asyncio.Lock()
        return _download_locks[filename]


async def _download_http(session: ClientSession, source_url: str, dest_path: Path) -> None:
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    try:
        async with session.get(source_url) as response:
            response.raise_for_status()
            with tmp_path.open("wb") as output:
                async for chunk in response.content.iter_chunked(1024 * 1024):
                    output.write(chunk)
        if tmp_path.stat().st_size == 0:
            tmp_path.unlink(missing_ok=True)
            raise ValueError(f"downloaded empty file from {source_url}")
        tmp_path.rename(dest_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _parse_object_store_url(raw_url: str, default_bucket: str) -> tuple[str, str]:
    parsed = urlparse(raw_url)
    bucket = parsed.netloc or default_bucket
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"invalid object url: {raw_url}")
    return bucket, key


async def _download_s3(state: WorkerState, raw_url: str, dest_path: Path) -> None:
    if state.s3_client is None:
        raise RuntimeError("R2 upload/download is not configured")

    bucket, key = _parse_object_store_url(raw_url, state.settings.r2_bucket)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    def _stream_download() -> None:
        state.s3_client.download_file(bucket, key, str(tmp_path))

    try:
        await asyncio.to_thread(_stream_download)
        if tmp_path.stat().st_size == 0:
            tmp_path.unlink(missing_ok=True)
            raise ValueError(f"downloaded empty file from {raw_url}")
        tmp_path.rename(dest_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


async def _ensure_asset(
    state: WorkerState,
    source_url: str,
    filename: str,
    input_dir: Path,
) -> None:
    dest_path = input_dir / filename

    if dest_path.exists():
        logger.info("cache HIT: %s (skip download)", filename)
        return

    lock = await _get_file_lock(filename)
    async with lock:
        if dest_path.exists():
            logger.info("cache HIT (after lock): %s", filename)
            return

        logger.info("cache MISS: %s ← %s", filename, source_url)
        if source_url.startswith(("http://", "https://")):
            await _download_http(state.http_session, source_url, dest_path)
        elif source_url.startswith(("s3://", "r2://")):
            await _download_s3(state, source_url, dest_path)
        else:
            raise ValueError(f"unsupported asset source: {source_url}")
        logger.info("downloaded: %s (%.1f KB)", filename, dest_path.stat().st_size / 1024)


async def _prepare_assets(
    state: WorkerState,
    prompt_payload: dict[str, Any],
    assets: list[dict[str, Any]],
) -> dict[str, Any]:
    prompt = copy.deepcopy(prompt_payload)
    input_dir = state.settings.input_root
    input_dir.mkdir(parents=True, exist_ok=True)

    if state.http_session is None:
        raise RuntimeError("HTTP session not initialized")

    download_tasks: list[tuple[str, str, dict[str, Any]]] = []

    for asset in assets:
        source_url = (
            asset.get("url")
            or asset.get("source_url")
            or asset.get("r2_url")
            or asset.get("object_url")
        )
        if not source_url:
            continue

        filename = _sanitize_filename(
            asset.get("filename")
            or asset.get("name")
            or Path(urlparse(source_url).path).name
        )
        download_tasks.append((source_url, filename, asset))

    if download_tasks:
        await asyncio.gather(
            *[
                _ensure_asset(state, url, fname, input_dir)
                for url, fname, _ in download_tasks
            ]
        )

    for source_url, filename, asset in download_tasks:
        comfy_relative_path = "/".join(
            part for part in [state.settings.input_prefix, filename] if part
        )
        node_id = str(asset.get("node_id") or asset.get("nodeId") or "")
        input_name = str(asset.get("input_name") or asset.get("inputName") or "")

        if node_id and input_name:
            prompt.setdefault(node_id, {}).setdefault("inputs", {})[input_name] = comfy_relative_path
            continue

        placeholders = {
            str(value)
            for value in [
                asset.get("placeholder"),
                asset.get("logical_name"),
                asset.get("original_value"),
                asset.get("filename"),
            ]
            if value
        }
        if not placeholders:
            placeholders = {filename}

        prompt = _replace_value(prompt, lambda value, ph=placeholders: value in ph, comfy_relative_path)

    return prompt


async def _proxy_json(request: web.Request, path: str) -> web.Response:
    state: WorkerState = request.app["state"]
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    target_url = f"{state.settings.comfyui_base_url}{path}"
    async with state.http_session.get(target_url) as response:
        payload = await response.read()
        return web.Response(
            status=response.status,
            body=payload,
            content_type=response.content_type,
        )


async def _upload_output_if_needed(
    state: WorkerState,
    item: dict[str, Any],
    record: JobRecord,
) -> None:
    filename = item.get("filename")
    if not filename:
        return

    subfolder = item.get("subfolder") or ""
    local_path = state.settings.output_root / subfolder / filename
    if not local_path.exists():
        logger.warning("output file not found: %s", local_path)
        return

    if not state.settings.uploads_enabled or state.s3_client is None:
        return

    ts = int(time.time())
    remote_filename = f"{ts}_{filename}"
    key = state.settings.output_key(remote_filename)

    def _put_object() -> None:
        with local_path.open("rb") as file_obj:
            state.s3_client.put_object(
                Bucket=state.settings.r2_bucket,
                Key=key,
                Body=file_obj,
            )

    await asyncio.to_thread(_put_object)
    record.uploaded_urls[filename] = state.settings.public_url(remote_filename)


def _iter_output_items(history_data: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    outputs = history_data.get("outputs")
    if not isinstance(outputs, dict):
        return results

    for node_output in outputs.values():
        if not isinstance(node_output, dict):
            continue
        for key in ("images", "audios", "gifs", "videos", "video"):
            values = node_output.get(key)
            if not isinstance(values, list):
                continue
            for item in values:
                if isinstance(item, dict) and item.get("type") == "output":
                    results.append(item)
    return results


async def _cleanup_job(record: JobRecord) -> None:
    def _cleanup() -> None:
        shutil.rmtree(record.temp_dir, ignore_errors=True)

    await asyncio.to_thread(_cleanup)


async def healthz(request: web.Request) -> web.Response:
    state: WorkerState = request.app["state"]
    return web.json_response(
        {
            "status": "ok",
            "comfyui_base_url": state.settings.comfyui_base_url,
            "uploads_enabled": state.settings.uploads_enabled,
            "node_id": state.settings.node_id,
        }
    )


async def system_stats(request: web.Request) -> web.Response:
    return await _proxy_json(request, "/system_stats")


async def queue_status(request: web.Request) -> web.Response:
    return await _proxy_json(request, "/queue")


async def prompt(request: web.Request) -> web.Response:
    state: WorkerState = request.app["state"]
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    payload = await request.json()
    client_id = payload.get("client_id") or payload.get("clientId") or uuid.uuid4().hex
    extra_data = payload.get("extra_data")
    if not isinstance(extra_data, dict):
        extra_data = {}
    input_assets = _normalize_assets(
        payload.pop("input_assets", None)
        or payload.pop("assets", None)
        or extra_data.get("input_assets")
    )

    prompt_payload = payload.get("prompt")
    if not isinstance(prompt_payload, dict):
        raise web.HTTPBadRequest(text="`prompt` must be an object")

    temp_id = uuid.uuid4().hex
    temp_dir = state.settings.tmp_root / temp_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        resolved_prompt = await _prepare_assets(state, prompt_payload, input_assets)
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.exception("failed to prepare assets")
        raise web.HTTPBadRequest(text=f"failed to prepare assets: {exc}") from exc

    upstream_payload = dict(payload)
    upstream_payload["client_id"] = client_id
    upstream_payload["prompt"] = resolved_prompt

    target_url = f"{state.settings.comfyui_base_url}/prompt"
    try:
        async with state.http_session.post(target_url, json=upstream_payload) as response:
            raw = await response.read()
            if response.status != 200:
                raise web.HTTPBadGateway(text=raw.decode("utf-8", errors="ignore"))
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    data = json.loads(raw.decode("utf-8"))
    prompt_id = data.get("prompt_id")
    if prompt_id:
        async with state.jobs_lock:
            state.jobs[prompt_id] = JobRecord(
                prompt_id=prompt_id,
                client_id=client_id,
                temp_id=temp_id,
                temp_dir=temp_dir,
            )

    return web.json_response(data)


async def history(request: web.Request) -> web.Response:
    state: WorkerState = request.app["state"]
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    prompt_id = request.match_info["prompt_id"]
    target_url = f"{state.settings.comfyui_base_url}/history/{prompt_id}"

    async with state.http_session.get(target_url) as response:
        raw = await response.read()
        if response.status != 200:
            return web.Response(status=response.status, body=raw, content_type=response.content_type)

    history_payload = json.loads(raw.decode("utf-8"))
    history_data = history_payload.get(prompt_id)

    if isinstance(history_data, dict):
        async with state.jobs_lock:
            record = state.jobs.get(prompt_id)

        if record and not record.uploads_completed:
            items = _iter_output_items(history_data)
            try:
                for item in items:
                    item.setdefault("prompt_id", prompt_id)
                    await _upload_output_if_needed(state, item, record)
                record.uploads_completed = True
                await _cleanup_job(record)
            except Exception:
                logger.exception("failed to upload outputs for prompt %s", prompt_id)

        if record:
            history_data["worker"] = {
                "node_id": state.settings.node_id,
                "uploaded_urls": record.uploaded_urls,
            }

    return web.json_response(history_payload)


async def view_proxy(request: web.Request) -> web.StreamResponse:
    state: WorkerState = request.app["state"]
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    query = request.query_string
    target_url = f"{state.settings.comfyui_base_url}/view"
    if query:
        target_url = f"{target_url}?{query}"

    async with state.http_session.get(target_url) as response:
        body = await response.read()
        headers = {}
        content_type = response.headers.get("Content-Type")
        if content_type:
            headers["Content-Type"] = content_type
        return web.Response(status=response.status, body=body, headers=headers)


async def websocket_proxy(request: web.Request) -> web.WebSocketResponse:
    state: WorkerState = request.app["state"]
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    client_id = request.query.get("clientId") or request.query.get("client_id")
    if not client_id:
        raise web.HTTPBadRequest(text="missing clientId")

    frontend_ws = web.WebSocketResponse(heartbeat=20)
    await frontend_ws.prepare(request)

    backend_url = f"{state.settings.comfyui_base_url.replace('http://', 'ws://').replace('https://', 'wss://')}/ws?clientId={client_id}"
    try:
        backend_ws = await state.http_session.ws_connect(backend_url, heartbeat=20)
    except Exception as exc:
        await frontend_ws.close(message=str(exc).encode("utf-8", errors="ignore"))
        return frontend_ws

    async def _relay_backend() -> None:
        async for message in backend_ws:
            if message.type == WSMsgType.TEXT:
                await frontend_ws.send_str(message.data)
            elif message.type == WSMsgType.BINARY:
                await frontend_ws.send_bytes(message.data)
            elif message.type == WSMsgType.ERROR:
                break

    async def _relay_frontend() -> None:
        async for message in frontend_ws:
            if message.type == WSMsgType.TEXT:
                await backend_ws.send_str(message.data)
            elif message.type == WSMsgType.BINARY:
                await backend_ws.send_bytes(message.data)
            elif message.type == WSMsgType.ERROR:
                break

    relay_tasks = [
        asyncio.create_task(_relay_backend()),
        asyncio.create_task(_relay_frontend()),
    ]

    done, pending = await asyncio.wait(relay_tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    for task in done:
        task.result()

    await backend_ws.close()
    await frontend_ws.close()
    return frontend_ws


def create_app() -> web.Application:
    settings = Settings()
    state = WorkerState(settings)
    app = web.Application()
    app["state"] = state

    app.router.add_get("/healthz", healthz)
    app.router.add_post("/prompt", prompt)
    app.router.add_get("/history/{prompt_id}", history)
    app.router.add_get("/queue", queue_status)
    app.router.add_get("/system_stats", system_stats)
    app.router.add_get("/view", view_proxy)
    app.router.add_get("/ws", websocket_proxy)

    async def _startup(app: web.Application) -> None:
        await app["state"].startup()

    async def _cleanup(app: web.Application) -> None:
        await app["state"].shutdown()

    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host=Settings().host, port=Settings().port)
