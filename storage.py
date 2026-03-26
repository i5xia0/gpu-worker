import asyncio
import copy
import hashlib
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Coroutine
from urllib.parse import urlparse

import aiohttp
import boto3
from aiohttp import ClientSession
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
)

from config import JobRecord, ObjectStoreConfig, Settings, WorkerState, logger

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 15.0

_RETRYABLE_HTTP_CODES = frozenset({408, 429, 500, 502, 503, 504})

_download_locks: dict[str, tuple[asyncio.Lock, int]] = {}
_download_locks_guard = asyncio.Lock()


def _is_retryable_error(exc: BaseException) -> bool:
    """识别可重试的瞬时异常，避免对明确失败场景重复请求。"""
    if isinstance(exc, aiohttp.ClientResponseError) and exc.status in _RETRYABLE_HTTP_CODES:
        return True
    if isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError, OSError)):
        return True
    if isinstance(exc, (ConnectionClosedError, EndpointConnectionError)):
        return True
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
        return code in ("RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable")
    return False


async def _retry(
    fn: Callable[..., Coroutine],
    *args,
    label: str = "",
    max_retries: int = MAX_RETRIES,
) -> Any:
    """统一重试入口：指数退避 + 上限延时。"""
    last_exc: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return await fn(*args)
        except Exception as exc:
            last_exc = exc
            if not _is_retryable_error(exc) or attempt == max_retries:
                raise
            delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
            logger.warning(
                "retry %d/%d %s: %s (wait %.1fs)",
                attempt, max_retries, label, exc, delay,
            )
            await asyncio.sleep(delay)
    raise last_exc  # type: ignore[misc]


class _FileLock:
    """引用计数文件锁，所有使用者释放后自动从全局表中移除。"""

    def __init__(self, filename: str):
        self._filename = filename
        self._lock: asyncio.Lock | None = None
        self._acquired = False

    async def __aenter__(self) -> None:
        async with _download_locks_guard:
            if self._filename in _download_locks:
                lock, refcount = _download_locks[self._filename]
                _download_locks[self._filename] = (lock, refcount + 1)
            else:
                lock = asyncio.Lock()
                _download_locks[self._filename] = (lock, 1)
            self._lock = lock
        try:
            await self._lock.acquire()
            self._acquired = True
        except BaseException:
            async with _download_locks_guard:
                entry = _download_locks.get(self._filename)
                if entry is not None:
                    lock, refcount = entry
                    if refcount <= 1:
                        _download_locks.pop(self._filename, None)
                    else:
                        _download_locks[self._filename] = (lock, refcount - 1)
            raise

    async def __aexit__(self, *exc) -> None:
        if not self._acquired or self._lock is None:
            return
        self._lock.release()
        async with _download_locks_guard:
            entry = _download_locks.get(self._filename)
            if entry is not None:
                lock, refcount = entry
                if refcount <= 1:
                    _download_locks.pop(self._filename, None)
                else:
                    _download_locks[self._filename] = (lock, refcount - 1)


def _get_string(raw: Any, *keys: str) -> str:
    if not isinstance(raw, dict):
        return ""
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def resolve_object_store_config(raw: Any, settings: Settings) -> ObjectStoreConfig:
    """将请求级配置与环境默认配置合并成最终对象存储配置。"""
    defaults = settings.default_object_store()
    return ObjectStoreConfig(
        endpoint=_get_string(raw, "endpoint", "endpoint_url", "r2_endpoint") or defaults.endpoint,
        region=_get_string(raw, "region", "bucket_region", "r2_region") or defaults.region,
        bucket=_get_string(raw, "bucket", "bucket_name", "r2_bucket") or defaults.bucket,
        access_key_id=_get_string(raw, "access_key_id", "accessKeyId", "r2_access_key_id")
        or defaults.access_key_id,
        secret_access_key=_get_string(
            raw,
            "secret_access_key",
            "secretAccessKey",
            "r2_secret_access_key",
        )
        or defaults.secret_access_key,
        public_base_url=_get_string(
            raw,
            "public_base_url",
            "public_url",
            "publicBaseUrl",
            "r2_public_base_url",
            "base_url",
        )
        or defaults.public_base_url,
        output_prefix=_get_string(raw, "output_prefix", "prefix", "path", "r2_output_prefix")
        or defaults.output_prefix,
    )


def build_s3_client(config: ObjectStoreConfig):
    if not config.uploads_enabled:
        logger.info("s3 client: SKIP (uploads not configured)")
        return None

    logger.info(
        "s3 client: OK endpoint=%s bucket=%s public_base_url=%s",
        config.endpoint, config.bucket, config.public_base_url or "(none)",
    )
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=config.endpoint,
        region_name=config.region,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        config=BotoConfig(signature_version="s3v4"),
    )


def normalize_assets(raw_assets: Any) -> list[dict[str, Any]]:
    """兼容 list/dict 两种上游资产格式，统一为 list[dict]。"""
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


_URL_SCHEMES = ("http://", "https://", "s3://", "r2://")


def _is_workflow_url(value: str) -> bool:
    """值是完整可下载 URL 时返回 True；普通文本或相对路径不匹配。"""
    return isinstance(value, str) and value.startswith(_URL_SCHEMES)


def _collect_embedded_urls(data: Any) -> list[tuple[Any, str | int, str]]:
    """递归扫描嵌套结构，返回 (parent_container, key_or_index, url) 三元组列表。"""
    hits: list[tuple[Any, str | int, str]] = []
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and _is_workflow_url(value):
                hits.append((data, key, value))
            elif isinstance(value, (dict, list)):
                hits.extend(_collect_embedded_urls(value))
    elif isinstance(data, list):
        for idx, value in enumerate(data):
            if isinstance(value, str) and _is_workflow_url(value):
                hits.append((data, idx, value))
            elif isinstance(value, (dict, list)):
                hits.extend(_collect_embedded_urls(value))
    return hits


def _url_to_stable_filename(url: str) -> str:
    """
    基于 URL 生成稳定且不易冲突的本地文件名。
    格式: {hash8}_{basename}；无有效 basename 时回退到 {hash8}.bin。
    """
    parsed = urlparse(url)
    raw_basename = os.path.basename(parsed.path.rstrip("/")) if parsed.path else ""
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    if raw_basename:
        safe = raw_basename.replace("\\", "_")
        return f"{url_hash}_{safe}"
    return f"{url_hash}.bin"


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

async def _download_http_once(session: ClientSession, source_url: str, dest_path: Path) -> None:
    # 先写入临时文件，再原子 rename，避免半成品命中缓存。
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


async def _download_http(session: ClientSession, source_url: str, dest_path: Path) -> None:
    await _retry(
        _download_http_once, session, source_url, dest_path,
        label=f"http-download {source_url}",
    )


def _parse_object_store_url(raw_url: str, default_bucket: str) -> tuple[str, str]:
    parsed = urlparse(raw_url)
    bucket = parsed.netloc or default_bucket
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"invalid object url: {raw_url}")
    return bucket, key


async def _download_s3_once(
    raw_url: str,
    dest_path: Path,
    s3_client,
    bucket: str,
    key: str,
) -> None:
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    def _stream_download() -> None:
        s3_client.download_file(bucket, key, str(tmp_path))

    try:
        await asyncio.to_thread(_stream_download)
        if tmp_path.stat().st_size == 0:
            tmp_path.unlink(missing_ok=True)
            raise ValueError(f"downloaded empty file from {raw_url}")
        tmp_path.rename(dest_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


async def _download_s3(
    raw_url: str,
    dest_path: Path,
    s3_client,
    object_store: ObjectStoreConfig,
) -> None:
    if s3_client is None:
        raise RuntimeError("R2 upload/download is not configured")

    bucket, key = _parse_object_store_url(raw_url, object_store.bucket)
    await _retry(
        _download_s3_once, raw_url, dest_path, s3_client, bucket, key,
        label=f"s3-download {raw_url}",
    )


async def _ensure_asset(
    state: WorkerState,
    source_url: str,
    filename: str,
    input_dir: Path,
    s3_client,
    object_store: ObjectStoreConfig,
) -> None:
    dest_path = input_dir / filename

    if dest_path.exists():
        logger.info("cache HIT: %s (skip download)", filename)
        return

    async with _FileLock(filename):
        if dest_path.exists():
            logger.info("cache HIT (after lock): %s", filename)
            return

        logger.info("cache MISS: %s ← %s", filename, source_url)
        if source_url.startswith(("http://", "https://")):
            await _download_http(state.http_session, source_url, dest_path)
        elif source_url.startswith(("s3://", "r2://")):
            await _download_s3(source_url, dest_path, s3_client, object_store)
        else:
            raise ValueError(f"unsupported asset source: {source_url}")
        logger.info("downloaded: %s (%.1f KB)", filename, dest_path.stat().st_size / 1024)


async def prepare_assets(
    state: WorkerState,
    prompt_payload: dict[str, Any],
    assets: list[dict[str, Any]],
    s3_client,
    object_store: ObjectStoreConfig,
) -> dict[str, Any]:
    """下载并替换 prompt 里的输入引用，最终返回可直接提交到 ComfyUI 的 payload。"""
    prompt = copy.deepcopy(prompt_payload)
    input_dir = state.settings.input_root
    input_dir.mkdir(parents=True, exist_ok=True)

    if state.http_session is None:
        raise RuntimeError("HTTP session not initialized")

    # ---- Phase 1: 显式 input_assets ----
    logger.info("prepare_assets: phase1 input_assets=%d", len(assets))
    download_tasks: list[tuple[str, str, dict[str, Any]]] = []
    url_filename_map: dict[str, str] = {}

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
        url_filename_map[source_url] = filename

    if download_tasks:
        await asyncio.gather(
            *[
                _ensure_asset(state, url, fname, input_dir, s3_client, object_store)
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

    # ---- Phase 2: 扫描 prompt 内嵌 URL 并自动下载替换 ----
    embedded = _collect_embedded_urls(prompt)
    logger.info("prepare_assets: phase2 embedded_urls=%d", len(embedded))
    if embedded:
        new_downloads: dict[str, str] = {}
        for _, _, url in embedded:
            if url not in url_filename_map and url not in new_downloads:
                new_downloads[url] = _url_to_stable_filename(url)

        if new_downloads:
            logger.info("prepare_assets: phase2 new_downloads=%d", len(new_downloads))
            await asyncio.gather(
                *[
                    _ensure_asset(state, url, fname, input_dir, s3_client, object_store)
                    for url, fname in new_downloads.items()
                ]
            )
            url_filename_map.update(new_downloads)

        for parent, key, url in embedded:
            filename = url_filename_map.get(url)
            if filename:
                comfy_path = "/".join(
                    part for part in [state.settings.input_prefix, filename] if part
                )
                parent[key] = comfy_path

    return prompt


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

async def _upload_once(
    s3_client,
    bucket: str,
    key: str,
    local_path: Path,
) -> None:
    def _put_object() -> None:
        with local_path.open("rb") as file_obj:
            s3_client.put_object(Bucket=bucket, Key=key, Body=file_obj)

    await asyncio.to_thread(_put_object)


async def upload_output_if_needed(
    state: WorkerState,
    item: dict[str, Any],
    record: JobRecord,
) -> None:
    """按需上传输出文件，并把公开 URL 回填到 job 记录。"""
    filename = item.get("filename")
    if not filename:
        return

    if filename in record.uploaded_urls:
        logger.info("upload SKIP (already uploaded): %s", filename)
        return

    subfolder = item.get("subfolder") or ""
    local_path = state.settings.output_root / subfolder / filename
    if not local_path.exists():
        logger.warning("output file not found: %s", local_path)
        return

    if not record.object_store.uploads_enabled or record.s3_client is None:
        logger.info("upload SKIP (not configured): %s", filename)
        return

    ts = int(time.time())
    remote_filename = f"{ts}_{filename}"
    key = record.object_store.output_key(state.settings.node_id, remote_filename)

    logger.info(
        "upload START: %s → bucket=%s key=%s",
        local_path, record.object_store.bucket, key,
    )

    await _retry(
        _upload_once,
        record.s3_client,
        record.object_store.bucket,
        key,
        local_path,
        label=f"s3-upload {filename}",
    )

    public_url = record.object_store.public_url(state.settings.node_id, remote_filename)
    if public_url:
        record.uploaded_urls[filename] = public_url
        logger.info("upload DONE: %s → %s", filename, public_url)
    else:
        logger.info("upload DONE: %s (no public_base_url configured)", filename)


def iter_output_items(history_data: dict[str, Any]) -> list[dict[str, Any]]:
    """从 history.outputs 中提取可上传的 output 类型媒体条目。"""
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


async def cleanup_job(record: JobRecord) -> None:
    """异步清理单任务临时目录。"""
    def _cleanup() -> None:
        shutil.rmtree(record.temp_dir, ignore_errors=True)

    await asyncio.to_thread(_cleanup)
