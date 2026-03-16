import asyncio
import copy
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from aiohttp import ClientSession
from botocore.config import Config as BotoConfig

from config import JobRecord, ObjectStoreConfig, Settings, WorkerState, logger

_download_locks: dict[str, asyncio.Lock] = {}
_download_locks_guard = asyncio.Lock()


async def _get_file_lock(filename: str) -> asyncio.Lock:
    async with _download_locks_guard:
        if filename not in _download_locks:
            _download_locks[filename] = asyncio.Lock()
        return _download_locks[filename]


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
        return None

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


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

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


async def _download_s3(
    raw_url: str,
    dest_path: Path,
    s3_client,
    object_store: ObjectStoreConfig,
) -> None:
    if s3_client is None:
        raise RuntimeError("R2 upload/download is not configured")

    bucket, key = _parse_object_store_url(raw_url, object_store.bucket)
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

    lock = await _get_file_lock(filename)
    async with lock:
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

    return prompt


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

async def upload_output_if_needed(
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

    if not record.object_store.uploads_enabled or record.s3_client is None:
        return

    ts = int(time.time())
    remote_filename = f"{ts}_{filename}"
    key = record.object_store.output_key(state.settings.node_id, remote_filename)

    def _put_object() -> None:
        with local_path.open("rb") as file_obj:
            record.s3_client.put_object(
                Bucket=record.object_store.bucket,
                Key=key,
                Body=file_obj,
            )

    await asyncio.to_thread(_put_object)
    public_url = record.object_store.public_url(state.settings.node_id, remote_filename)
    if public_url:
        record.uploaded_urls[filename] = public_url


def iter_output_items(history_data: dict[str, Any]) -> list[dict[str, Any]]:
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
    def _cleanup() -> None:
        shutil.rmtree(record.temp_dir, ignore_errors=True)

    await asyncio.to_thread(_cleanup)
