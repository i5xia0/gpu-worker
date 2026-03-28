import asyncio
import json
import shutil
import time
import uuid

from aiohttp import WSMsgType, web

from config import JobRecord, Settings, WorkerState, logger
from storage import (
    build_s3_client,
    cleanup_job,
    iter_output_items,
    normalize_assets,
    prepare_assets,
    resolve_object_store_config,
    upload_output_if_needed,
)

_MAX_UPLOAD_ATTEMPTS = 3


async def _run_output_uploads(
    state: WorkerState,
    record: JobRecord,
    prompt_id: str,
    items: list[dict],
) -> None:
    filenames = [it.get("filename", "?") for it in items]
    logger.info(
        "upload BATCH START: prompt=%s attempt=%d files=%s",
        prompt_id, record.upload_attempts, filenames,
    )
    try:
        for item in items:
            item.setdefault("prompt_id", prompt_id)
            await upload_output_if_needed(state, item, record)
        record.uploads_completed = True
        record.completed_at = time.time()
        logger.info(
            "upload BATCH DONE: prompt=%s uploaded_urls=%s",
            prompt_id, list(record.uploaded_urls.keys()),
        )
        await cleanup_job(record)
    except Exception:
        record.upload_attempts += 1
        if record.upload_attempts >= _MAX_UPLOAD_ATTEMPTS:
            logger.error(
                "upload BATCH FAILED permanently: prompt=%s attempts=%d, marking completed",
                prompt_id, record.upload_attempts,
            )
            record.uploads_completed = True
            record.completed_at = time.time()
            await cleanup_job(record)
        else:
            logger.warning(
                "upload BATCH FAILED: prompt=%s attempt=%d/%d, will retry on next poll",
                prompt_id, record.upload_attempts, _MAX_UPLOAD_ATTEMPTS,
            )
    finally:
        record.upload_task = None


async def _proxy_json(request: web.Request, path: str) -> web.Response:
    """透传 ComfyUI 的只读 JSON 接口（如 queue/system_stats）。"""
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


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

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
    """接收上游任务，完成资产准备后转发到 ComfyUI /prompt。"""
    state: WorkerState = request.app["state"]
    await state.release_expired_jobs()
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    payload = await request.json()
    client_id = payload.get("client_id") or payload.get("clientId") or uuid.uuid4().hex
    raw_extra_data = payload.get("extra_data")
    extra_data = dict(raw_extra_data) if isinstance(raw_extra_data, dict) else {}
    object_store_raw = (
        payload.pop("r2", None)
        or payload.pop("s3", None)
        or extra_data.pop("r2", None)
        or extra_data.pop("s3", None)
    )
    object_store = resolve_object_store_config(object_store_raw, state.settings)
    s3_client = build_s3_client(object_store)
    if extra_data:
        payload["extra_data"] = extra_data
    else:
        payload.pop("extra_data", None)
    input_assets = normalize_assets(
        payload.pop("input_assets", None)
        or payload.pop("assets", None)
        or extra_data.get("input_assets")
    )

    prompt_payload = payload.get("prompt")
    if not isinstance(prompt_payload, dict):
        raise web.HTTPBadRequest(text="`prompt` must be an object")

    logger.info(
        "POST /prompt: client_id=%s input_assets=%d has_s3_config=%s nodes=%d",
        client_id, len(input_assets), object_store_raw is not None, len(prompt_payload),
    )

    temp_id = uuid.uuid4().hex
    temp_dir = state.settings.tmp_root / temp_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        resolved_prompt = await prepare_assets(
            state,
            prompt_payload,
            input_assets,
            s3_client,
            object_store,
        )
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.exception("failed to prepare assets")
        raise web.HTTPBadRequest(text=f"failed to prepare assets: {exc}") from exc

    logger.info("assets prepared: client_id=%s resolved_nodes=%d", client_id, len(resolved_prompt))

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
                object_store=object_store,
                s3_client=s3_client,
            )
        logger.info(
            "job registered: prompt_id=%s client_id=%s uploads_enabled=%s",
            prompt_id, client_id, object_store.uploads_enabled,
        )

    return web.json_response(data)


async def history(request: web.Request) -> web.Response:
    """读取 ComfyUI history，并在首次命中时执行输出上传与临时目录清理。

    优化策略：
    - 上传进行中直接返回 {}，不再每次都请求 ComfyUI。
    - 上传完成后缓存 history，后续请求直接返回缓存结果。
    - upload_task 的创建在 jobs_lock 内完成，避免并发重复启动。
    - 上传失败超过 _MAX_UPLOAD_ATTEMPTS 次后标记为已完成，防止无限重试。
    """
    state: WorkerState = request.app["state"]
    await state.release_expired_jobs()
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    prompt_id = request.match_info["prompt_id"]

    # ---- Fast path: check in-memory state before hitting ComfyUI ----
    async with state.jobs_lock:
        record = state.jobs.get(prompt_id)

    if record is not None:
        if record.upload_task is not None:
            logger.debug("history: prompt=%s upload in progress, short-circuit", prompt_id)
            return web.json_response({})

        if record.uploads_completed and record.cached_history is not None:
            history_payload = record.cached_history
            history_data = history_payload.get(prompt_id)
            if isinstance(history_data, dict):
                history_data["worker"] = {
                    "node_id": state.settings.node_id,
                    "uploaded_urls": dict(record.uploaded_urls),
                }
            logger.info(
                "history: prompt=%s returning cached result with uploaded_urls=%d",
                prompt_id, len(record.uploaded_urls),
            )
            return web.json_response(history_payload)

    # ---- Normal path: fetch from ComfyUI ----
    target_url = f"{state.settings.comfyui_base_url}/history/{prompt_id}"
    async with state.http_session.get(target_url) as response:
        raw = await response.read()
        if response.status != 200:
            return web.Response(status=response.status, body=raw, content_type=response.content_type)

    history_payload = json.loads(raw.decode("utf-8"))
    history_data = history_payload.get(prompt_id)

    if isinstance(history_data, dict):
        if record is None:
            async with state.jobs_lock:
                record = state.jobs.get(prompt_id)

        if not record:
            logger.debug("history: prompt=%s no job record found", prompt_id)

        if record and not record.uploads_completed:
            items = iter_output_items(history_data)
            uploads_required = (
                bool(items)
                and record.object_store.uploads_enabled
                and record.s3_client is not None
            )

            logger.info(
                "history: prompt=%s output_items=%d uploads_required=%s upload_task_running=%s",
                prompt_id, len(items), uploads_required, record.upload_task is not None,
            )

            if uploads_required:
                async with state.jobs_lock:
                    if record.upload_task is None:
                        record.cached_history = history_payload
                        record.upload_task = asyncio.create_task(
                            _run_output_uploads(state, record, prompt_id, items)
                        )
                        logger.info("history: prompt=%s upload task started", prompt_id)
                    else:
                        logger.info("history: prompt=%s upload task already running", prompt_id)
                return web.json_response({})

            record.uploads_completed = True
            record.completed_at = time.time()
            record.cached_history = history_payload
            if not items:
                logger.info("history: prompt=%s no output items, marking completed", prompt_id)
                await cleanup_job(record)

        if record and record.uploads_completed:
            if record.cached_history is None:
                record.cached_history = history_payload
            logger.info(
                "history: prompt=%s returning with uploaded_urls=%d",
                prompt_id, len(record.uploaded_urls),
            )

        if record:
            history_data["worker"] = {
                "node_id": state.settings.node_id,
                "uploaded_urls": dict(record.uploaded_urls),
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
        headers = {}
        for header_name in (
            "Content-Type",
            "Content-Length",
            "Content-Disposition",
            "Cache-Control",
            "ETag",
            "Last-Modified",
        ):
            value = response.headers.get(header_name)
            if value:
                headers[header_name] = value

        proxy_response = web.StreamResponse(status=response.status, headers=headers)
        await proxy_response.prepare(request)
        async for chunk in response.content.iter_chunked(1024 * 1024):
            await proxy_response.write(chunk)
        await proxy_response.write_eof()
        return proxy_response


# ---------------------------------------------------------------------------
# WebSocket proxy — filtering & throttling
# ---------------------------------------------------------------------------

_WS_LIFECYCLE_TYPES = frozenset({
    "executing",
    "executed",
    "execution_start",
    "execution_cached",
    "execution_error",
    "execution_success",
    "execution_interrupted",
})

_WS_MONITOR_INTERVAL = 5.0
_WS_PROGRESS_INTERVAL = 3.0


def _slim_ws_event(data: dict) -> str | None:
    """裁剪 ComfyUI WS 事件，只保留调度端实际消费的字段。

    丢弃上游不需要的大体积载荷（如 executed.output、
    execution_error.current_inputs/current_outputs），
    用紧凑 JSON 序列化减少传输字节数。
    返回 None 表示该消息应被丢弃。
    """
    msg_type = data.get("type", "")
    raw = data.get("data") or {}

    if msg_type == "executing":
        slim = {"prompt_id": raw.get("prompt_id", ""), "node": raw.get("node")}
    elif msg_type == "executed":
        slim = {"prompt_id": raw.get("prompt_id", ""), "node": raw.get("node", "")}
    elif msg_type == "progress":
        slim = {
            "prompt_id": raw.get("prompt_id", ""),
            "value": raw.get("value", 0),
            "max": raw.get("max", 0),
        }
    elif msg_type == "execution_cached":
        slim = {"prompt_id": raw.get("prompt_id", ""), "nodes": raw.get("nodes", [])}
    elif msg_type == "execution_error":
        slim = {
            "prompt_id": raw.get("prompt_id", ""),
            "node_id": raw.get("node_id", ""),
            "node_type": raw.get("node_type", ""),
            "exception_type": raw.get("exception_type", ""),
            "exception_message": raw.get("exception_message", ""),
            "traceback": raw.get("traceback", ""),
        }
    elif msg_type in ("execution_success", "execution_start", "execution_interrupted"):
        slim = {"prompt_id": raw.get("prompt_id", "")}
    elif msg_type == "crystools.monitor":
        gpus = raw.get("gpus")
        gpu0: dict = {}
        if isinstance(gpus, list) and gpus:
            gpu0 = gpus[0] if isinstance(gpus[0], dict) else {}
        slim = {
            "cpu_utilization": raw.get("cpu_utilization", 0),
            "ram_used_percent": raw.get("ram_used_percent", 0),
            "gpus": [{
                "gpu_utilization": gpu0.get("gpu_utilization", 0),
                "vram_total": gpu0.get("vram_total", 0),
                "vram_used": gpu0.get("vram_used", 0),
            }] if gpu0 else [],
        }
    else:
        slim = raw

    return json.dumps({"type": msg_type, "data": slim}, separators=(",", ":"))


async def websocket_proxy(request: web.Request) -> web.WebSocketResponse:
    """双向代理上游 <-> ComfyUI 的 WebSocket 事件流。

    优化策略：
    - 丢弃二进制帧（预览图），上游调度端不消费。
    - 按消息类型只保留调度端消费的字段，裁掉 output 等大载荷。
    - 对 crystools.monitor 和 progress 做时间节流。
    """
    state: WorkerState = request.app["state"]
    if state.http_session is None:
        raise web.HTTPServiceUnavailable(text="worker session not ready")

    client_id = request.query.get("clientId") or request.query.get("client_id")
    if not client_id:
        raise web.HTTPBadRequest(text="missing clientId")

    frontend_ws = web.WebSocketResponse(heartbeat=20)
    await frontend_ws.prepare(request)

    backend_url = (
        f"{state.settings.comfyui_base_url.replace('http://', 'ws://').replace('https://', 'wss://')}"
        f"/ws?clientId={client_id}"
    )
    logger.info("ws open: client_id=%s", client_id)
    try:
        backend_ws = await state.http_session.ws_connect(backend_url, heartbeat=20)
    except Exception as exc:
        logger.warning("ws backend connect failed: client_id=%s error=%s", client_id, exc)
        await frontend_ws.close(message=str(exc).encode("utf-8", errors="ignore"))
        return frontend_ws

    async def _relay_backend() -> None:
        """ComfyUI -> 上游：过滤、裁剪、节流后转发。"""
        last_monitor_ts = 0.0
        last_progress_ts = 0.0

        async for message in backend_ws:
            if message.type == WSMsgType.TEXT:
                try:
                    data = json.loads(message.data)
                except (json.JSONDecodeError, ValueError):
                    continue

                msg_type = data.get("type", "")
                msg_data = data.get("data") or {}

                if msg_type == "crystools.monitor":
                    now = time.monotonic()
                    if now - last_monitor_ts < _WS_MONITOR_INTERVAL:
                        continue
                    last_monitor_ts = now

                if msg_type == "progress":
                    now = time.monotonic()
                    value = msg_data.get("value", 0)
                    max_val = msg_data.get("max", 1)
                    is_final = value >= max_val
                    if not is_final and now - last_progress_ts < _WS_PROGRESS_INTERVAL:
                        continue
                    last_progress_ts = now

                compressed = _slim_ws_event(data)
                if compressed is None:
                    continue

                prompt_id = msg_data.get("prompt_id", "")
                if msg_type in _WS_LIFECYCLE_TYPES:
                    logger.info(
                        "ws event: client_id=%s type=%s prompt_id=%s node=%s",
                        client_id, msg_type, prompt_id,
                        msg_data.get("node", ""),
                    )
                elif msg_type == "progress":
                    logger.debug(
                        "ws event: client_id=%s type=progress prompt_id=%s %s/%s",
                        client_id, prompt_id,
                        msg_data.get("value", 0), msg_data.get("max", 0),
                    )
                elif msg_type:
                    logger.debug(
                        "ws event: client_id=%s type=%s prompt_id=%s",
                        client_id, msg_type, prompt_id,
                    )

                await frontend_ws.send_str(compressed)
            elif message.type == WSMsgType.BINARY:
                pass
            elif message.type == WSMsgType.ERROR:
                break

    async def _relay_frontend() -> None:
        """上游 -> ComfyUI：透传。"""
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

    try:
        done, pending = await asyncio.wait(relay_tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in done:
            exc = task.exception()
            if exc is not None:
                logger.warning("ws relay error: client_id=%s error=%s", client_id, exc)
    finally:
        if not backend_ws.closed:
            await backend_ws.close()
        if not frontend_ws.closed:
            await frontend_ws.close()
        logger.info("ws closed: client_id=%s", client_id)
    return frontend_ws


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

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
    app = create_app()
    settings: Settings = app["state"].settings
    web.run_app(app, host=settings.host, port=settings.port)
