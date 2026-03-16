import asyncio
import json
import shutil
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
    state: WorkerState = request.app["state"]
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
            items = iter_output_items(history_data)
            try:
                for item in items:
                    item.setdefault("prompt_id", prompt_id)
                    await upload_output_if_needed(state, item, record)
                record.uploads_completed = True
                await cleanup_job(record)
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
    web.run_app(create_app(), host=Settings().host, port=Settings().port)
