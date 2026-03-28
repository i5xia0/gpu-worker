# GPU Worker

这个目录提供一个 `ComfyUI` 兼容代理服务，目标是让调度端继续按原有方式访问：

- `POST /prompt`
- `GET /history/{prompt_id}`
- `GET /queue`
- `GET /system_stats`
- `GET /view?...`
- `GET /ws?clientId=...`

代理层会把请求转发到本机真实 `ComfyUI`，并在输入输出两端补上对象存储逻辑。

## 运行方式

推荐直接通过仓库根目录的 `docker compose up -d` 启动，`gpu-worker` 会监听：

```text
http://<host>:8190
```

## 输入资产协议

`/prompt` 在兼容原始 ComfyUI 请求体的基础上，额外支持：

```json
{
  "client_id": "scheduler-client-id",
  "prompt": {},
  "s3": {
    "endpoint_url": "https://<account>.r2.cloudflarestorage.com",
    "region": "auto",
    "bucket_name": "ai-videos",
    "access_key_id": "<access-key>",
    "secret_access_key": "<secret-key>",
    "public_base_url": "https://r2.example.com",
    "path": "uploads"
  },
  "input_assets": [
    {
      "url": "https://example.com/input.png",
      "filename": "input.png",
      "node_id": "12",
      "input_name": "image"
    }
  ]
}
```

字段说明：

- `url` / `source_url` / `r2_url` / `object_url`：输入资源地址
- `filename`：落地文件名
- `node_id`：要注入的 workflow 节点 ID
- `input_name`：该节点的输入字段名
- `s3` / `r2`：当前任务使用的对象存储配置，支持 `endpoint_url`、`region`、`bucket_name`、`access_key_id`、`secret_access_key`、`public_base_url`、`path`

`s3` / `r2` 既可以放在顶层，也可以放在 `extra_data` 里。Worker 会按以下优先级取值：

1. 当前请求中的 `s3` / `r2`
2. 环境变量默认值（`R2_*`、`WORKER_OUTPUT_PREFIX`）

如果没有提供 `node_id + input_name`，Worker 会尝试按以下占位值在整个 workflow 里做字符串替换：

- `placeholder`
- `logical_name`
- `original_value`
- `filename`

输入文件默认直接缓存在 `media` 根目录，也就是 ComfyUI 的 input 根目录。
当上游对同一个资源始终传递稳定文件名时，Worker 会先检查 `media/<filename>` 是否已存在，命中则跳过下载。

## Workflow 节点内嵌 URL

除了通过 `input_assets` 显式声明输入资源外，Worker 还支持**在 workflow 节点中直接填写完整 URL**。Worker 会在提交到 ComfyUI 前自动发现、下载并替换这些 URL。

支持的 URL 协议：`http://`、`https://`、`s3://`、`r2://`。

示例 — 节点直接写 URL：

```json
{
  "prompt": {
    "12": {
      "class_type": "LoadImage",
      "inputs": {
        "image": "https://cdn.example.com/photos/input.jpg"
      }
    },
    "15": {
      "class_type": "LoadImage",
      "inputs": {
        "image": "r2://ai-videos/uploads/ref_photo.png"
      }
    }
  }
}
```

Worker 处理后，实际提交给 ComfyUI 的 `prompt` 会变成：

```json
{
  "12": {
    "class_type": "LoadImage",
    "inputs": {
      "image": "a1b2c3d4_input.jpg"
    }
  },
  "15": {
    "class_type": "LoadImage",
    "inputs": {
      "image": "e5f6a7b8_ref_photo.png"
    }
  }
}
```

### 处理优先级

1. **`input_assets`（显式声明）**：优先处理，使用调用方指定的 `filename` 和 `node_id + input_name` 定位
2. **Workflow 内嵌 URL（自动发现）**：在 `input_assets` 处理完成后，递归扫描 `prompt` 中剩余的 URL 字符串进行下载替换
3. 已被 `input_assets` 处理过的 URL 不会重复下载

### 文件名策略

内嵌 URL 的本地文件名格式为 `{hash8}_{basename}`，其中 `hash8` 是 URL 的 MD5 前 8 位。这确保：

- **稳定性**：同一 URL 始终映射到同一文件名，可命中缓存
- **防冲突**：不同 URL 即使 basename 相同也不会覆盖
- **可读性**：文件名保留原始 basename，便于排查

### 注意事项

- 只有字符串值整体为 URL 时才会被识别，嵌在文本中的 URL 不受影响（如 prompt 文本 `"visit https://example.com"` 不会被替换）
- `s3://` / `r2://` 形式的 URL 需要请求体中提供对应的 `s3` / `r2` 配置，或通过环境变量 `R2_*` 配置
- 与 `input_assets` 共享相同的缓存目录、文件锁和重试策略

## 输出上传约定

Worker 在访问 `/history/{prompt_id}` 时，会把输出文件上传到：

```text
<path>/<WORKER_NODE_ID>/<unix_timestamp>_<filename>
```

例如：

```text
uploads/s3-g0/1709510400_ComfyUI_00001_.mp4
```

其中：

- `path` 优先取请求里的 `s3.path` / `r2.path`
- 未传时回退到环境变量 `WORKER_OUTPUT_PREFIX`
- 公网访问前缀优先取请求里的 `public_base_url`
- 未传时回退到环境变量 `R2_PUBLIC_BASE_URL`

`/history/{prompt_id}` 的返回结构与 ComfyUI 基本一致，Worker 只会在任务命中时附加一个 `worker` 字段：

```json
{
  "07520bca-43bd-4f7c-bf0b-d817d969c1d6": {
    "outputs": {
      "node_x": {
        "images": [
          {
            "filename": "ComfyUI_00001_.png",
            "subfolder": "",
            "type": "output"
          }
        ]
      }
    },
    "status": {
      "status_str": "success"
    },
    "worker": {
      "node_id": "s3-g0",
      "uploaded_urls": {
        "ComfyUI_00001_.png": "https://r2.example.com/uploads/s3-g0/1709510400_ComfyUI_00001_.png"
      }
    }
  }
}
```

说明：

- `worker.node_id`：当前 Worker 节点标识（`WORKER_NODE_ID`）
- `worker.uploaded_urls`：`原始输出文件名 -> 上传后公网 URL` 映射
- 未命中 Worker 任务记录时，不会附加 `worker` 字段

当任务配置了对象存储上传，且 ComfyUI 已产出可上传文件但 Worker 仍在后台上传时，`/history/{prompt_id}` 会先返回：

```json
{}
```

等上传完成后，再返回完整的 ComfyUI history 结果以及 `worker.uploaded_urls`。

**性能优化**：上传进行中时，Worker 会直接返回 `{}`，不再每次都向 ComfyUI 请求完整 history。上传完成后，Worker 会缓存 history 结果，后续轮询直接返回缓存。上传失败超过 3 次后，Worker 会标记任务为已完成（可能只含部分 URL），防止无限重试。

上传完成后，Worker 不会立刻删除内存中的任务记录，而是会继续保留一段时间，方便调度端或其他消费者重复查询同一个 `prompt_id` 时仍能拿到 `worker.uploaded_urls`。
保留时长由环境变量 `WORKER_HISTORY_RETENTION_SECONDS` 控制，默认值为 `3600` 秒。

要与现有调度端兼容，调度端拼接结果 URL 时使用的前缀必须和这个 key 前缀保持一致；如果 `scheduler` 已优先读取 `worker.uploaded_urls`，则可以直接使用 Worker 回传的最终 URL。

## WebSocket 代理

`GET /ws?clientId=...` 提供 ComfyUI WebSocket 的双向代理。为减少传输开销，代理层会对 ComfyUI 发出的事件做过滤和裁剪：

- **二进制帧（预览图）**：完全丢弃，不转发给上游。调度端通常不需要实时预览。
- **事件字段裁剪**：只保留调度端实际消费的字段。例如 `executed` 事件会去掉 `output` 载荷，`execution_error` 会去掉 `current_inputs` / `current_outputs`。
- **高频消息节流**：`crystools.monitor` 最多每 5 秒转发一次；`progress` 最多每 3 秒转发一次（最后一步始终转发）。
- **生命周期事件**：`executing`、`executed`、`execution_start`、`execution_cached`、`execution_error`、`execution_success`、`execution_interrupted` 始终转发，并记录 INFO 日志。

上游 -> ComfyUI 方向的消息保持透传，不做过滤。

## 文件查看代理

`GET /view?...` 会把请求透传到 ComfyUI 的 `/view`，用于读取 `input`、`output`、`temp` 等目录下的图片、视频和其他媒体文件。

为避免大文件读取时把整个响应一次性加载到 Worker 内存中，`gpu-worker` 现在会对 `/view` 使用流式透传：一边从 ComfyUI 读取，一边回写给上游，同时保留常见响应头（如 `Content-Type`、`Content-Length`、`Content-Disposition`）。

## 日志与排查

Worker 的日志级别由环境变量 `WORKER_LOG_LEVEL` 控制，默认 `INFO`，可设为 `DEBUG` 获取更详细输出。

```bash
WORKER_LOG_LEVEL=DEBUG
```

### 关键日志关键词

在容器日志中搜索以下关键词即可快速定位流程状态：

| 关键词 | 含义 |
|--------|------|
| `POST /prompt` | 收到提交请求，会打印 `client_id`、`input_assets` 数量、是否带 `s3` 配置 |
| `prepare_assets: phase1` | 开始处理显式 `input_assets`，打印资产数量 |
| `prepare_assets: phase2` | 开始扫描 workflow 内嵌 URL，打印发现的 URL 数量 |
| `cache HIT` | 输入文件已存在，跳过下载 |
| `cache MISS` | 输入文件不存在，开始下载 |
| `downloaded` | 下载完成，打印文件名和大小 |
| `assets prepared` | 全部输入准备完毕 |
| `job registered` | 任务已注册，打印 `prompt_id` 和是否启用上传 |
| `history:` | `/history` 请求处理状态，包含输出文件数量和上传任务状态 |
| `upload START` | 开始上传单个文件，打印本地路径、bucket、remote key |
| `upload DONE` | 单个文件上传成功，打印最终公网 URL |
| `upload SKIP` | 文件上传跳过（已上传 / 未配置 / 文件不存在） |
| `upload BATCH START` | 后台上传任务启动，打印待上传文件列表 |
| `upload BATCH DONE` | 后台上传任务全部完成 |
| `upload BATCH FAILED` | 后台上传任务出现异常（会显示当前重试次数） |
| `upload BATCH FAILED permanently` | 上传重试达到上限，标记任务为已完成 |
| `short-circuit` | 上传进行中，`/history` 请求被短路（未请求 ComfyUI） |
| `returning cached result` | 上传完成后从缓存返回 history |
| `ws open` | WebSocket 代理连接建立 |
| `ws closed` | WebSocket 代理连接关闭 |
| `ws event` | WebSocket 事件转发（生命周期事件为 INFO，其余为 DEBUG） |
| `s3 client: OK` | S3/R2 客户端初始化成功，打印 endpoint 和 bucket |
| `s3 client: SKIP` | S3/R2 客户端未创建（配置缺失） |
| `retry` | 下载或上传触发了重试 |

### 排查示例

确认请求是否到达 Worker：

```bash
docker logs comfyui-gpu-worker 2>&1 | grep "POST /prompt"
```

确认输入下载是否命中缓存：

```bash
docker logs comfyui-gpu-worker 2>&1 | grep -E "cache (HIT|MISS)"
```

确认输出上传是否完成：

```bash
docker logs comfyui-gpu-worker 2>&1 | grep -E "upload (START|DONE|SKIP|BATCH)"
```

查看某个 prompt 的完整流程：

```bash
docker logs comfyui-gpu-worker 2>&1 | grep "<prompt_id>"
```

## 存储清理（独立脚本）

本目录下的 `scripts/storage-guard.sh` 用于监控 `media` 占用并按年龄与水位线清理旧输入/输出文件，**不并入 Worker 主进程**，需单独触发。

- 宿主机（项目根）：`./worker/scripts/storage-guard.sh [status|cleanup|watch]`
- 容器内：`bash /workspace/worker/scripts/storage-guard.sh cleanup`

线上 GPU 建议用 cron 或 systemd timer 定时执行 `cleanup`，例如 `0 * * * *` 每小时一次；配置见项目根 `.env` 中的 `STORAGE_GUARD_*` 变量。
