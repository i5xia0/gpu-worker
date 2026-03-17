# GPU Worker

这个目录提供一个 `ComfyUI` 兼容代理服务，目标是让调度端继续按原有方式访问：

- `POST /prompt`
- `GET /history/{prompt_id}`
- `GET /queue`
- `GET /system_stats`
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

要与现有调度端兼容，调度端拼接结果 URL 时使用的前缀必须和这个 key 前缀保持一致；如果 `scheduler` 已优先读取 `worker.uploaded_urls`，则可以直接使用 Worker 回传的最终 URL。

## 存储清理（独立脚本）

本目录下的 `scripts/storage-guard.sh` 用于监控 `media` 占用并按年龄与水位线清理旧输入/输出文件，**不并入 Worker 主进程**，需单独触发。

- 宿主机（项目根）：`./worker/scripts/storage-guard.sh [status|cleanup|watch]`
- 容器内：`bash /workspace/worker/scripts/storage-guard.sh cleanup`

线上 GPU 建议用 cron 或 systemd timer 定时执行 `cleanup`，例如 `0 * * * *` 每小时一次；配置见项目根 `.env` 中的 `STORAGE_GUARD_*` 变量。
