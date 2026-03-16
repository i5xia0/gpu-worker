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
<WORKER_OUTPUT_PREFIX>/<WORKER_NODE_ID>/<unix_timestamp>_<filename>
```

例如：

```text
uploads/s3-g0/1709510400_ComfyUI_00001_.mp4
```

要与现有调度端兼容，调度端拼接结果 URL 时使用的前缀必须和这个 key 前缀保持一致。

## 存储清理（独立脚本）

本目录下的 `scripts/storage-guard.sh` 用于监控 `media` 占用并按年龄与水位线清理旧输入/输出文件，**不并入 Worker 主进程**，需单独触发。

- 宿主机（项目根）：`./worker/scripts/storage-guard.sh [status|cleanup|watch]`
- 容器内：`bash /workspace/worker/scripts/storage-guard.sh cleanup`

线上 GPU 建议用 cron 或 systemd timer 定时执行 `cleanup`，例如 `0 * * * *` 每小时一次；配置见项目根 `.env` 中的 `STORAGE_GUARD_*` 变量。
