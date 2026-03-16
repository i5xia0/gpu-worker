#!/bin/bash
# 存储监控与清理脚本（Worker 配套，独立运行，不并入 app.py）
# 用法:
#   ./worker/scripts/storage-guard.sh status
#   ./worker/scripts/storage-guard.sh cleanup
#   ./worker/scripts/storage-guard.sh watch
# 容器内: bash /workspace/worker/scripts/storage-guard.sh cleanup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(basename "$(cd "$SCRIPT_DIR/.." && pwd)")"

if [ "$(basename "$SCRIPT_DIR")" = "scripts" ] && [ "$PARENT_DIR" = "worker" ]; then
  COMFYUI_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
elif [ "$(basename "$SCRIPT_DIR")" = "scripts" ]; then
  COMFYUI_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
  COMFYUI_ROOT="$SCRIPT_DIR"
fi

cd "$COMFYUI_ROOT"

if [ -f "$COMFYUI_ROOT/.env" ]; then
  # shellcheck disable=SC1091
  source "$COMFYUI_ROOT/.env"
fi

MODE="${1:-status}"

TARGET_PATH="${STORAGE_GUARD_TARGET_PATH:-$COMFYUI_ROOT/media}"
OUTPUT_SUBDIR="${STORAGE_GUARD_OUTPUT_SUBDIR:-s3-g0}"
OUTPUT_PATH="$TARGET_PATH/$OUTPUT_SUBDIR"

INPUT_MAX_AGE_DAYS="${STORAGE_GUARD_INPUT_MAX_AGE_DAYS:-3}"
OUTPUT_MAX_AGE_DAYS="${STORAGE_GUARD_OUTPUT_MAX_AGE_DAYS:-7}"
HIGH_WATERMARK_PERCENT="${STORAGE_GUARD_HIGH_WATERMARK_PERCENT:-80}"
LOW_WATERMARK_PERCENT="${STORAGE_GUARD_LOW_WATERMARK_PERCENT:-65}"
WATCH_INTERVAL_SECONDS="${STORAGE_GUARD_WATCH_INTERVAL_SECONDS:-300}"
DRY_RUN="${STORAGE_GUARD_DRY_RUN:-false}"

log() {
  printf '[storage-guard] %s\n' "$*"
}

usage_percent() {
  df -P "$TARGET_PATH" | awk 'NR==2 {gsub("%", "", $5); print $5}'
}

file_count() {
  local path="$1"
  if [ ! -d "$path" ]; then
    echo 0
    return
  fi
  find "$path" -type f | wc -l | tr -d ' '
}

dir_size() {
  local path="$1"
  if [ ! -d "$path" ]; then
    echo "0"
    return
  fi
  du -sh "$path" 2>/dev/null | awk '{print $1}'
}

input_file_count() {
  if [ ! -d "$TARGET_PATH" ]; then
    echo 0
    return
  fi
  find "$TARGET_PATH" -maxdepth 1 -type f | wc -l | tr -d ' '
}

input_size() {
  if [ ! -d "$TARGET_PATH" ]; then
    echo "0"
    return
  fi

  local tmp_file
  tmp_file="$(mktemp)"
  find "$TARGET_PATH" -maxdepth 1 -type f -print0 > "$tmp_file"
  if [ ! -s "$tmp_file" ]; then
    rm -f "$tmp_file"
    echo "0"
    return
  fi

  xargs -0 du -ch < "$tmp_file" 2>/dev/null | awk 'END {print $1}'
  rm -f "$tmp_file"
}

delete_file() {
  local file_path="$1"
  if [ "$DRY_RUN" = "true" ]; then
    log "[dry-run] would delete $file_path"
    return
  fi

  rm -f "$file_path"
  log "deleted $file_path"
}

cleanup_by_age() {
  if [ -d "$TARGET_PATH" ]; then
    while IFS= read -r -d '' file_path; do
      delete_file "$file_path"
    done < <(find "$TARGET_PATH" -maxdepth 1 -type f -mtime +"$INPUT_MAX_AGE_DAYS" -print0)
  fi

  if [ -d "$OUTPUT_PATH" ]; then
    while IFS= read -r -d '' file_path; do
      delete_file "$file_path"
    done < <(find "$OUTPUT_PATH" -type f -mtime +"$OUTPUT_MAX_AGE_DAYS" -print0)
  fi
}

oldest_input_files() {
  if [ ! -d "$TARGET_PATH" ]; then
    return
  fi
  find "$TARGET_PATH" -maxdepth 1 -type f -printf '%T@ %p\n' | sort -n
}

oldest_output_files() {
  if [ ! -d "$OUTPUT_PATH" ]; then
    return
  fi
  find "$OUTPUT_PATH" -type f -printf '%T@ %p\n' | sort -n
}

aggressive_cleanup() {
  local current_usage
  current_usage="$(usage_percent)"
  if [ "$current_usage" -lt "$HIGH_WATERMARK_PERCENT" ]; then
    log "disk usage ${current_usage}% is below high watermark ${HIGH_WATERMARK_PERCENT}%"
    return
  fi

  log "disk usage ${current_usage}% reached high watermark, starting aggressive cleanup"

  while IFS= read -r line; do
    current_usage="$(usage_percent)"
    if [ "$current_usage" -le "$LOW_WATERMARK_PERCENT" ]; then
      log "disk usage dropped to ${current_usage}%, stop output cleanup"
      break
    fi
    file_path="${line#* }"
    if [ -n "$file_path" ]; then
      delete_file "$file_path"
    fi
  done < <(oldest_output_files)

  current_usage="$(usage_percent)"
  if [ "$current_usage" -le "$LOW_WATERMARK_PERCENT" ]; then
    return
  fi

  log "output cleanup not enough, continue with input cleanup"
  while IFS= read -r line; do
    current_usage="$(usage_percent)"
    if [ "$current_usage" -le "$LOW_WATERMARK_PERCENT" ]; then
      log "disk usage dropped to ${current_usage}%, stop input cleanup"
      break
    fi
    file_path="${line#* }"
    if [ -n "$file_path" ]; then
      delete_file "$file_path"
    fi
  done < <(oldest_input_files)
}

print_status() {
  log "target_path=$TARGET_PATH"
  log "output_path=$OUTPUT_PATH"
  log "input_max_age_days=$INPUT_MAX_AGE_DAYS output_max_age_days=$OUTPUT_MAX_AGE_DAYS"
  log "high_watermark=${HIGH_WATERMARK_PERCENT}% low_watermark=${LOW_WATERMARK_PERCENT}% dry_run=$DRY_RUN"
  echo
  df -h "$TARGET_PATH"
  echo
  printf 'input files:  %s\n' "$(input_file_count)"
  printf 'input size:   %s\n' "$(input_size)"
  printf 'output files: %s\n' "$(file_count "$OUTPUT_PATH")"
  printf 'output size:  %s\n' "$(dir_size "$OUTPUT_PATH")"
}

run_cleanup() {
  print_status
  echo
  log "starting age-based cleanup"
  cleanup_by_age
  log "starting watermark-based cleanup"
  aggressive_cleanup
  echo
  print_status
}

watch_loop() {
  while true; do
    run_cleanup
    echo
    log "sleep ${WATCH_INTERVAL_SECONDS}s before next round"
    sleep "$WATCH_INTERVAL_SECONDS"
  done
}

case "$MODE" in
  status)
    print_status
    ;;
  cleanup)
    run_cleanup
    ;;
  watch)
    watch_loop
    ;;
  *)
    echo "用法: $0 [status|cleanup|watch]"
    exit 1
    ;;
esac
