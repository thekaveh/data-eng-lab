#!/usr/bin/env bash
# Shared helpers for data-eng-lab bootstrap scripts. Source this file.

log() { printf '\033[1;36m[data-eng-lab]\033[0m %s\n' "$*" >&2; }
err() { printf '\033[1;31m[data-eng-lab]\033[0m %s\n' "$*" >&2; }

# envval KEY FILE -> prints the value of the last KEY= line, or empty.
envval() {
  local key="$1" file="$2"
  [ -f "$file" ] || return 0
  grep -E "^${key}=" "$file" | tail -n1 | cut -d= -f2-
}

# set_env KEY VALUE FILE -> idempotent upsert (replaces existing or appends).
set_env() {
  local key="$1" value="$2" file="$3"
  touch "$file"
  if grep -qE "^${key}=" "$file"; then
    # portable in-place edit (BSD + GNU sed): use a temp file
    grep -vE "^${key}=" "$file" > "${file}.tmp" || true
    printf '%s=%s\n' "$key" "$value" >> "${file}.tmp"
    mv "${file}.tmp" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

# set_env_default KEY VALUE FILE -> set only if KEY absent.
set_env_default() {
  local key="$1" value="$2" file="$3"
  touch "$file"
  grep -qE "^${key}=" "$file" || printf '%s=%s\n' "$key" "$value" >> "$file"
}

# wait_healthy SERVICE... -> polls until each compose service (of project
# PROJECT_NAME) is healthy/Up, or times out (HEALTH_TIMEOUT secs, default 300).
# Override the probe with HEALTH_PROBE="my_probe_fn" for tests.
_default_health_probe() {
  local svc="$1"
  docker ps \
    --filter "label=com.docker.compose.project=${PROJECT_NAME:-data-eng-lab}" \
    --filter "label=com.docker.compose.service=${svc}" \
    --format '{{.Status}}' | head -n1
}

wait_healthy() {
  local probe="${HEALTH_PROBE:-_default_health_probe}"
  local timeout="${HEALTH_TIMEOUT:-300}"
  local start now status svc
  start="${SECONDS}"
  for svc in "$@"; do
    while true; do
      status="$("$probe" "$svc")"
      case "$status" in
        *healthy*|Up*) log "service '$svc' ready ($status)"; break ;;
      esac
      now="${SECONDS}"
      if [ $(( now - start )) -ge "$timeout" ]; then
        err "timeout waiting for '$svc' (last status: '${status:-none}')"
        return 1
      fi
      sleep 3
    done
  done
}
