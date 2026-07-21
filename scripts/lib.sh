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

# resolve_project_name [ENV_FILE] -> effective Docker project name.
# Precedence: exported $PROJECT_NAME > PROJECT_NAME in ENV_FILE > default 'data-eng-lab'.
resolve_project_name() {
  local env_file="${1:-}"
  if [ -n "${PROJECT_NAME:-}" ]; then
    printf '%s' "$PROJECT_NAME"
    return 0
  fi
  local from_env=""
  if [ -n "$env_file" ] && [ -f "$env_file" ]; then
    from_env="$(envval PROJECT_NAME "$env_file")"
  fi
  printf '%s' "${from_env:-data-eng-lab}"
}
