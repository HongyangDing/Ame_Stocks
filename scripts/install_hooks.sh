#!/usr/bin/env bash
set -Eeuo pipefail

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

git config core.hooksPath .githooks
printf 'Configured core.hooksPath=.githooks for %s\n' "$repository_root"
