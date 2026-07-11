#!/usr/bin/env bash
set -Eeuo pipefail

readonly EXPECTED_BRANCH="main"
readonly EXPECTED_ORIGIN="git@github.com:HongyangDing/Ame_Stocks.git"
readonly DEPLOY_TARGET="root@188.245.240.94"
readonly DEPLOY_PATH="/opt/american_stocks"

fail() {
  printf 'Ame_Stocks progress sync failed: %s\n' "$1" >&2
  exit 1
}

if [[ "${AME_STOCKS_SKIP_PROGRESS_SYNC:-0}" == "1" ]]; then
  printf 'Ame_Stocks progress sync explicitly skipped.\n' >&2
  exit 0
fi

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

branch=$(git branch --show-current)
[[ "$branch" == "$EXPECTED_BRANCH" ]] || fail "expected branch $EXPECTED_BRANCH, found $branch"

origin=$(git remote get-url origin)
[[ "$origin" == "$EXPECTED_ORIGIN" ]] || fail "unexpected origin: $origin"

worktree_status=$(git status --porcelain --untracked-files=normal)
if [[ -n "$worktree_status" ]]; then
  printf '%s\n' "$worktree_status" >&2
  fail "worktree is not clean; commit every in-scope file before synchronization"
fi

local_head=$(git rev-parse HEAD)
if [[ "${AME_STOCKS_SYNC_DRY_RUN:-0}" == "1" ]]; then
  printf 'Dry run passed: branch=%s commit=%s deploy=%s:%s\n' \
    "$branch" "$local_head" "$DEPLOY_TARGET" "$DEPLOY_PATH"
  exit 0
fi

printf 'Pushing %s to GitHub...\n' "$local_head"
git push origin "$EXPECTED_BRANCH"

github_head=$(git ls-remote origin "refs/heads/$EXPECTED_BRANCH" | awk '{print $1}')
[[ "$github_head" == "$local_head" ]] || fail "GitHub head does not match local head"

remote_status=$(
  ssh -o BatchMode=yes -o ConnectTimeout=15 "$DEPLOY_TARGET" \
    "git -C '$DEPLOY_PATH' status --porcelain --untracked-files=no"
)
[[ -z "$remote_status" ]] || fail "remote checkout contains tracked changes"

printf 'Fast-forwarding %s:%s...\n' "$DEPLOY_TARGET" "$DEPLOY_PATH"
ssh -o BatchMode=yes -o ConnectTimeout=15 "$DEPLOY_TARGET" \
  "git -C '$DEPLOY_PATH' pull --ff-only origin '$EXPECTED_BRANCH'"

remote_head=$(
  ssh -o BatchMode=yes -o ConnectTimeout=15 "$DEPLOY_TARGET" \
    "git -C '$DEPLOY_PATH' rev-parse HEAD"
)
[[ "$remote_head" == "$local_head" ]] || fail "remote head does not match local head"

printf 'Progress synchronized: local=GitHub=remote=%s\n' "$local_head"
