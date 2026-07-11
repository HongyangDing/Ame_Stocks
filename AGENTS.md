# Ame_Stocks repository instructions

## Mandatory progress synchronization

These rules apply to every task in this repository.

After changing any repository file:

1. Run verification appropriate to the change.
2. Stage every in-scope changed file and create a focused Git commit.
3. Do not finish or hand off with tracked or untracked task files left outside the commit.
4. Let the versioned `post-commit` hook push `main` to GitHub and fast-forward
   `/opt/american_stocks` on the remote server.
5. Verify that local `HEAD`, GitHub `origin/main`, and remote `HEAD` are identical.

The hook must fail closed: never force-push, never merge automatically, never reset either
checkout, and never touch runtime data, Docker, Caddy, or the legacy Mogikabu project.

If synchronization fails, report the exact failed stage and leave the commit intact locally.
Repair the connection or remote worktree, then rerun `scripts/sync_progress.sh`. Do not use
`AME_STOCKS_SKIP_PROGRESS_SYNC=1` unless the user explicitly requests an unsynchronized
emergency commit.

Do not edit deployed source files directly. All source changes originate in the local checkout,
go through GitHub, and reach the server through `git pull --ff-only`.
