# Changelog

## Unreleased

- Accept the managed Codex daemon's control-socket symlink for automatic
  return. Only a final-component link pointing directly at an absolute
  canonical socket is followed, and both the link's and the socket's
  directories must be owned by this user and not group/world-writable. Routes
  store the link and re-resolve it on every connect, so daemon restarts do not
  strand them. Restart the wakeup service to load the fix.
- Bind automatic-return registrations to the Codex conversation rather than to
  a shared checkout. Every registered task must have that conversation as its
  requester, and the route pins the conversation's own folder at registration,
  so lead channels kept in another checkout can register. A conversation that
  later moves fails closed until `wake rebind NAME --confirm-folder-change`
  re-pins it. Result notifications are limited to the conversation's own tasks.
- When Codex's sandbox hides the app server, `delegate --async` now exits
  non-zero with the escalation and `wake register` commands to use, instead of
  a bare permission error. It does not fall back silently to polling.

## 1.2.3 — 2026-09-29

- Let Codex own its local app server. The installer now runs
  `codex app-server daemon start`, which reuses a running server or starts
  Codex's managed one that Codex keeps updated. Earlier releases installed
  `codex-local-app-server.service` pinned to the Codex binary found at install
  time; Codex never updates a server it did not start, so later Codex releases,
  and models the backend offers only to them, stayed hidden. The installer
  retires that exact unit (a same-named unit it did not write is left alone).
  Restart Codex windows that were connected to the retired server.
- `install.sh --check` reports the Codex server's versions and warns when the
  running server is not Codex-managed, is older than the CLI, or when the
  retired unit is still present. With no server running it explains that
  opening Codex starts one.

## 1.2.2 — 2026-09-17

- Make uncertain lead-message delivery immediately actionable: print the exact
  task/message recovery commands, retain structured relay diagnostics, and show
  every queued, sending, or uncertain blocker in `task status`, including after
  the task itself reaches a terminal state.
- Add bound-lead `message-reconcile` for recording independently verified native
  acceptance without resending; its lead-asserted provenance stays auditable.
- Add explicit `message-supersede` for atomically replacing all unresolved lead
  messages with one complete correction. Superseded content remains auditable
  but is excluded from worker work queues and cannot be retried; late receipts
  and acknowledgments remain prominent without reactivating stale content.
- Upgrade task stores to schema 3 so older clients cannot ignore supersession
  semantics. Version hook registrations so already-running older watchers fail
  closed. Update all participating installations together.

## 1.2.1 — 2026-09-11

- Add an optional Claude worker mailbox hook that wakes the bound session for
  tasks and unread messages without a relay model call. Enable with
  `install.sh --claude` and restart/resume workers; retain `--claude` on updates.
  Existing relay-only installations remain supported.
- Add `task available` so the original lead can queue a fixed reminder for an
  unclaimed task without changing its request or granting execution authority.
- Preserve structured relay diagnostics, including API retries before tool use,
  tool availability, native send observations, public worker matching, and fallback
  state. Capture failed-relay exchange IDs from the CLI's diagnostic stream.
- Distinguish queued hook notifications, native receipts, worker claims and
  completed work. Keep original task/message identities and claim authority.

## 1.2.0 — 2026-09-09

- Add ongoing task conversations: worker questions/updates, lead direction and
  correlated replies delivered to the same bound participants. Keep immutable
  message history and exact recipient acknowledgments separate from task results.
- Automatically wake the lead for every unconsumed worker message; retain
  messages across task transitions and service restarts. Foreground collection
  returns messages with exit 4. Revalidate worker identity before lead delivery
  and retain ambiguous sends for explicit reconciliation rather than blind replay.
- Preserve worker-message order before task results, including recovery from
  uncertain notifications, while allowing independent workers to progress.
- Avoid repeatedly loading or rewriting consumed conversation history during
  polling. Recover cleanly from SQLite storage exhaustion and failed commits.
- Report the observed primary responder model instead of guessing from aggregate
  usage that can include helper models; leave ambiguous model identity unproven.
- Drain CLI capability-probe output to prevent a SIGPIPE race from falsely
  reporting that a supported responder lacks `--safe-mode`.
- Harden adversarial-review findings: actionable concurrent-send recovery,
  sanitized model metadata, unread counts in task status, and a schema-2 marker
  that makes older clients refuse a migrated conversation store explicitly.

## 1.1.0 — 2026-09-09

- Named-worker discovery uses a fresh public host listing; sandboxed callers need no manual UUID lookup.

- Add automatic existing-worker return via `delegate --async`, exact calling
  thread registration and an installer-supervised durable notification service.
  Configure the public local Codex server automatically on user-systemd hosts;
  other hosts retain foreground collection without manual watcher setup.
- Separate delivery, recorded notification and explicit consumption. Reconcile
  service crashes/lost receipts, retain ambiguous sends without blind replay,
  and keep independent workers' outcomes flowing. Preserve session permissions.

- Add `delegate` and `task` commands for existing Claude workers, with a
  transactional mailbox, separate delivery receipts, atomic claims, progress,
  immutable report snapshots, bounded foreground waits, and explicit result
  acknowledgment. Duplicate claims cannot authorize another execution.
- Support a verified worker name/UUID mapping when sandbox process visibility
  differs from Claude's native messaging view. No private inbox access is used.
- Add `task wait-any` for collecting selected workers' ready outcomes without
  waiting behind slower workers. Preserve independent per-result acknowledgment,
  explicit failures/blockers, and later completion after blocker acknowledgment.
- Fix a multi-worker journal-check race: inspect each database object's type
  with one `lstat` instead of treating a journal removed between two checks as
  unsafe. Symlinks and non-regular database objects remain rejected.
- Diagnose a missing resumed Claude session and clear its obsolete current
  session ID when recovery uses a fresh `ask --attach` responder.
- Reject non-regular request/report files before reading, preventing named pipes
  from hanging task commands. Add multi-worker, recovery, and installed-cache
  tests plus an opt-in live Claude canary. Keep the README to installation and
  general usage; move advanced material to `docs/reference.md`.

## 1.0.2+codex.20260827162704 — 2026-08-27

- Detailed Codex requests must now use a private request file so terminal wait
  cards cannot repeatedly expose a large inline prompt. The CLI enforces
  `--topic` as a one-line, 120-character maximum and `--task` as a one-line,
  240-character maximum; the bundled request skill applies the file-backed
  launch rule in new sessions and installations.
- `ask --timeout` is now a primary notification deadline followed by an
  unconditional `--grace` window (default equal to the primary). GNU `timeout`
  sends TERM at the resulting work deadline and allows a fixed ten-second
  bounded shutdown before SIGKILL. Progress switches to `terminating`, and
  lazy recovery cannot reap a live claim inside that TERM-to-KILL interval.
- Default foreground progress is now a single quiet status sentence every 60
  seconds. Detailed deadlines, event counts, activity age and tool/action are
  retained in `status` and available live through `--verbose-progress` or
  `SECONDOPINION_PROGRESS_MODE=verbose`.
- Detached execution was removed. `ask --background` and `review --background`
  now fail before creating an exchange; foreground is the only supported mode.
  Its supervisor publishes a heartbeat for cross-session status and duplicate-
  attach protection, each launch writes an immutable per-run log, archive
  preserves all retry diagnostics, and cross-namespace cancellation fails closed.
- Foreground `ask` now consumes Claude's structured JSON event stream and
  emits structured progress (configurable with
  `SECONDOPINION_PROGRESS_SECS`). `status` records heartbeat age, activity age,
  event count and the last sanitized tool/action, including a heartbeat-based
  liveness state when PID namespaces hide the responder process.
- Every headless launch and its claim share a cryptographic run ID. On failure
  or timeout, the supervisor releases only that exact run's unfinished
  claim and returns the exchange to `published` for immediate `ask --attach`;
  foreign claims and invalid partial responses remain untouched for inspection.
- `--max-turns` is now opt-in. The wall-clock timeout remains the mandatory
  reliability bound; arbitrary default turn caps no longer terminate healthy
  responders just before publication. Because grace defaults to the primary
  timeout, this can double an older caller's wall-clock/cost ceiling; callers
  that prioritize cost should set `--grace` or `--max-turns` explicitly.
- Test entrypoints now clear caller-owned auto-prune, retention and thread
  variables so an interactive shell configuration cannot change test fixtures.
- Headless Claude starts with `--safe-mode`, suppressing hooks, plugins,
  auto-memory and session-environment setup. `ask` checks support before
  creating an exchange and gives an exact `claude auth login` plus `--attach`
  recovery path for an expired OAuth token.
- Progress parsing is incremental rather than reparsing the complete stream on
  every heartbeat. Regression coverage now includes live claim reaping,
  TERM-resistant workers, PID-namespace liveness, duplicate attach, immutable
  logs, topic bounds, auth failure, and installed-cache correspondence.
- Foreground progress now clamps its next sleep to the work deadline, making
  the `terminating` transition observable even when the normal reporting
  interval is longer than the timeout. Stale foreign attach explicitly retains
  the incumbent identity under `previous_responder_*` before replacement.
- Published `show` now emits only a hash-validated snapshot, while draft
  inspection remains available. A cryptographic foreground launch reservation
  prevents an unrelated manual responder from stealing a live run's claim.
  Forced SIGKILL timeout suppresses Bash's misleading `Killed (...)` job line;
  the bounded timeout result remains visible. Tests include a real second PID
  namespace rather than only simulated foreign metadata.
- Launch reservations now have an explicit `launching` status and a five-second
  abandonment bound; manual claim and duplicate attach fail closed during that
  state, then recover normally after a genuine abandoned launch. Process exit
  code and validated-answer completion are recorded separately, so exit 0
  without a published answer is never represented as successful completion.

## 1.0.0 — 2026-08-20

First public release. secondopinion gives one AI coding agent a sealed,
verifiable second opinion from another — today Codex → Claude Code — with no
human relay and nothing installed on the responding side.

- `secondopinion ask`: one command creates and publishes the exchange, runs a
  headless Claude Code responder in the calling checkout, waits, validates and
  prints the answer (`--background`, `--timeout`, `--model`, `--json`). The
  responder prompt is **self-contained** — the full respond workflow travels
  inline plus `--permission-mode dontAsk` and a tool allowlist (`--write` =
  `acceptEdits`) — so nothing has to be installed in Claude, only the `claude`
  CLI on PATH: the mirror of the Claude→Codex plugin, which installs nothing
  in Codex.
- `scripts/install.sh` default is the **true Codex plugin**
  (`codex plugin marketplace add` + `codex plugin add secondopinion@secondopinion`)
  and leaves the Claude side empty (removing any earlier secondopinion
  skill/plugin there). `--claude` opts into the Claude Code plugin for
  interactive responding; `--skills` is the symlink form for setups without
  plugin support; `--plugin` is a deprecated alias for `--claude`. `--check`
  requires the Codex side in exactly one current form and accepts an absent
  Claude side.
- Standard plugin layout: `skills/<name>/SKILL.md`, `bin/`, `scripts/`,
  `LICENSE`, `CHANGELOG.md`; both manifests use the default `skills/` scan.
- Compatibility: `agent-mailbox` remains a deprecated alias, `AGENT_MAILBOX_*`
  variables are honoured with a warning, `~/.agent-mailbox` is migrated once by
  `scripts/install.sh` (old path left as a symlink). The legacy path is removed
  from `[sandbox_workspace_write].writable_roots`: Codex's bubblewrap sandbox
  fails fatally on a symlinked writable root ("cannot enforce sandbox read-only
  path …/.git because it crosses writable symlink"); the real store entry covers
  accesses through the symlink.
- `scripts/install.sh` sets `[sandbox_workspace_write] network_access = true`
  in `~/.codex/config.toml` (needed for Codex-run commands to reach Claude);
  an explicit `false` is reported, never flipped.
- Env vars: `SECONDOPINION_DIR`, `SECONDOPINION_OWNER`,
  `SECONDOPINION_STALE_CLAIM_SECS`, `SECONDOPINION_BACKUP_DIR`,
  `SECONDOPINION_CLAUDE`, `SECONDOPINION_CLAUDE_ARGS`,
  `SECONDOPINION_MAX_TURNS`, `SECONDOPINION_ASK_TIMEOUT`,
  `SECONDOPINION_AUTO_PRUNE`.
- Operational parity with the Claude→Codex companion plugin: `jobs`, `result`,
  `cancel` (verified pid + start time, race-safe with answer publication),
  `review`/`review-result` (read-only, adversarial profile, structured JSON
  with an honest parse-failure path), `ask --follow-up` (immutable linked
  exchanges), opt-in `--persist`/`--resume` native sessions, validated
  `--effort`, requested/realized model recording, and bounded per-repository
  archive retention with a tombstoned, dry-run-first `prune` that holds each
  exchange's lock through removal, plus a stderr retention notice from
  `archive`/`jobs` when a bucket goes over its bound. `ask --background`
  handshakes startup (nonzero `responder=startup-failed`), warns inside
  PID-namespaced sandboxes whose teardown kills detached responders, and
  `ask --attach ID` re-launches a responder for an existing published
  exchange; `ask`/`review --max-turns` sizes the responder turn budget. The
  CLI's final line pairs `main \"$@\"; exit` so a running invocation never
  re-reads its own script file — an in-place update of the dev tree under a
  blocked `ask` previously got parsed as shell input after the response
  (exit 2, \"syntax error near unexpected token\").
- Release hardening (final Codex QA round): `ask` refuses unreadable respond
  instructions before creating an exchange and rejects a zero timeout (GNU
  `timeout 0` = no limit); a responder that claims and then dies is reported as
  `state=claimed` with the `--takeover` path; `--background` reports the real
  state; `archive` relocates the responder log into the archived exchange (no
  orphan files); the installer backs up a real directory at the Codex skill
  location, `--check` never lets a current skill symlink mask a stale plugin
  and stays silent about sides it could not inspect; whitespace-form
  `[ sandbox_workspace_write ]` headers are recognized (no duplicate tables).
- Opt-in auto-prune: `SECONDOPINION_AUTO_PRUNE=1` (or per call `archive
  --prune`) makes an archive landing in an over-bound bucket prune THAT
  repository's bucket only — fail-open (a prune failure never fails the
  archive), reported as a parseable `auto_prune=...` stdout key. The
  bucket-scoped run skips the store-wide crash-litter GC and repairs, which
  stay with manual `prune --apply`.

### Hardening before release (2026-08-19/20)

Reliability hardening from an exhaustive live + adversarial test campaign
(all defects reproduced first; every fix carries a regression test — suite
grows 309→407).

- **No more availability crashes on damaged meta.** An exchange whose
  `created_epoch`/`claimed_epoch` is empty (crash-torn or hand-edited meta)
  no longer aborts `list`/`jobs` mid-output (rows after it were silently
  dropped with exit 0) or crashes `claim`/`status` with a raw bash arithmetic
  error; it lists with age `-`, and takeover on an epoch-less claim fails safe
  (age 0). `status` on a meta-less exchange directory (a crash-orphaned `new`)
  reports a clean error naming the cleanup instead of a raw `cat` failure.
- **Lost-token outage closed.** A responder that died after publishing
  `response.md` but before finalizing meta used to strand the exchange in
  `claimed` for `SECONDOPINION_STALE_CLAIM_SECS` (default 30 min) until a
  takeover re-ran `respond`. `wait` and `result` now roll a valid on-disk
  response forward under the exchange lock (write-once + hash validation make
  this safe); an invalid response is never finalized. Likewise, a takeover
  that crashed between removing the old claim and recording the new one left
  a claim nothing could ever answer — `claim` now treats state=claimed with
  no `claim/` as orphaned and re-claims immediately.
- **Locks time out.** A wedged holder used to hang every mutator on that
  exchange silently and forever; `lock()` now fails with `exchange busy`
  after `SECONDOPINION_LOCK_WAIT_SECS` (default 30).
- **Prune removal is rename-first.** `flock` is per-inode and `lock()` opens
  with O_CREAT, so a concurrent locker could recreate `.lock` inside a
  directory mid-`rm -rf` and acquire a lock the pruner did not hold
  (found by an adversarial self-review; mechanism demonstrated live).
  `prune --apply` now renames the target to `archive/.prune-trash.*` under
  the held lock before deleting, and sweeps stale trash on the next apply.
- **`ask --attach` refuses a live responder.** Attaching while the recorded
  background responder is still running would truncate its log mid-write and
  orphan it from `cancel`; it now fails with a `cancel` hint.
- **Timeout diagnostics.** A foreground `ask` timeout now appends a
  `killed by ask --timeout` line to the responder log (previously empty —
  headless `claude -p` buffers everything until completion) and the retry
  hints name `ask --attach` (the actual recovery) instead of only `wait`.
- `meta_set` fsyncs the store's authoritative file after rename (best-effort,
  same discipline as tombstones; untestable in the suite — power-loss only).

From the live interactive Codex spot-check and a second fix round:

- **The PID-namespace warning now also lands on stdout** as parseable keys
  (`sandbox=pid-namespaced`, `sandbox_note=...`): the live spot-check proved
  the stderr WARNING fires inside Codex's sandbox but the calling agent
  swallowed it, so the user never saw it. The request skill now tells the
  agent to relay the constraint and to never end its turn with a launched
  `--background` ask unresolved. (`SECONDOPINION_TEST_PID1_COMM` lets the
  suite exercise the detection on a non-namespaced host.)
- **`archive` refuses a cross-filesystem `archive/`** before mutating
  anything: `mv -T` onto another device degrades to a non-atomic copy+rm
  whose interruption poisons every later archive of that ID — demonstrated
  by test: the old behavior silently archived onto the foreign device.
- **Empty `jobs` explains itself**: a repository with no exchanges now says
  so on stderr and points at `jobs --all`; an empty store says "no exchanges
  in the store". Previously it printed nothing, exit 0.
- The repo-root `CHANGELOG.md` had silently drifted from this file (it still
  said 1.0.0); both changelog top entries are now asserted against the tool
  version by the plugin suite.

Backlog round (same day):

- **Foreground responder identity.** A foreground `ask` now records the
  responder's pid + kernel start time, so `status`/`jobs` from any other
  session show real liveness (and `cancel` can reach it) while the ask runs.
- **Atomic `new` + crash-litter GC.** `new` stages the exchange in a private
  dot-dir and renames it into place; `prune` GCs day-old meta-less orphan
  dirs, `.new.*` staging and stale `.*.tmp.*` files (fresh litter and locked
  exchanges are skipped, exchanges are swept under their own lock).
- **`install.sh --uninstall`** is a COMPLETE removal: plugins, marketplaces,
  skill symlinks, CLI symlinks, sandbox config edits (the
  [sandbox_workspace_write] table is removed only when it holds nothing but
  our settings), the store with all exchanges/archives, the default state
  dir and any leftover plugin cache. Only a custom SECONDOPINION_BACKUP_DIR
  location is left alone.
- **Platform preflight.** The CLI refuses to run without flock(1) and states
  its Linux+GNU-only requirement; the setsid pid-tracking invariant is
  documented at the launch site.
- **Attach + respond edge cases.** `ask --attach` re-checks state under the
  exchange lock (a racing claim wins cleanly; no responder is wasted) and
  refuses `--write` on review exchanges; an `ln` failure without an existing
  response is no longer misreported as write-once; `SECONDOPINION_CLAUDE_ARGS`
  is word-split but never glob-expanded.

## Prehistory (internal, as `agent-mailbox`)

Before the public release the tool lived as `agent-mailbox` (internal versions
1.0.0–1.3.6, git tags `agent-mailbox--v*`): the exchange store with
publish/claim/respond/read-response/archive, hash-bound prompt and response,
atomic claim, worktree-aware matching; symlink containment and retry-safe
operations; validated bounded headers; plugin packaging and eleven Codex QA
hardening rounds (semantic TOML handling, state-aware side-effect-free
`--check`, fail-closed plugin inspection, non-clobbering backups).
