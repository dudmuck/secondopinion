#!/usr/bin/env python3
"""Cooperative, same-user worker mailbox. SQLite transactions are the commit boundary.

No Claude private state. Foreground callers wait; explicit async delegation uses
the installed return service. Unconsumed events survive caller and worker exits.
"""

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time


TERMINAL = {"complete", "failed", "refused"}
STATES = {"acknowledged", "preflight", "running", "needs_attention"} | TERMINAL
TRANSITIONS = {
    "acknowledged": STATES,
    "preflight": STATES - {"acknowledged"},
    "running": {"running", "needs_attention"} | TERMINAL,
    "needs_attention": STATES,
}
MAX_TEXT = 2 * 1024 * 1024


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds")


def digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def emit(value):
    print(json.dumps(value, ensure_ascii=True, sort_keys=True), flush=True)


def bounded(value):
    if not re.fullmatch(r"[0-9]{1,8}", value) or int(value) > 86400:
        raise argparse.ArgumentTypeError("must be seconds between 0 and 86400")
    return int(value)


def identifier(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,119}", value):
        raise ValueError("ID/session/consumer must be 1-120 letters, digits, _, . or -")
    return value


def read_text(path):
    # Opening a FIFO normally can wait forever before validation or a deadline.
    # Validate the opened object (not a racy path stat) before reading any data.
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), encoding="utf-8") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("request/result must be a regular file")
        value = stream.read(MAX_TEXT + 1)
    if not value.strip() or len(value.encode("utf-8")) > MAX_TEXT:
        raise ValueError("request/result must be nonempty UTF-8, at most 2 MiB")
    return value


class Mailbox:
    def __init__(self, store):
        # The supported trust boundary is the same OS user, as with exchanges.
        # Refuse symlinked database objects before SQLite can write through them.
        self.store = Path(store).absolute()
        self.store.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.store.is_symlink() or not self.store.is_dir():
            raise ValueError("task store must be a real directory")
        path = self.store / "tasks.sqlite3"
        for suffix in ("", "-journal", "-wal", "-shm"):
            candidate = Path(str(path) + suffix)
            try:
                mode = candidate.lstat().st_mode
            except FileNotFoundError:
                continue
            # A journal can disappear on another writer's commit. Separate
            # exists()/is_file() calls falsely classify that normal race as an
            # unsafe object. Inspect one lstat snapshot, without following links.
            if not stat.S_ISREG(mode):
                raise ValueError("refusing unsafe task database object")
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=FULL")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1, 2, 3):
            raise ValueError(f"unsupported task database version {version}")
        with self.transaction():
            self.db.execute("""CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, worker TEXT NOT NULL, requester TEXT NOT NULL,
                repo TEXT NOT NULL, request TEXT NOT NULL, request_sha256 TEXT NOT NULL,
                state TEXT NOT NULL, revision INTEGER NOT NULL, updated_utc TEXT NOT NULL,
                updated_epoch REAL NOT NULL, message TEXT NOT NULL, action_needed TEXT NOT NULL,
                delivery_receipt TEXT, result TEXT, result_sha256 TEXT)""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS events (
                task_id TEXT NOT NULL REFERENCES tasks(id), revision INTEGER NOT NULL,
                kind TEXT NOT NULL, state TEXT NOT NULL, updated_utc TEXT NOT NULL,
                actor TEXT NOT NULL, message TEXT NOT NULL,
                PRIMARY KEY(task_id, revision))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS acknowledgments (
                task_id TEXT NOT NULL REFERENCES tasks(id), consumer TEXT NOT NULL,
                revision INTEGER NOT NULL, updated_utc TEXT NOT NULL,
                PRIMARY KEY(task_id, consumer))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS delivery_receipts (
                task_id TEXT PRIMARY KEY REFERENCES tasks(id), receipt TEXT NOT NULL,
                updated_utc TEXT NOT NULL)""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS worker_routes (
                task_id TEXT PRIMARY KEY REFERENCES tasks(id), name TEXT NOT NULL)""")
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                               "AND name='worker_hooks'").fetchone():
                columns = [row['name'] for row in self.db.execute('PRAGMA table_info(worker_hooks)')]
                if 'protocol' not in columns:
                    # This invalidates a pre-opened old watcher's three-value
                    # heartbeat even when it still owns its session lease.
                    self.db.execute('ALTER TABLE worker_hooks ADD COLUMN protocol INTEGER')
            # Schema 3 adds explicit conversation supersession. Older clients
            # must refuse the store because they could otherwise redeliver a
            # stale instruction that a newer correction replaced.
            self.db.execute("PRAGMA user_version=3")

    @contextlib.contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            # SQLite can roll back automatically on FULL/IOERR. A failed COMMIT
            # can instead leave the transaction open: recover both cases while
            # preserving the original error for the caller.
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def get(self, task_id):
        identifier(task_id)
        row = self.db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError(f"unknown task: {task_id}")
        task = dict(row)
        delivery = self.db.execute("SELECT updated_utc FROM delivery_receipts WHERE task_id=?", (task_id,)).fetchone()
        task["delivered_utc"] = delivery[0] if delivery else None
        route = self.db.execute("SELECT name FROM worker_routes WHERE task_id=?", (task_id,)).fetchone()
        task["worker_name"] = route[0] if route else None
        if digest(task["request"]) != task["request_sha256"]:
            raise ValueError("task request hash mismatch")
        if task["state"] == "complete" and task["result"] is None:
            raise ValueError("complete task has no result")
        if task["result"] is not None and digest(task["result"]) != task["result_sha256"]:
            raise ValueError("task result hash mismatch")
        return task

    def record(self, task, kind, actor, event_message, **changes):
        changes.update(revision=task["revision"] + 1, updated_utc=utc(), updated_epoch=time.time())
        # Keys come only from this module, never user text.
        self.db.execute("UPDATE tasks SET " + ",".join(k + "=?" for k in changes) + " WHERE id=?",
                        (*changes.values(), task["id"]))
        self.db.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?)", (
            task["id"], changes["revision"], kind, changes.get("state", task["state"]),
            changes["updated_utc"], actor, event_message))
        return self.get(task["id"])

    def create(self, task_id, worker, requester, repo, request, worker_name=None):
        for value in (task_id, worker, requester):
            identifier(value)
        if worker_name is not None and (not worker_name.strip() or len(worker_name) > 120 or
                                        any(ord(c) < 32 for c in worker_name)):
            raise ValueError("worker name must be a nonempty single line, at most 120 characters")
        with self.transaction():
            row = self.db.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row:
                task = self.get(task_id)
                expected = (worker, requester, repo, request)
                if tuple(task[k] for k in ("worker", "requester", "repo", "request")) != expected:
                    raise ValueError("task ID already exists with different worker, requester, checkout or request")
            else:
                self.db.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    task_id, worker, requester, repo, request, digest(request), "created", 0,
                    utc(), time.time(), "", "", None, None, None))
                task = self.record(self.get(task_id), "created", requester, "task created")
            if worker_name is not None and task["worker_name"] != worker_name:
                if task["worker_name"] is not None or task["state"] != "created" or task["delivery_receipt"]:
                    raise ValueError("cannot change a recorded route or add one after delivery/claim")
                self.db.execute("INSERT INTO worker_routes VALUES (?,?)", (task_id, worker_name))
                task = self.record(task, "route", requester, "requester bound worker name: " + worker_name)
            return task

    def claim(self, task_id, session):
        with self.transaction():
            task = self.get(task_id)
            if task["worker"] != session:
                raise ValueError("session does not match assigned worker; no claim was made")
            if task["state"] != "created":
                return {"execute": False, "task": task}
            task = self.record(task, "claimed", session, "worker acknowledged", state="acknowledged")
            return {"execute": True, "task": task}

    def update(self, args):
        if not args.message.strip() or len(args.message) > 4096 or len(args.action_needed) > 4096:
            raise ValueError("message must be nonempty; message/action-needed are limited to 4096 characters")
        result = read_text(args.file) if args.file else None
        if args.state == "complete" and result is None:
            raise ValueError("complete requires --file with the final result")
        if result is not None and args.state not in TERMINAL:
            raise ValueError("--file is only for terminal outcomes")
        if args.state == "needs_attention" and not args.action_needed.strip():
            raise ValueError("needs_attention requires --action-needed")
        with self.transaction():
            task = self.get(args.id)
            if task["worker"] != args.session:
                raise ValueError("session does not match assigned worker")
            if task["revision"] != args.revision:
                raise ValueError("revision conflict; read task status before retrying")
            if args.state not in TRANSITIONS.get(task["state"], set()):
                raise ValueError(f"cannot change {task['state']} to {args.state}; terminal tasks are immutable")
            return self.record(task, "worker", args.session, args.message,
                               state=args.state, message=args.message, action_needed=args.action_needed,
                               result=result, result_sha256=digest(result) if result is not None else None)

    def delivered(self, task_id, receipt):
        if not receipt.strip() or len(receipt) > 512 or "\n" in receipt or "\r" in receipt:
            raise ValueError("receipt must be a nonempty single line of at most 512 characters")
        with self.transaction():
            task = self.get(task_id)
            if task["delivery_receipt"]:
                return task
            # A worker can claim or finish before the messenger records its receipt.
            # Preserve both worker state and revision of a terminal result.
            self.db.execute("UPDATE tasks SET delivery_receipt=? WHERE id=?", (receipt, task_id))
            self.db.execute("INSERT INTO delivery_receipts VALUES (?,?,?)", (task_id, receipt, utc()))
            return self.get(task_id)

    def acknowledge(self, task_id, consumer, revision):
        identifier(consumer)
        with self.transaction():
            task = self.get(task_id)
            if task["revision"] != revision or task["state"] not in TERMINAL | {"needs_attention"}:
                raise ValueError("ack requires the current terminal/attention revision that was consumed")
            old = self.db.execute("SELECT revision FROM acknowledgments WHERE task_id=? AND consumer=?",
                                  (task_id, consumer)).fetchone()
            if old and old[0] >= revision:
                return {"acknowledged": False, "id": task_id, "revision": revision}
            self.db.execute("INSERT OR REPLACE INTO acknowledgments VALUES (?,?,?,?)",
                            (task_id, consumer, revision, utc()))
            return {"acknowledged": True, "id": task_id, "revision": revision}

    def status(self, task_id, stale_after=300):
        task = self.get(task_id)
        # Also called inside pending()'s transaction. Status must not initialize
        # conversation tables or acquire a nested write transaction.
        counts = {}
        delivery_alerts = []
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_messages'").fetchone():
            counts = dict(self.db.execute('SELECT m.recipient,COUNT(*) FROM task_messages m '
                "LEFT JOIN message_delivery d ON d.task=m.task AND d.message=m.id "
                "WHERE m.task=? AND m.acknowledged_utc IS NULL AND COALESCE(d.state,'')!='superseded' "
                'GROUP BY m.recipient', (task_id,)))
            rows = self.db.execute("SELECT m.id,m.sequence,m.acknowledged_utc,d.state,d.attempt,d.receipt,d.error,d.updated_utc "
                "FROM task_messages m JOIN message_delivery d ON d.task=m.task AND d.message=m.id "
                "WHERE m.task=? AND ((m.acknowledged_utc IS NULL AND d.state IN ('queued','sending','uncertain')) "
                "OR (d.state='superseded' AND (d.receipt IS NOT NULL OR m.acknowledged_utc IS NOT NULL))) "
                'ORDER BY m.sequence', (task_id,)).fetchall()
            unresolved_ids = [row['id'] for row in rows if row['state'] != 'superseded']
            for row in rows:
                alert = dict(row)
                from relay_diagnostics import latest_message
                alert['diagnostics'] = latest_message(self, task_id, row['id'])
                alert['blocks_later_relay_messages'] = row['state'] != 'superseded'
                alert['inspect_command'] = f'secondopinion task message-read {task_id} {row["id"]}'
                if row['state'] == 'superseded':
                    correction = self.db.execute('SELECT superseded_by FROM message_supersessions '
                        'WHERE task=? AND message=?', (task_id, row['id'])).fetchone()
                    alert['risk'] = 'superseded_message_reached_or_was_consumed_by_worker'
                    alert['superseded_by'] = correction['superseded_by'] if correction else None
                    delivery_alerts.append(alert)
                    continue
                if row['state'] == 'uncertain':
                    alert['retry_after_proving_non_delivery'] = (
                        f'secondopinion task message-retry {task_id} {row["id"]} '
                        f'--session {task["requester"]} --confirm-not-delivered')
                    alert['reconcile_after_proving_acceptance'] = (
                        f'secondopinion task message-reconcile {task_id} {row["id"]} '
                        f'--session {task["requester"]} --attempt {row["attempt"] or "ATTEMPT"} '
                        '--receipt ACTUAL_RECEIPT --confirm-accepted')
                alert['supersede_with_complete_correction'] = (
                    f'secondopinion task message-supersede {task_id} {row["id"]} '
                    f'--id CORRECTION_ID --session {task["requester"]} --file CORRECTION_FILE '
                    f'--expect-superseded {",".join(unresolved_ids)} '
                    '--confirm-ambiguous-prior-delivery')
                delivery_alerts.append(alert)
        task['unread_messages'] = {role: counts.get(task[role], 0) for role in ('requester', 'worker')}
        task['message_delivery_alerts'] = delivery_alerts
        task["age_seconds"] = max(0, int(time.time() - task["updated_epoch"]))
        task["stale"] = task["state"] not in TERMINAL and task["age_seconds"] >= stale_after
        task["notification"] = "foreground polling; automatic return only with a registered wake service"
        from relay_diagnostics import latest
        task["delivery_diagnostics"] = latest(self, task_id)
        return task

    def wait(self, task_id, timeout):
        from task_conversation import Conversation
        conversation = Conversation(self)
        deadline = time.monotonic() + timeout
        next_progress = time.monotonic() + 60
        while True:
            task = self.status(task_id)
            messages = conversation.messages(task_id, task['requester'], unread=True)
            if messages:
                emit(dict(task=task, messages=messages))
                return 4
            if task["state"] in TERMINAL | {"needs_attention"}:
                emit(task)
                return 0 if task["state"] == "complete" else 3
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                emit(task)
                return 124
            if time.monotonic() >= next_progress:
                print(f"Waiting for worker: {task['state']} (last update {task['age_seconds']}s ago).", file=sys.stderr, flush=True)
                next_progress = time.monotonic() + 60
            time.sleep(min(0.25, remaining))

    def pending(self, consumer, repo=None, task_ids=None):
        identifier(consumer)
        selected = list(dict.fromkeys(task_ids)) if task_ids is not None else None
        if selected is not None and not 1 <= len(selected) <= 256:
            raise ValueError("select between 1 and 256 distinct task IDs")
        with self.transaction():
            # Validate every explicitly selected task, even if already consumed.
            # Unknown IDs must not silently disappear from a coordinator's watch.
            if selected is not None:
                for task_id in selected:
                    self.get(task_id)
            conditions = ["t.state IN ('complete','failed','refused','needs_attention')",
                          "t.revision > COALESCE(a.revision,0)"]
            values = [consumer]
            if repo is not None:
                conditions.append("t.repo=?")
                values.append(str(Path(repo).resolve()))
            if selected is not None:
                conditions.append("t.id IN (" + ",".join("?" for _ in selected) + ")")
                values.extend(selected)
            rows = self.db.execute("SELECT t.id FROM tasks t LEFT JOIN acknowledgments a "
                                   "ON t.id=a.task_id AND a.consumer=? WHERE " +
                                   " AND ".join(conditions) + " ORDER BY t.updated_epoch, t.id", values)
            return [self.status(row[0]) for row in rows.fetchall()]

    def wait_any(self, task_ids, consumer, timeout):
        from task_conversation import Conversation
        conversation = Conversation(self)
        deadline = time.monotonic() + timeout
        next_progress = time.monotonic() + 60
        while True:
            pending = self.pending(consumer, task_ids=task_ids)
            messages = [message for task_id in dict.fromkeys(task_ids)
                        if consumer == self.get(task_id)['requester']
                        for message in conversation.messages(task_id, consumer, unread=True)]
            if messages:
                emit(dict(outcomes=pending, messages=messages))
                return 4
            if pending:
                emit(pending)
                return 0 if all(task["state"] == "complete" for task in pending) else 3
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                emit([])
                return 124
            if time.monotonic() >= next_progress:
                print("Waiting for selected workers; no unconsumed outcome yet.", file=sys.stderr, flush=True)
                next_progress = time.monotonic() + 60
            time.sleep(min(0.25, remaining))


def instructions(task, cli, store):
    command = shlex.join(["env", f"SECONDOPINION_DIR={store}", cli, "task"])
    task_id, worker = shlex.quote(task["id"]), shlex.quote(task["worker"])
    return f"""Delegated task {task['id']}; assigned worker session {task['worker']}.
Use the shared mailbox to return results. The messenger may exit and has no return inbox.
Do not reply to another peer or assume Codex is a native Claude teammate.
Read the exact request and checkout with:
  {command} status {task_id}
Then read that checkout's repository policies. Preserve the request's authority and scope.
Before starting work, atomically claim:
  {command} claim {task_id} --session {worker}
Execute only if the returned execute field is true. A duplicate delivery returns false:
do not launch the work again. Inspect existing progress instead. A restarted session must
reconcile actual executor state before continuing; this mailbox never grants a takeover.
Publish progress using the latest revision from status/claim, replacing N below:
  {command} update {task_id} --session {worker} --revision N --state running --message 'concise progress'
For a blocker use --state needs_attention with --action-needed 'what is required'.
To ask the lead a question or send an update while the task continues, write the text
to a private file, then send it directly to the durable mailbox:
  {command} message {task_id} --id UNIQUE_MESSAGE_ID --session {worker} --file /absolute/message.md
Use the SAME message ID and content when retrying. To answer an incoming message,
add --reply-to ITS_MESSAGE_ID. Read conversation history with:
  {command} messages {task_id}
If waiting for an answer, end your turn; the lead's reply notifies this same session.
Messages do not complete tasks, grant new authority, or authorize duplicate execution.
For refusal/failure use --state refused or failed with a meaningful --message.
To complete, write your final report to a private scratch file and publish its contents:
  {command} update {task_id} --session {worker} --revision N --state complete --message 'done' --file /absolute/report.md
The tool snapshots the report; its original file may then be removed. A revision conflict
requires reading status before retrying. Never edit the database directly. Never report
complete before the requested work and its required restoration/validation have finished.
"""


def relay_prompt(task, cli, store, content=None, receipt_command=None):
    command = shlex.join(["env", f"SECONDOPINION_DIR={store}", cli, "task"])
    route = ""
    if task["worker_name"] and task.get("worker_renamed_from"):
        route = f"""The requester bound this UUID to the peer name {json.dumps(task['worker_renamed_from'])}.
A fresh public host-side worker listing shows the same session (same UUID and
checkout) now named {json.dumps(task['worker_name'])}. Use that current name: require exactly
one ListAgents peer with that exact name and send to its messaging reference. If
ListAgents also exposes a UUID it must match the assigned UUID. Do not fall back to
any other name, including the old one. If the current name is absent or ambiguous, fail.
"""
    elif task["worker_name"]:
        route = f"""The requester explicitly bound this UUID to the exact peer name
{json.dumps(task['worker_name'])}, verified from a public host-side worker listing.
Use this recorded name mapping: require exactly one ListAgents peer with that exact
name and send to its messaging reference. If ListAgents also exposes a UUID it must
match the assigned UUID. A sandboxed `claude agents --json` may be empty because of
PID namespaces; it does not invalidate this requester-supplied mapping or prove exit.
Do not fall back to a different name. If this exact name is absent or ambiguous, fail.
"""
    else:
        route = f"""First call ListAgents. Some Claude versions show a peer name and a short
messaging reference rather than the session UUID. If the UUID is not shown, use
the public CLI `claude agents --json` to find the row whose sessionId exactly equals
{task['worker']}, then match its exact name in ListAgents. Require that name to be
unique in BOTH listings and the CLI row's cwd to match {shlex.quote(task['repo'])}.
SendMessage should use the reference from that matched ListAgents row. These are
two public views of the same session, not interchangeable IDs. If neither direct
UUID resolution nor that unambiguous mapping works, fail delivery and explain that
the requester can supply --worker-name after verifying the mapping on the host.
An empty sandboxed CLI listing is not proof that the worker stopped.
"""
    if content is None:
        content = instructions(task, cli, store)
    if receipt_command is None:
        receipt_command = f"{command} delivered {shlex.quote(task['id'])} --receipt 'ACTUAL_MESSAGE_RECEIPT'"
    return f"""You are a delivery relay for an existing Claude worker, not its executor.
Resolve ONLY the exact session {task['worker']}.
{route}
Do not guess a peer,
start another worker, resume a private session, or take over a team. If ListAgents or
SendMessage is unavailable, or that exact session is absent/ambiguous, publish a failed
delivery explanation in your secondopinion response and stop; do not claim delivery.
Send the following instructions to that exact session using SendMessage:

{content}

After SendMessage confirms acceptance, record its message ID/receipt:
  {receipt_command}
Then publish your secondopinion response as a DELIVERY RECEIPT, explicitly saying worker
completion is pending or unverified, and exit. Do not wait for an inbox reply. Do not
claim/update/complete the worker task yourself. Codex waits on the worker mailbox.
Keep the final relay response to at most three sentences: delivery outcome, actual
message receipt if any, and task ID. If delivery fails, include the concrete reason.
"""


def delegate(box, args):
    request = read_text(args.file)
    if not args.worker:
        if not args.worker_name:
            raise ValueError("delegate requires --worker or --worker-name")
        from worker_directory import Directory
        args.worker = Directory(box).resolve(args.worker_name, Path.cwd())["sessionId"]
    task = box.create(args.id, args.worker, args.requester, str(Path.cwd().resolve()), request, args.worker_name)
    notification = None
    delivery_state = 'accepted'
    if args.async_delivery:
        # Registration is completed before the relay can cause worker execution.
        # It never starts/resumes a Codex conversation or bypasses a permission.
        from codex_wakeup import automatic_registration, AutomaticUnavailable
        try:
            notification = automatic_registration(box.store, args.id, args.requester)
        except AutomaticUnavailable as error:
            print(str(error) + "; waiting for the worker in this call instead.", file=sys.stderr)
    # Serialize deliveries of one task without holding a database transaction over Claude.
    lock_path = box.store / (".task-delivery-" + task["id"] + ".lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("a delivery attempt is already active; use task wait for this ID")
        task = box.get(args.id)
        delivery_state = ('accepted' if task['delivery_receipt'] else
                          'worker_acknowledged' if task['state'] != 'created' else 'unconfirmed')
        if not task["delivery_receipt"] and task["state"] == "created":
            from worker_hook import make_available, notify
            from relay_diagnostics import observe, record, run_relay
            make_available(box, task)
            fallback = notify(box, task)
            if fallback['state'] == 'queued':
                delivery_state = 'queued'
                record(box, args.id, dict(fallback, stage='worker_hook', reason='worker_mailbox_notice_queued',
                                         tools_called=[], relay_exit_code=None))
            else:
                delivery_state = 'unconfirmed'
        if not task["delivery_receipt"] and task["state"] == "created" and delivery_state == 'unconfirmed':
            relay_task = task
            if task["worker_name"]:
                # A retry after the bound worker restarted must address its current name.
                try:
                    from worker_directory import Directory
                    current = Directory(box).bound(task)
                    if current.get("renamed_from"):
                        relay_task = dict(task, worker_name=current["name"], worker_renamed_from=current["renamed_from"])
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    pass  # the relay itself still requires the recorded exact name
            with tempfile.TemporaryDirectory(prefix="task-relay-", dir=box.store) as tmp:
                prompt = Path(tmp) / "request.md"
                prompt.write_text(relay_prompt(relay_task, args.cli, box.store), encoding="utf-8")
                # The sealed relay answer remains in its exchange. Do not print its
                # narrative as if it were the delegated worker's final result.
                exchange, exit_code = run_relay(args.cli, prompt, box.store, 'delivery ' + args.id[:100], args.delivery_timeout)
                task = box.get(args.id)
                diagnostics = observe(box.store, exchange, exit_code)
                diagnostics['worker_directory_match'] = None
                try:
                    from worker_directory import Directory, WorkerMismatch
                    current = Directory(box).bound(task)
                    diagnostics['worker_directory_match'] = True
                    if current.get('renamed_from'):
                        diagnostics.update(worker_name=current['name'], worker_renamed_from=current['renamed_from'])
                except WorkerMismatch as error:
                    diagnostics['worker_directory_match'] = False
                    diagnostics['worker_directory_reason'] = str(error)
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    pass
                if task["state"] == "created" and not task["delivery_receipt"]:
                    # A worker can load/rearm its hook while the relay is failing.
                    fallback = notify(box, task)
                    diagnostics['fallback'] = fallback
                    record(box, args.id, diagnostics)
                    if fallback['state'] == 'queued':
                        delivery_state = 'queued'
                    else:
                        emit({"task": box.status(args.id), "delivery": "unconfirmed",
                              "relay_exit_code": exit_code, "diagnostics": diagnostics})
                        print("Worker delivery is unconfirmed (" + diagnostics['reason'] + "). "
                              "Retry with the SAME task ID; a worker with the Claude plugin can discover it automatically.", file=sys.stderr)
                        return 1
                else:
                    diagnostics['worker_state_observed'] = task['state']
                    if task['delivery_receipt']:
                        diagnostics.update(stage='receipt', reason='receipt_persisted')
                    record(box, args.id, diagnostics)
                    delivery_state = 'accepted' if task['delivery_receipt'] else 'worker_acknowledged'
    finally:
        os.close(fd)
    # Relay failure cannot erase a worker acknowledgment/result that already landed.
    if notification is not None:
        emit({"task": box.status(args.id), "notification": "automatic",
              "registration": notification["name"], "delivery": delivery_state,
              "notice": "Delivery acceptance is not worker completion. Results and blockers arrive separately."})
        return 0
    return box.wait(args.id, args.timeout)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--store", required=True)
    p.add_argument("--cli", required=True)
    root = p.add_subparsers(dest="mode", required=True)
    root.add_parser("workers", help="fresh public worker names, UUIDs and checkouts; JSON")
    task = root.add_parser("task", help="durable worker tasks; JSON output")
    commands = task.add_subparsers(dest="command", required=True)
    from task_conversation import add_commands
    add_commands(commands)
    create = commands.add_parser("create", help="idempotent create with an owner-chosen stable ID")
    delivery = root.add_parser("delegate", help="deliver once, then foreground wait; reuse ID to recover")
    for sub in (create, delivery):
        sub.add_argument("--id", required=True)
        sub.add_argument("--worker", required=sub is create, help="exact existing Claude session ID")
        sub.add_argument("--worker-name", help="exact unique worker name; delegate resolves UUID automatically when omitted")
        sub.add_argument("--requester", default=os.environ.get("CODEX_THREAD_ID", "local"))
        sub.add_argument("--file", required=True)
    delivery.add_argument("--timeout", type=bounded, default=900)
    delivery.add_argument("--delivery-timeout", type=bounded, default=120)
    delivery.add_argument("--async", dest="async_delivery", action="store_true",
                          help="automatically return outcomes to this Codex thread; requires installer service")
    for name in ("status", "instructions", "relay-prompt", "claim", "update", "delivered", "events", "ack", "wait", "result", "available"):
        sub = commands.add_parser(name)
        sub.add_argument("id")
        if name == "status":
            sub.add_argument("--stale-after", type=bounded, default=300)
        if name in ("claim", "update", "available"):
            sub.add_argument("--session", required=True)
        if name == "update":
            sub.add_argument("--revision", type=int, required=True)
            sub.add_argument("--state", choices=sorted(STATES), required=True)
            sub.add_argument("--message", required=True)
            sub.add_argument("--action-needed", default="")
            sub.add_argument("--file")
        if name == "delivered":
            sub.add_argument("--receipt", required=True)
        if name == "ack":
            sub.add_argument("--consumer", required=True)
            sub.add_argument("--revision", type=int, required=True)
        if name == "wait":
            sub.add_argument("--timeout", type=bounded, default=900)
    inbox = commands.add_parser("inbox", help="unconsumed terminal/attention events; never auto-acks")
    inbox.add_argument("--consumer", required=True)
    inbox.add_argument("--repo", default=str(Path.cwd().resolve()))
    wait_any = commands.add_parser("wait-any", help="collect ready outcomes from selected tasks without waiting for slower workers")
    wait_any.add_argument("ids", nargs="+")
    wait_any.add_argument("--consumer", required=True)
    wait_any.add_argument("--timeout", type=bounded, default=900)
    return p


def main():
    os.umask(0o077)
    args = parser().parse_args()
    box = Mailbox(args.store)
    try:
        if args.mode == "workers":
            from worker_directory import Directory
            emit(Directory(box).rows())
            return 0
        if args.mode == "delegate":
            if args.delivery_timeout == 0:
                raise ValueError("--delivery-timeout must be positive")
            return delegate(box, args)
        command = args.command
        if command in ('message', 'message-read', 'message-ack', 'message-delivered',
                       'message-reconcile', 'message-retry', 'message-supersede',
                       'messages', 'receive'):
            from task_conversation import dispatch
            return dispatch(box, args)
        if command == "create":
            emit(box.create(args.id, args.worker, args.requester, str(Path.cwd().resolve()), read_text(args.file), args.worker_name))
        elif command == "status":
            emit(box.status(args.id, args.stale_after))
        elif command == "available":
            task = box.get(args.id)
            if args.session != task['requester'] or str(Path.cwd().resolve()) != task['repo']:
                raise ValueError('task-available notice requires the bound requester and checkout')
            caller = os.environ.get('CODEX_THREAD_ID')
            if caller and caller != args.session:
                raise ValueError('calling Codex thread does not match the task lead')
            if task['state'] != 'created':
                raise ValueError('task is already claimed; use task messages or status')
            from worker_hook import make_available, notify
            make_available(box, task)
            notification = notify(box, task)
            emit(dict(task=box.status(args.id), notification=notification, delivery='queued'))
            return 0 if notification['state'] == 'queued' else 1
        elif command in ("instructions", "relay-prompt"):
            fn = instructions if command == "instructions" else relay_prompt
            print(fn(box.get(args.id), args.cli, box.store))
        elif command == "claim":
            emit(box.claim(args.id, args.session))
        elif command == "update":
            emit(box.update(args))
        elif command == "delivered":
            emit(box.delivered(args.id, args.receipt))
        elif command == "events":
            box.get(args.id)
            emit([dict(row) for row in box.db.execute("SELECT * FROM events WHERE task_id=? ORDER BY revision", (args.id,))])
        elif command == "ack":
            emit(box.acknowledge(args.id, args.consumer, args.revision))
        elif command == "wait":
            return box.wait(args.id, args.timeout)
        elif command == "result":
            task = box.status(args.id)
            emit(task)
            return 0 if task["state"] == "complete" else 3
        elif command == "inbox":
            emit(box.pending(args.consumer, repo=args.repo))
        elif command == "wait-any":
            return box.wait_any(args.ids, args.consumer, args.timeout)
        return 0
    finally:
        box.db.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, sqlite3.Error, UnicodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Interrupted; task retained. Resume with task status/wait; do not duplicate execution.", file=sys.stderr)
        sys.exit(130)
