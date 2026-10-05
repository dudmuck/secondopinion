"""Public Claude worker discovery; no private inbox access or command forwarding.

The installed service publishes only Claude's public agents listing. This gives
PID-namespaced callers the same name/UUID/checkout mapping as the host, without
changing their sandbox. Stale or ambiguous mappings never authorize delivery.
"""
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import time
import uuid

MAX_AGE = 10


class WorkerMismatch(ValueError):
    """A successful public listing did not contain the bound unique worker."""


def host_session(box, task):
    """The Claude session now running a task's bound worker conversation.

    Claude can resume a conversation under a new session ID (--fork-session). The
    task keeps its bound worker ID for claims and messages; only notification and
    the sender's liveness check follow a recorded continuation.
    """
    if not box.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_continuations'").fetchone():
        return task['worker']
    row = box.db.execute('SELECT host FROM worker_continuations WHERE worker=? AND repo=?',
                         (task['worker'], task['repo'])).fetchone()
    return row[0] if row else task['worker']


class Directory:
    def __init__(self, box):
        self.box = box
        box.db.execute("""CREATE TABLE IF NOT EXISTS public_worker_directory (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            checked REAL NOT NULL, rows TEXT NOT NULL, error TEXT NOT NULL)""")
        box.db.execute("""CREATE TABLE IF NOT EXISTS worker_continuations (
            worker TEXT NOT NULL, repo TEXT NOT NULL, host TEXT NOT NULL, evidence TEXT NOT NULL,
            recorded_utc TEXT NOT NULL, PRIMARY KEY(worker, repo))""")
        box.db.execute("""CREATE TABLE IF NOT EXISTS worker_continuation_log (
            recorded_utc TEXT NOT NULL, previous TEXT NOT NULL, host TEXT NOT NULL,
            repo TEXT NOT NULL, workers TEXT NOT NULL, evidence TEXT NOT NULL)""")

    @staticmethod
    def public_rows():
        result = subprocess.run(["claude", "agents", "--json"], capture_output=True,
                                text=True, timeout=3)
        if result.returncode or len(result.stdout) > 1024 * 1024:
            raise ValueError("public Claude worker listing failed")
        rows = json.loads(result.stdout)
        if not isinstance(rows, list) or len(rows) > 4096:
            raise ValueError("invalid public Claude worker listing")
        clean = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("invalid public worker row")
            # Public headless responders are not existing workers. Background
            # sessions are persistent conversations under Claude's background host.
            if row.get("kind") not in ("interactive", "background"):
                continue
            session, name, cwd = (row.get(k) for k in ("sessionId", "name", "cwd"))
            if not all(isinstance(v, str) for v in (session, name, cwd)) or \
                    str(uuid.UUID(session)) != session or not Path(cwd).is_absolute() or \
                    not name or len(name) > 200 or any(ord(c) < 32 for c in name):
                raise ValueError("invalid public worker identity")
            clean.append(dict(sessionId=session, name=name, cwd=str(Path(cwd).resolve()),
                              kind=row["kind"]))
        return clean

    def refresh(self):
        started = time.time()
        try:
            rows, error = self.public_rows(), ""
        except (OSError, ValueError, subprocess.TimeoutExpired) as failure:
            rows, error = [], type(failure).__name__ + ": public worker listing unavailable"
        with self.box.transaction():
            self.box.db.execute("INSERT OR REPLACE INTO public_worker_directory VALUES (1,?,?,?)",
                                (started, json.dumps(rows), error))

    def service_running(self):
        try:
            fd = os.open(self.box.store / ".wake-service.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return False
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("unsafe worker directory lease")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return False
            except BlockingIOError:
                return True
        finally:
            os.close(fd)

    def rows(self):
        row = self.box.db.execute("SELECT * FROM public_worker_directory WHERE singleton=1").fetchone()
        if row and self.service_running() and 0 <= time.time() - row["checked"] <= MAX_AGE:
            if row["error"]:
                raise ValueError(row["error"])
            return json.loads(row["rows"])
        # Legacy/no-service callers can still use the public command directly.
        return self.public_rows()

    def resolve(self, name, repo):
        # A just-started worker may have registered after the last host refresh.
        # Wait only for an absent name, never for ambiguity or a mismatched cwd.
        deadline = time.monotonic() + 5
        while True:
            matches = [row for row in self.rows() if row["name"] == name]
            if matches or not self.service_running() or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        if len(matches) != 1:
            raise ValueError("worker name is absent or ambiguous in the fresh public listing: " + name)
        row = matches[0]
        if row["cwd"] != str(Path(repo).resolve()):
            raise ValueError("named worker belongs to a different checkout")
        return row

    def bound(self, task):
        rows = self.rows()
        host = host_session(self.box, task)
        matches = [row for row in rows if row['sessionId'] == host]
        if len(matches) != 1 or matches[0]['cwd'] != task['repo']:
            hint = '' if matches else (
                f'; session {host} is not running. If it was resumed as a new session, that '
                "session's hook continues the task once its launch shows the resume, or run "
                f'`secondopinion task worker-continue --from {host} --to NEW_SESSION` in {task["repo"]}')
            raise WorkerMismatch('bound worker is absent, ambiguous or in a different checkout' + hint)
        row = dict(matches[0])
        if host != task['worker']:
            row['continued_from'] = task['worker']
        # The UUID and checkout identify the worker; a restart or rename changes only its
        # display name. Senders address the current name, so it must be unique. The
        # recorded route stays the requester's original binding.
        if len([r for r in rows if r['name'] == row['name']]) != 1:
            raise WorkerMismatch("bound worker's current name is ambiguous in the public listing")
        if task['worker_name'] and row['name'] != task['worker_name']:
            row['renamed_from'] = task['worker_name']
        return row

    def continue_session(self, previous, session, repo, evidence):
        """Record that worker conversation `previous` now runs as Claude session `session`.

        Allowed only once `previous` has left the public listing (its tasks never
        move away from a running session) and `session` runs in the same checkout.
        Tasks keep their bound worker ID; this re-points notification only.
        """
        from task_mailbox import utc
        for value in (previous, session):
            if str(uuid.UUID(value)) != value:
                raise ValueError('use exact canonical Claude session UUIDs')
        if previous == session:
            raise ValueError('a session cannot continue itself')
        repo = str(Path(repo).resolve())
        rows = self.rows()
        if any(row['sessionId'] == previous for row in rows):
            raise ValueError(f'session {previous} is still running; its tasks stay with it')
        current = [row for row in rows if row['sessionId'] == session]
        if len(current) != 1 or current[0]['cwd'] != repo:
            raise ValueError(f'session {session} is not running in {repo}')
        with self.box.transaction():
            # Chained resumes re-point every conversation the previous session was running.
            workers = sorted({previous} | {row[0] for row in self.box.db.execute(
                'SELECT worker FROM worker_continuations WHERE host=? AND repo=?', (previous, repo))})
            tasks = [row[0] for row in self.box.db.execute(
                'SELECT id FROM tasks WHERE repo=? AND worker IN (' + ','.join('?' for _ in workers) + ') ORDER BY id',
                (repo, *workers))]
            if not tasks:
                raise ValueError(f'no task in {repo} is bound to session {previous}')
            now = utc()
            self.box.db.executemany('INSERT OR REPLACE INTO worker_continuations VALUES (?,?,?,?,?)',
                                    [(worker, repo, session, evidence, now) for worker in workers])
            self.box.db.execute('INSERT INTO worker_continuation_log VALUES (?,?,?,?,?,?)',
                                (now, previous, session, repo, json.dumps(workers), evidence))
        return dict(previous=previous, session=session, repo=repo, workers=workers, tasks=tasks, evidence=evidence)
