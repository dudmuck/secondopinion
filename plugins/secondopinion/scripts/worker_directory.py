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


class Directory:
    def __init__(self, box):
        self.box = box
        box.db.execute("""CREATE TABLE IF NOT EXISTS public_worker_directory (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            checked REAL NOT NULL, rows TEXT NOT NULL, error TEXT NOT NULL)""")

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
            # Public headless responders are not existing interactive workers.
            if row.get("kind") != "interactive":
                continue
            session, name, cwd = (row.get(k) for k in ("sessionId", "name", "cwd"))
            if not all(isinstance(v, str) for v in (session, name, cwd)) or \
                    str(uuid.UUID(session)) != session or not Path(cwd).is_absolute() or \
                    not name or len(name) > 200 or any(ord(c) < 32 for c in name):
                raise ValueError("invalid public worker identity")
            clean.append(dict(sessionId=session, name=name, cwd=str(Path(cwd).resolve()),
                              kind="interactive"))
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
        matches = [row for row in rows if row['sessionId'] == task['worker']]
        if len(matches) != 1 or matches[0]['cwd'] != task['repo']:
            raise WorkerMismatch('bound worker is absent, ambiguous or in a different checkout')
        row = dict(matches[0])
        # The UUID and checkout identify the worker; a restart or rename changes only its
        # display name. Senders address the current name, so it must be unique. The
        # recorded route stays the requester's original binding.
        if len([r for r in rows if r['name'] == row['name']]) != 1:
            raise WorkerMismatch("bound worker's current name is ambiguous in the public listing")
        if task['worker_name'] and row['name'] != task['worker_name']:
            row['renamed_from'] = task['worker_name']
        return row
