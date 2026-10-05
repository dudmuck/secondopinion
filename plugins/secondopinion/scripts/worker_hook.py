#!/usr/bin/env python3
"""Claude asyncRewake hook: notify this session about its own durable mailbox.

No model, private Claude state, task claim, receipt, or execution. One bounded
watcher per session/store; native hook exit 2 queues the fixed reminder.
"""
import fcntl
import json
import os
from pathlib import Path
import shlex
import sqlite3
import stat
import subprocess
import sys
import time
import uuid

from task_mailbox import Mailbox, digest, identifier

LIFETIME = 23 * 60 * 60
MAX_AGE = 5
REPEAT_AFTER = 60
HOOK_PROTOCOL = 3


def make_available(box, task):
    # Merely drafting a task must not wake its executor. Delegate calls this
    # only after requester/async-registration checks, or the lead explicitly
    # sends the typed task-available notice.
    with box.transaction():
        box.db.execute('CREATE TABLE IF NOT EXISTS worker_hook_tasks (task TEXT PRIMARY KEY REFERENCES tasks(id))')
        box.db.execute('INSERT OR IGNORE INTO worker_hook_tasks VALUES (?)', (task['id'],))


def lock_path(box, session):
    return box.store / ('.worker-hook-' + digest(session) + '.lock')


def ready(box, task):
    if not box.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_hooks'").fetchone():
        return False
    from worker_directory import host_session
    session = host_session(box, task)
    row = box.db.execute('SELECT * FROM worker_hooks WHERE session=?', (session,)).fetchone()
    if (not row or 'protocol' not in row.keys() or row['protocol'] != HOOK_PROTOCOL or
            row['repo'] != task['repo'] or not 0 <= time.time() - row['heartbeat'] < MAX_AGE):
        return False
    try:
        fd = os.open(lock_path(box, session), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return False
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError('unsafe worker hook lease')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True
    finally:
        os.close(fd)


def notify(box, task):
    listening = ready(box, task)
    # The hook may have emitted and exited between task creation and this check.
    # Its recent attempt is observable but is still not a native receipt.
    emitted = False
    hook_columns = ([row['name'] for row in box.db.execute('PRAGMA table_info(worker_hooks)')]
                    if box.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                      "AND name='worker_hooks'").fetchone() else [])
    if (not listening and 'protocol' in hook_columns and
            box.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                           "AND name='worker_hook_notices'").fetchone()):
        from worker_directory import host_session
        row = box.db.execute('SELECT n.emitted FROM worker_hook_notices n JOIN worker_hooks h ON h.session=n.session '
            'WHERE n.session=? AND h.repo=? AND h.protocol=? AND n.notice=?',
            (host_session(box, task),task['repo'],HOOK_PROTOCOL,'task:' + task['id'])).fetchone()
        emitted = task['state'] == 'created' and row is not None and 0 <= time.time()-row['emitted'] < REPEAT_AFTER
    if not listening and not emitted:
        return dict(transport='worker_hook', state='not_listening')
    # Sender still requires the exact UUID (or its recorded continuation) and
    # checkout, and a unique current name.
    from worker_directory import Directory
    current = Directory(box).bound(task)
    result = dict(transport='worker_hook', state='queued', worker_directory_match=True,
                  observation='watcher_listening' if listening else 'reminder_emitted')
    if current.get('renamed_from'):
        result.update(worker_name=current['name'], worker_renamed_from=current['renamed_from'])
    if current.get('continued_from'):
        result.update(worker_session=current['sessionId'], worker_continued_from=current['continued_from'])
    return result


def workers(box, session, repo):
    """This session plus every worker conversation it continues in this checkout."""
    hosted = []
    if box.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_continuations'").fetchone():
        hosted = [row[0] for row in box.db.execute(
            'SELECT worker FROM worker_continuations WHERE host=? AND repo=? ORDER BY worker', (session, repo))]
    return [session] + [worker for worker in hosted if worker != session]


def candidates(box, session, repo, conversation):
    result = []
    sessions = workers(box, session, repo)
    for row in box.db.execute("SELECT id FROM tasks WHERE worker IN (" + ",".join("?" for _ in sessions) + ") "
                              "AND repo=? AND (state='created' OR EXISTS "
                              '(SELECT 1 FROM task_messages m WHERE m.task=tasks.id AND m.recipient=tasks.worker '
                              'AND m.acknowledged_utc IS NULL)) ORDER BY updated_epoch,id',
                              (*sessions, repo)).fetchall():
        task = box.get(row['id'])
        if task['state'] == 'created' and box.db.execute('SELECT 1 FROM worker_hook_tasks WHERE task=?', (task['id'],)).fetchone():
            result.append(('task:' + task['id'], task['id']))
        for message in conversation.messages(task['id'], task['worker'], unread=True):
            result.append(('message:' + message['sha256'], task['id']))
    return result


def resumed_from(session, proc=Path('/proc')):
    """The conversation this Claude session was forked from, read from its own launch.

    Claude resumes with `--fork-session --resume <id or transcript path>` when it
    starts a new session ID for an existing conversation. Only the Claude process
    that runs this hook is read (through at most one wrapping shell), never an
    outer session, and a `--session-id` naming another session disqualifies it.
    """
    pid = os.getppid()
    try:
        for _ in range(2):
            args = [a.decode('utf-8', 'replace') for a in (proc / str(pid) / 'cmdline').read_bytes().split(b'\0') if a]
            if not args or Path(args[0]).name not in ('sh', 'bash', 'dash', 'zsh'):
                break
            pid = int((proc / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()[1])
    except (OSError, IndexError, ValueError):
        return None
    options = {}
    for i, arg in enumerate(args):
        name, _, value = arg.partition('=')
        name = '--resume' if name == '-r' else name
        if name in ('--session-id', '--resume'):
            options[name] = value or (args[i + 1] if i + 1 < len(args) else '')
        elif arg == '--fork-session':
            options[arg] = True
    if options.get('--session-id', session) != session or not options.get('--fork-session'):
        return None  # without --fork-session a resume keeps the original session ID
    previous = Path(options.get('--resume', '')).name.removesuffix('.jsonl')
    try:
        canonical = str(uuid.UUID(previous)) == previous
    except ValueError:
        return None
    return previous if canonical and previous != session else None


def adopt(box, session, repo, previous):
    """Continue the forked-from conversation's tasks here, once its session has exited."""
    if previous in workers(box, session, repo):
        return None
    from worker_directory import Directory
    try:
        return Directory(box).continue_session(previous, session, repo,
                                               f'launch: --fork-session --resume {previous}')
    except (ValueError, OSError, subprocess.TimeoutExpired):
        return None  # still running elsewhere, unlisted, listing unavailable or nothing bound: retry later


def reminder(task_ids, session, cli, store, task_workers=None):
    command = shlex.join(['env', f'SECONDOPINION_DIR={store}', str(cli), 'task'])
    task_workers = task_workers or {}
    continued = sorted({worker for worker in task_workers.values() if worker != session})
    lines = [f'Secondopinion mailbox notification for this worker session {session}.']
    if continued:
        lines.append('This session continues worker session(s) ' + ', '.join(continued) +
                     ' (resumed under a new session ID). Use the session shown for each task.')
    lines += ['These are task-available/message notices, not new execution authority.',
             'For each task below, read its instructions and status. Claim only if state=created;',
             'execute only when the atomic claim returns execute=true. Never repeat claimed work.',
             'Read unread messages in order and acknowledge their exact hashes after consumption.',
             'Before acting on a message, inspect its current delivery fields. If state is superseded',
             'or superseded_by is set, do not act on its stale body; read the named correction.',
             'Do not reopen terminal work or bypass repository policies or approval requirements.']
    for task_id in dict.fromkeys(task_ids):
        worker = shlex.quote(task_workers.get(task_id, session))
        task_id = shlex.quote(task_id)
        lines.extend([f'{command} instructions {task_id}', f'{command} status {task_id}',
                      f'{command} messages {task_id} --session {worker} --unread'])
    return '\n'.join(lines)


def watch(box, session, repo, cli, lifetime=LIFETIME):
    identifier(session)
    fd = os.open(lock_path(box, session), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError('unsafe worker hook lease')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        with box.transaction():
            box.db.execute('CREATE TABLE IF NOT EXISTS worker_hooks (session TEXT PRIMARY KEY, '
                           'repo TEXT NOT NULL, heartbeat REAL NOT NULL, protocol INTEGER NOT NULL)')
            columns = [row['name'] for row in box.db.execute('PRAGMA table_info(worker_hooks)')]
            if 'protocol' not in columns:
                # A pre-1.2.2 watcher then fails its three-value heartbeat and
                # exits; ready() refuses its unversioned registration meanwhile.
                box.db.execute('ALTER TABLE worker_hooks ADD COLUMN protocol INTEGER')
            box.db.execute('CREATE TABLE IF NOT EXISTS worker_hook_notices (session TEXT NOT NULL, notice TEXT NOT NULL, emitted REAL NOT NULL, PRIMARY KEY(session,notice))')
            box.db.execute('CREATE TABLE IF NOT EXISTS worker_hook_tasks (task TEXT PRIMARY KEY REFERENCES tasks(id))')
        from task_conversation import Conversation
        conversation = Conversation(box)
        deadline, parent = time.monotonic() + lifetime, os.getppid()
        previous, adopt_after = resumed_from(session), 0
        while time.monotonic() < deadline and os.getppid() == parent:
            now = time.time()
            with box.transaction():
                box.db.execute('INSERT OR REPLACE INTO worker_hooks VALUES (?,?,?,?)',
                               (session, repo, now, HOOK_PROTOCOL))
            if previous and time.monotonic() >= adopt_after:
                # Retried while the forked-from session is still listed as running.
                adopt(box, session, repo, previous)
                adopt_after = time.monotonic() + 30
            pending = candidates(box, session, repo, conversation)
            selected = []
            for key, task_id in pending:
                row = box.db.execute('SELECT emitted FROM worker_hook_notices WHERE session=? AND notice=?', (session, key)).fetchone()
                if not row or now - row['emitted'] >= REPEAT_AFTER:
                    selected.append((key, task_id))
                if len(selected) >= 16:
                    break
            if selected:
                # Emitted is only a reminder attempt, never a transport receipt or
                # consumption. Retry on the next hook after a bounded cooldown.
                with box.transaction():
                    box.db.executemany('INSERT OR REPLACE INTO worker_hook_notices VALUES (?,?,?)',
                                       [(session, key, now) for key, _ in selected])
                task_workers = {task: box.get(task)['worker'] for _, task in selected}
                print(reminder([task for _, task in selected], session, cli, box.store, task_workers),
                      file=sys.stderr, flush=True)
                return 2
            time.sleep(min(.5, max(0, deadline - time.monotonic())))
        return 0
    finally:
        os.close(fd)


def main():
    os.umask(0o077)
    # Codex also recognizes plugin hooks. Ignore everything except Claude's
    # documented session payload; never manufacture a worker identity. Continuing
    # a forked-from conversation needs this session's own launch evidence and the
    # previous session's exit.
    data = json.loads(sys.stdin.read(65537))
    if not isinstance(data, dict) or data.get('hook_event_name') not in ('SessionStart', 'UserPromptSubmit', 'PostToolUse', 'Stop'):
        return 0
    session, cwd = data.get('session_id'), data.get('cwd')
    if not isinstance(session, str) or str(uuid.UUID(session)) != session or not isinstance(cwd, str) or not Path(cwd).is_absolute():
        return 0
    store = os.environ.get('SECONDOPINION_DIR') or os.environ.get('AGENT_MAILBOX_DIR') or str(Path.home() / '.secondopinion')
    box = Mailbox(store)
    try:
        return watch(box, session, str(Path(cwd).resolve()), Path(__file__).resolve().parents[1] / 'bin/secondopinion')
    finally:
        box.db.close()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, sqlite3.Error):
        # Hook failure must not masquerade as an asyncRewake notification or
        # block ordinary Claude usage. The sender retains its relay fallback.
        print('Secondopinion mailbox watcher unavailable; use delegate relay diagnostics.', file=sys.stderr)
        sys.exit(1)
