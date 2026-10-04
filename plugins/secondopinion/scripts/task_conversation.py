"""Durable task conversations between the two bound participants.

Messages are independent of task revisions and never grant another execution.
Native delivery is at least observable, not exactly-once consumption. Ambiguous
relay sends require explicit reconciliation before retrying the same message.
"""
import fcntl
import json
import os
from pathlib import Path
import shlex
import stat
import sys
import tempfile
import time
import uuid

from task_mailbox import bounded, digest, emit, identifier, read_text, utc

MAX_MESSAGE = 16384


def message_digest(message):
    return digest(json.dumps({k: message[k] for k in
        ('task', 'id', 'sender', 'recipient', 'reply_to', 'body')},
        sort_keys=True, ensure_ascii=True, separators=(',', ':')))


class Conversation:
    def __init__(self, box):
        self.box, self.db = box, box.db
        with box.transaction():
            self.db.execute("""CREATE TABLE IF NOT EXISTS task_messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                task TEXT NOT NULL REFERENCES tasks(id), id TEXT NOT NULL,
                sender TEXT NOT NULL, recipient TEXT NOT NULL, reply_to TEXT,
                body TEXT NOT NULL, sha256 TEXT NOT NULL, created_utc TEXT NOT NULL,
                acknowledged_utc TEXT, UNIQUE(task,id),
                FOREIGN KEY(task,reply_to) REFERENCES task_messages(task,id))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS message_delivery (
                task TEXT NOT NULL, message TEXT NOT NULL, state TEXT NOT NULL,
                attempt TEXT, receipt TEXT, error TEXT NOT NULL, updated_utc TEXT NOT NULL,
                PRIMARY KEY(task,message),
                FOREIGN KEY(task,message) REFERENCES task_messages(task,id))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS message_supersessions (
                task TEXT NOT NULL, message TEXT NOT NULL, superseded_by TEXT NOT NULL,
                actor TEXT NOT NULL, created_utc TEXT NOT NULL,
                PRIMARY KEY(task,message),
                FOREIGN KEY(task,message) REFERENCES task_messages(task,id),
                FOREIGN KEY(task,superseded_by) REFERENCES task_messages(task,id))""")
            self.db.execute('CREATE INDEX IF NOT EXISTS unread_task_messages '
                            'ON task_messages(task,recipient,sequence) WHERE acknowledged_utc IS NULL')

    def participant(self, task_id, session, repo=None):
        task = self.box.get(task_id)
        identifier(session)
        if session not in (task['worker'], task['requester']):
            raise ValueError('session is not a participant in this task')
        if repo is not None and task['repo'] != str(Path(repo).resolve()):
            raise ValueError('message checkout does not match task')
        return task

    def get(self, task_id, message_id):
        task = self.box.get(task_id)
        identifier(message_id)
        row = self.db.execute('SELECT * FROM task_messages WHERE task=? AND id=?',
                              (task_id, message_id)).fetchone()
        if row is None:
            raise ValueError('unknown task message: ' + message_id)
        message = dict(row)
        if message_digest(message) != message['sha256']:
            raise ValueError('message hash mismatch')
        if {message['sender'], message['recipient']} != {task['worker'], task['requester']}:
            raise ValueError('message participants do not match task')
        delivery = self.db.execute('SELECT * FROM message_delivery WHERE task=? AND message=?',
                                  (task_id, message_id)).fetchone()
        message['delivery'] = dict(delivery) if delivery else None
        if delivery:
            from relay_diagnostics import latest_message
            message['delivery_diagnostics'] = latest_message(self.box, task_id, message_id)
        else:
            message['delivery_diagnostics'] = None
        old = self.db.execute('SELECT superseded_by FROM message_supersessions WHERE task=? AND message=?',
                              (task_id, message_id)).fetchone()
        new = self.db.execute('SELECT s.message FROM message_supersessions s JOIN task_messages m '
                              'ON m.task=s.task AND m.id=s.message '
                              'WHERE s.task=? AND s.superseded_by=? ORDER BY m.sequence',
                              (task_id, message_id)).fetchall()
        message['superseded_by'] = old['superseded_by'] if old else None
        message['supersedes'] = [row['message'] for row in new]
        message['handling'] = ('do_not_act; read correction ' + old['superseded_by']
                               if old else 'consume only if still applicable')
        return message

    def _post(self, task_id, message_id, session, body, reply_to=None, repo=None):
        identifier(message_id)
        if not body.strip() or len(body.encode('utf-8')) > MAX_MESSAGE:
            raise ValueError('message must be nonempty UTF-8, at most 16384 bytes')
        task = self.participant(task_id, session, repo)
        if task['worker'] == task['requester']:
            raise ValueError('conversation requires two distinct participants')
        recipient = task['worker'] if session == task['requester'] else task['requester']
        existing = self.db.execute('SELECT id FROM task_messages WHERE task=? AND id=?',
                                   (task_id, message_id)).fetchone()
        if existing:
            message = self.get(task_id, message_id)
            if (message['sender'], message['body'], message['reply_to']) != (session, body, reply_to):
                raise ValueError('message ID already exists with different sender, content or reply target')
            return message
        if task['state'] == 'created':
            raise ValueError('wait for the worker to claim the task before sending messages')
        if reply_to is not None:
            parent = self.get(task_id, reply_to)
            if parent['recipient'] != session:
                raise ValueError('reply must reference a message from the other participant')
        self.db.execute('''INSERT INTO task_messages
            (task,id,sender,recipient,reply_to,body,sha256,created_utc) VALUES (?,?,?,?,?,?,?,?)''',
            (task_id, message_id, session, recipient, reply_to, body,
             message_digest(dict(task=task_id, id=message_id, sender=session,
                                 recipient=recipient, reply_to=reply_to, body=body)), utc()))
        if recipient == task['worker']:
            self.db.execute("INSERT INTO message_delivery VALUES (?,?,'queued',NULL,NULL,'',?)",
                            (task_id, message_id, utc()))
        return self.get(task_id, message_id)

    def post(self, task_id, message_id, session, body, reply_to=None, repo=None):
        with self.box.transaction():
            return self._post(task_id, message_id, session, body, reply_to, repo)

    def supersede(self, task_id, old_id, new_id, session, body, repo=None, expected_ids=None):
        """Atomically replace unresolved lead messages with one correction."""
        if old_id == new_id:
            raise ValueError('correction message ID must differ from the superseded message ID')
        # Use the same per-task lock as delivery. A state left as `sending` by a
        # killed sender is recoverable once its OS lock is gone; a live relay is
        # rejected before either message changes.
        lock = self.box.store / ('.message-delivery-' + digest(task_id) + '.lock')
        fd = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError('unsafe message delivery lock')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ValueError('message delivery is active; wait for it to finish before superseding') from error
            with self.box.transaction():
                task = self.participant(task_id, session, repo)
                old = self.get(task_id, old_id)
                if session != task['requester'] or old['sender'] != session or old['recipient'] != task['worker']:
                    raise ValueError('only the task lead can supersede its own worker message')
                if old['delivery']['state'] == 'superseded':
                    existing = self.get(task_id, old['superseded_by'])
                    if existing['id'] != new_id or existing['body'] != body:
                        raise ValueError('message is already superseded by a different correction')
                    return existing
                if old['acknowledged_utc'] or old['delivery']['state'] == 'delivered':
                    raise ValueError('consumed or confirmed-delivered messages cannot be superseded; send a correction instead')
                if old['delivery']['state'] not in ('queued', 'sending', 'uncertain'):
                    raise ValueError('message is not eligible for supersession')
                if self.db.execute('SELECT 1 FROM task_messages WHERE task=? AND id=?',
                                   (task_id, new_id)).fetchone():
                    raise ValueError('correction message ID already exists')
                unresolved = self.db.execute("SELECT m.id FROM task_messages m JOIN message_delivery d "
                    "ON d.task=m.task AND d.message=m.id WHERE m.task=? AND m.sender=? AND m.recipient=? "
                    "AND m.acknowledged_utc IS NULL AND d.state IN ('queued','sending','uncertain') "
                    'ORDER BY m.sequence', (task_id, session, task['worker'])).fetchall()
                unresolved_ids = [row['id'] for row in unresolved]
                if old_id not in unresolved_ids:
                    raise ValueError('named message is not in the unresolved lead-message set')
                if expected_ids is not None and expected_ids != unresolved_ids:
                    raise ValueError('unresolved lead-message set changed; expected ' +
                                     ','.join(expected_ids) + ' but found ' + ','.join(unresolved_ids))
                replacement = self._post(task_id, new_id, session, body, repo=repo)
                if replacement['recipient'] != task['worker']:
                    raise ValueError('replacement must be directed to the bound worker')
                when = utc()
                self.db.executemany('INSERT INTO message_supersessions VALUES (?,?,?,?,?)',
                    [(task_id, row['id'], new_id, session, when) for row in unresolved])
                self.db.executemany("UPDATE message_delivery SET state='superseded',error=?,updated_utc=? "
                    'WHERE task=? AND message=?',
                    [('replaced by complete correction ' + new_id, when, task_id, row['id'])
                     for row in unresolved])
                return self.get(task_id, new_id)
        finally:
            os.close(fd)

    def messages(self, task_id, session=None, unread=False):
        self.box.get(task_id)
        if session is not None:
            self.participant(task_id, session)
        if unread and session is None:
            raise ValueError('unread messages require a recipient session')
        query, params = 'SELECT id FROM task_messages WHERE task=?', (task_id,)
        if unread:
            query += " AND recipient=? AND acknowledged_utc IS NULL AND NOT EXISTS " \
                     "(SELECT 1 FROM message_delivery d WHERE d.task=task_messages.task " \
                     "AND d.message=task_messages.id AND d.state='superseded')"
            params += (session,)
        rows = self.db.execute(query + ' ORDER BY sequence', params).fetchall()
        return [self.get(task_id, row[0]) for row in rows]

    def acknowledge(self, task_id, message_id, session, sha256, repo=None):
        with self.box.transaction():
            self.participant(task_id, session, repo)
            message = self.get(task_id, message_id)
            if message['recipient'] != session or message['sha256'] != sha256:
                raise ValueError('message ack requires the recipient and exact consumed content hash')
            self.db.execute('UPDATE task_messages SET acknowledged_utc=COALESCE(acknowledged_utc,?) '
                            'WHERE task=? AND id=?', (utc(), task_id, message_id))
            return self.get(task_id, message_id)

    def delivered(self, task_id, message_id, attempt, receipt,
                  source='relay_callback', actor='delivery-relay'):
        if not receipt.strip() or len(receipt) > 512 or any(c in receipt for c in '\r\n'):
            raise ValueError('receipt must be a nonempty single line, at most 512 characters')
        stale_attempt = False
        with self.box.transaction():
            message = self.get(task_id, message_id)
            delivery = message['delivery']
            if delivery and delivery['attempt'] != attempt:
                self.db.execute('''CREATE TABLE IF NOT EXISTS message_delivery_diagnostics (
                    sequence INTEGER PRIMARY KEY, task TEXT NOT NULL, message TEXT NOT NULL,
                    observed_utc TEXT NOT NULL, details TEXT NOT NULL,
                    FOREIGN KEY(task,message) REFERENCES task_messages(task,id))''')
                details = dict(transport='receipt', stage='stale_attempt_receipt',
                    reason='receipt_for_replaced_attempt', attempt=attempt,
                    current_attempt=delivery['attempt'], receipt_source=source, actor=actor,
                    stale_receipt=receipt)
                self.db.execute('INSERT INTO message_delivery_diagnostics(task,message,observed_utc,details) '
                                'VALUES (?,?,?,?)',
                                (task_id, message_id, utc(), json.dumps(details, sort_keys=True)))
                stale_attempt = True
            elif not delivery or delivery['state'] not in ('sending', 'uncertain', 'delivered', 'superseded'):
                raise ValueError('receipt does not match the current message delivery attempt')
            elif delivery['receipt'] and delivery['receipt'] != receipt:
                raise ValueError('message delivery receipt is immutable')
            elif not stale_attempt:
                first_receipt = delivery['receipt'] is None
                state = 'superseded' if delivery['state'] == 'superseded' else 'delivered'
                error = ('receipt arrived after supersession' if state == 'superseded' else '')
                self.db.execute("UPDATE message_delivery SET state=?,receipt=?,error=?,updated_utc=? "
                                'WHERE task=? AND message=?', (state, receipt, error, utc(), task_id, message_id))
                if first_receipt:
                    self.db.execute('''CREATE TABLE IF NOT EXISTS message_delivery_diagnostics (
                        sequence INTEGER PRIMARY KEY, task TEXT NOT NULL, message TEXT NOT NULL,
                        observed_utc TEXT NOT NULL, details TEXT NOT NULL,
                        FOREIGN KEY(task,message) REFERENCES task_messages(task,id))''')
                    details = dict(transport='receipt', stage='receipt', reason='receipt_persisted',
                                   attempt=attempt, receipt_source=source, actor=actor)
                    self.db.execute('INSERT INTO message_delivery_diagnostics(task,message,observed_utc,details) '
                                    'VALUES (?,?,?,?)',
                                    (task_id, message_id, utc(), json.dumps(details, sort_keys=True)))
        if stale_attempt:
            raise ValueError('receipt belongs to a replaced message delivery attempt; diagnostic retained')
        return self.get(task_id, message_id)

    def receive(self, task_id, session, timeout):
        deadline, progress = time.monotonic() + timeout, time.monotonic() + 60
        while True:
            messages = self.messages(task_id, session, unread=True)
            if messages:
                emit(messages)
                return 0
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                emit([])
                return 124
            if time.monotonic() >= progress:
                print('Waiting for a task message.', file=sys.stderr, flush=True)
                progress = time.monotonic() + 60
            time.sleep(min(.25, remaining))


def worker_instructions(task, message, cli, store):
    command = shlex.join(['env', f'SECONDOPINION_DIR={store}', cli, 'task'])
    task_id, mid, session = map(shlex.quote, (task['id'], message['id'], task['worker']))
    supersedes = ("This is a complete correction superseding these earlier lead messages: " +
                  ', '.join(message['supersedes']) + ".\n" if message['supersedes'] else '')
    return f'''Task conversation message {message['id']} for existing task {task['id']}.
You are the bound worker {task['worker']}. The sender is the task's lead.
{supersedes}Delivery can change after a notification was queued. After reading, if delivery.state
is superseded or superseded_by is set, do not act on that stale body; read the named
correction and apply only the complete current direction.
Read the exact message and its retained conversation using:
  {command} message-read {task_id} {mid}
  {command} status {task_id}
If acknowledged_utc is already set, do not act on this message again.
Otherwise consume it within the original task's authority and repository policies,
then acknowledge the exact sha256 returned by message-read:
  {command} message-ack {task_id} {mid} --session {session} --sha256 HASH
A message is not another task claim. Do not rerun the original assignment or reopen
a terminal task. Reconcile actual work before continuing after interruption.
Send questions, updates or answers directly to the lead through the mailbox:
  {command} message {task_id} --id UNIQUE_REPLY_ID --session {session} --reply-to {mid} --file /absolute/reply.md
The reply file must contain your real response. Never reply to the temporary relay.
When waiting for an answer, end your turn; the lead's reply will notify this session.
Do not mark complete until the authorized work is finished. A reply is not completion.
'''


def deliver(conversation, task_id, message_id, session, cli, timeout, retry=False):
    """One bounded relay. Store before sending; never silently retry an uncertain send."""
    if timeout <= 0:
        raise ValueError('delivery timeout must be positive')
    box = conversation.box
    task = conversation.participant(task_id, session, Path.cwd())
    message = conversation.get(task_id, message_id)
    if session != task['requester'] or message['sender'] != session:
        raise ValueError('only the task lead can deliver its own message to the worker')
    # One lead-delivery owner per task, not per message: newer instructions must
    # not overtake an earlier queued/ambiguous send to this same worker.
    lock = box.store / ('.message-delivery-' + digest(task_id) + '.lock')
    fd = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError('unsafe message delivery lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('another lead message delivery is active for this task; '
                             'wait for it to finish, inspect message-read, then retry the '
                             'original message command with the same ID and content') from error
        message = conversation.get(task_id, message_id)
        if message['delivery']['state'] == 'superseded':
            raise ValueError('message was superseded by correction: ' + str(message['superseded_by']))
        if message['acknowledged_utc'] or message['delivery']['state'] == 'delivered':
            return message
        # A hook reads the durable, ordered conversation itself; it does not
        # resend arbitrary content or invent a native delivery receipt. Even an
        # uncertain relay can be discovered this way without repeating execution.
        from worker_hook import notify
        notification = notify(box, task)
        if notification['state'] == 'queued':
            return dict(message, notification=notification)
        if message['delivery']['state'] != 'queued' and not retry:
            raise ValueError('message delivery is ambiguous; inspect the worker and receipt before message-retry --confirm-not-delivered')
        for previous in conversation.messages(task_id, task['worker'], unread=True):
            if previous['sequence'] < message['sequence'] and previous['delivery']['state'] not in ('delivered', 'superseded'):
                raise ValueError('earlier lead message has unconfirmed delivery: ' + previous['id'] +
                                 '; deliver or reconcile it before sending this message')
        # A name can now belong to a replacement session, or the bound session can
        # be renamed. Check UUID and checkout again for every new attempt, and
        # address the session's current name; the original task route is fixed.
        from worker_directory import Directory
        current = Directory(box).bound(task)
        task = dict(task, worker_name=current['name'], worker_renamed_from=current.get('renamed_from'))
        attempt = str(uuid.uuid4())
        expected_state = message['delivery']['state']
        expected_attempt = message['delivery']['attempt']
        with box.transaction():
            box.db.execute("UPDATE message_delivery SET state='sending',attempt=?,receipt=NULL,error='',updated_utc=? "
                           'WHERE task=? AND message=? AND state=? AND attempt IS ?',
                           (attempt, utc(), task_id, message_id, expected_state, expected_attempt))
            if box.db.execute('SELECT changes()').fetchone()[0] != 1:
                current = conversation.get(task_id, message_id)
                if current['acknowledged_utc'] or current['delivery']['state'] == 'delivered':
                    return current
                if current['delivery']['state'] == 'superseded':
                    raise ValueError('message was superseded by correction: ' +
                                     str(current['superseded_by']))
                raise ValueError('message delivery changed during preparation; inspect message-read before retrying')
        command = shlex.join(['env', f'SECONDOPINION_DIR={box.store}', cli, 'task',
                             'message-delivered', task_id, message_id, '--attempt', attempt])
        from task_mailbox import relay_prompt
        prompt = relay_prompt(task, cli, box.store,
            content=worker_instructions(task, message, cli, box.store),
            receipt_command=command + " --receipt 'ACTUAL_MESSAGE_RECEIPT'")
        exchange, exit_code = None, None
        try:
            with tempfile.TemporaryDirectory(prefix='message-relay-', dir=box.store) as tmp:
                request = Path(tmp) / 'request.md'
                request.write_text(prompt, encoding='utf-8')
                from relay_diagnostics import run_relay
                exchange, exit_code = run_relay(cli, request, box.store,
                                                'message ' + message_id[:100], timeout)
        finally:
            # Also covers interruption and a relay dying after native acceptance.
            with box.transaction():
                box.db.execute("UPDATE message_delivery SET state='uncertain',error='relay ended without a receipt',updated_utc=? "
                               "WHERE task=? AND message=? AND state='sending' AND attempt=?",
                               (utc(), task_id, message_id, attempt))
        message = conversation.get(task_id, message_id)
        from relay_diagnostics import observe, record_message
        details = observe(box.store, exchange, exit_code)
        details['attempt'] = attempt
        if task['worker_renamed_from']:
            details.update(worker_name=task['worker_name'], worker_renamed_from=task['worker_renamed_from'])
        notification = None
        if message['delivery']['receipt']:
            details.update(stage='receipt', reason='receipt_persisted')
            prior = message.get('delivery_diagnostics') or {}
            for key in ('receipt_source', 'actor'):
                if key in prior:
                    details[key] = prior[key]
        else:
            # A worker hook can become available while the relay is failing.
            fallback = notify(box, task)
            details['fallback'] = fallback
            if fallback['state'] == 'queued':
                notification = fallback
        record_message(box, task_id, message_id, details)
        message = conversation.get(task_id, message_id)
        return dict(message, notification=notification) if notification else message
    finally:
        os.close(fd)


def warn_uncertain(message, session):
    delivery = message.get('delivery') or {}
    if delivery.get('state') != 'uncertain':
        return
    task_id, message_id = map(shlex.quote, (message['task'], message['id']))
    lead = shlex.quote(session)
    print(f'ATTENTION: delivery is uncertain for task {message["task"]}, message {message["id"]}.',
          file=sys.stderr)
    print(f'Inspect: secondopinion task message-read {task_id} {message_id}', file=sys.stderr)
    print('If independent evidence proves native acceptance, reconcile the recorded attempt with '
          f'secondopinion task message-reconcile {task_id} {message_id} --session {lead} '
          f'--attempt {shlex.quote(str(delivery.get("attempt") or "ATTEMPT"))} '
          "--receipt ACTUAL_RECEIPT --confirm-accepted", file=sys.stderr)
    print('Only after establishing non-delivery, retry with '
          f'secondopinion task message-retry {task_id} {message_id} --session {lead} '
          '--confirm-not-delivered. Later lead messages remain blocked until this is resolved or '
          'explicitly superseded by a correction.', file=sys.stderr)


def add_commands(commands):
    post = commands.add_parser('message', help='send a durable message to the other task participant')
    post.add_argument('id')
    post.add_argument('--id', dest='message_id', required=True)
    post.add_argument('--session', required=True)
    post.add_argument('--file', required=True)
    post.add_argument('--reply-to')
    post.add_argument('--delivery-timeout', type=bounded, default=120)
    for name in ('message-read', 'message-ack', 'message-delivered', 'message-reconcile',
                 'message-retry', 'message-supersede', 'messages', 'receive'):
        sub = commands.add_parser(name)
        sub.add_argument('id')
        if name.startswith('message-'):
            sub.add_argument('message_id')
        if name in ('message-ack', 'message-reconcile', 'message-retry', 'message-supersede', 'receive'):
            sub.add_argument('--session', required=True)
        if name == 'messages':
            sub.add_argument('--session')
            sub.add_argument('--unread', action='store_true')
        if name == 'message-ack':
            sub.add_argument('--sha256', required=True)
        if name == 'message-delivered':
            sub.add_argument('--attempt', required=True)
            sub.add_argument('--receipt', required=True)
        if name == 'message-reconcile':
            sub.add_argument('--attempt', required=True)
            sub.add_argument('--receipt', required=True)
            sub.add_argument('--confirm-accepted', action='store_true', required=True)
        if name == 'message-retry':
            sub.add_argument('--confirm-not-delivered', action='store_true', required=True)
            sub.add_argument('--delivery-timeout', type=bounded, default=120)
        if name == 'message-supersede':
            sub.add_argument('--id', dest='replacement_id', required=True)
            sub.add_argument('--file', required=True)
            sub.add_argument('--expect-superseded', required=True)
            sub.add_argument('--confirm-ambiguous-prior-delivery', action='store_true', required=True)
            sub.add_argument('--delivery-timeout', type=bounded, default=120)
        if name == 'receive':
            sub.add_argument('--timeout', type=bounded, default=900)


def dispatch(box, args):
    conversation = Conversation(box)
    command = args.command
    if command in ('message', 'message-reconcile', 'message-retry', 'message-supersede', 'message-ack', 'receive'):
        task = conversation.participant(args.id, args.session, Path.cwd())
        caller = os.environ.get('CODEX_THREAD_ID')
        if task['requester'] == args.session and caller and caller != args.session:
            raise ValueError('calling Codex thread does not match the task lead')
    if command == 'message':
        if args.delivery_timeout <= 0:
            raise ValueError('delivery timeout must be positive')
        message = conversation.post(args.id, args.message_id, args.session,
                                    read_text(args.file), args.reply_to, Path.cwd())
        if message['recipient'] == task['worker']:
            message = deliver(conversation, args.id, args.message_id, args.session, args.cli, args.delivery_timeout)
        warn_uncertain(message, args.session)
        emit(message)
        return 0 if not message['delivery'] or message['acknowledged_utc'] or message['delivery']['state'] == 'delivered' or message.get('notification', {}).get('state') == 'queued' else 1
    if command == 'message-retry':
        message = deliver(conversation, args.id, args.message_id, args.session, args.cli, args.delivery_timeout, retry=True)
        warn_uncertain(message, args.session)
        emit(message)
        return 0 if message['acknowledged_utc'] or message['delivery']['state'] == 'delivered' or message.get('notification', {}).get('state') == 'queued' else 1
    if command == 'message-supersede':
        if args.delivery_timeout <= 0:
            raise ValueError('delivery timeout must be positive')
        expected = args.expect_superseded.split(',')
        if not expected or any(not value or identifier(value) != value for value in expected) or len(set(expected)) != len(expected):
            raise ValueError('--expect-superseded must be a comma-separated ordered list of unique message IDs')
        message = conversation.supersede(args.id, args.message_id, args.replacement_id,
                                         args.session, read_text(args.file), Path.cwd(), expected)
        try:
            message = deliver(conversation, args.id, args.replacement_id, args.session,
                              args.cli, args.delivery_timeout)
        except (ValueError, OSError) as error:
            message = conversation.get(args.id, args.replacement_id)
            emit(dict(message=message, supersession_committed=True,
                      delivery=message['delivery']['state'], delivery_error=str(error)))
            print('ATTENTION: supersession committed and correction ' + args.replacement_id +
                  ' remains ' + message['delivery']['state'] +
                  ', but delivery did not complete: ' + str(error), file=sys.stderr)
            return 1
        warn_uncertain(message, args.session)
        emit(message)
        return 0 if message['acknowledged_utc'] or message['delivery']['state'] == 'delivered' or message.get('notification', {}).get('state') == 'queued' else 1
    if command == 'message-read':
        emit(conversation.get(args.id, args.message_id))
    elif command == 'message-ack':
        emit(conversation.acknowledge(args.id, args.message_id, args.session, args.sha256, Path.cwd()))
    elif command == 'message-delivered':
        emit(conversation.delivered(args.id, args.message_id, args.attempt, args.receipt))
    elif command == 'message-reconcile':
        message = conversation.get(args.id, args.message_id)
        if message['sender'] != task['requester'] or args.session != task['requester']:
            raise ValueError('only the task lead can reconcile its worker-message delivery')
        emit(conversation.delivered(args.id, args.message_id, args.attempt, args.receipt,
                                    source='lead_asserted', actor=args.session))
    elif command == 'messages':
        emit(conversation.messages(args.id, args.session, args.unread))
    elif command == 'receive':
        return conversation.receive(args.id, args.session, args.timeout)
    return 0
