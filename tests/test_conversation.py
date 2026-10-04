#!/usr/bin/env python3
"""Conversation identity, concurrency, recovery, routing and CLI integration."""
import concurrent.futures
import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import shlex
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest import mock

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
CLI = str(ROOT / 'plugins/secondopinion/bin/secondopinion')
sys.path.insert(0, str(ROOT / 'plugins/secondopinion/scripts'))
from task_mailbox import Mailbox, parser
from task_mailbox import digest
from task_conversation import Conversation, deliver, dispatch, MAX_MESSAGE, warn_uncertain, worker_instructions
from codex_wakeup import Wakeup
import test_wakeup as wake_tests


class ConversationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='secondopinion-conversation-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / 'checkout with spaces'
        self.repo.mkdir()
        self.box = Mailbox(self.root / 'store')
        self.addCleanup(self.box.db.close)
        self.box.create('t', 'worker', 'lead', str(self.repo), 'original request', 'bench')
        self.box.claim('t', 'worker')
        self.conversation = Conversation(self.box)
        self.file = self.root / 'text.md'
        self.file.write_text('Which test first?')
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(('SECONDOPINION_', 'AGENT_MAILBOX_'))}
        self.env.pop('CODEX_THREAD_ID', None)
        self.env['SECONDOPINION_DIR'] = str(self.box.store)

    def post(self, mid='q', sender='worker', body='Which test first?', reply_to=None):
        return self.conversation.post('t', mid, sender, body, reply_to, self.repo)

    def cli(self, *args, rc=0, env=None):
        result = subprocess.run([CLI, 'task', *map(str, args)], env=env or self.env,
                                cwd=self.repo, capture_output=True, text=True, timeout=25)
        self.assertEqual(result.returncode, rc, result.stderr + result.stdout)
        return json.loads(result.stdout) if result.stdout.strip() else None

    def send(self, retry=False):
        with mock.patch('task_conversation.Path.cwd', return_value=self.repo):
            return deliver(self.conversation, 't', 'answer', 'lead', CLI, 10, retry)

    def rows(self):
        return [dict(sessionId='worker', cwd=str(self.repo), name='bench')]

    def receipt(self, *args, **kwargs):
        attempt = self.conversation.get('t', 'answer')['delivery']['attempt']
        self.conversation.delivered('t', 'answer', attempt, 'native-receipt')
        return None, 0

    def test_question_reply_chain_and_order(self):
        q = self.post()
        a = self.post('a', 'lead', 'Parser first.', 'q')
        b = self.post('b', 'worker', 'Parser passed.', 'a')
        self.assertLess(q['sequence'], a['sequence'])
        self.assertLess(a['sequence'], b['sequence'])
        self.assertEqual([m['id'] for m in self.conversation.messages('t', 'lead', True)], ['q', 'b'])
        self.assertEqual([m['id'] for m in self.conversation.messages('t', 'worker', True)], ['a'])

    def test_exact_retry_is_idempotent(self):
        self.assertEqual(self.post(), self.post())
        self.assertEqual(len(self.conversation.messages('t')), 1)

    def test_retry_rejects_changed_content_sender_or_parent(self):
        self.post()
        self.post('a', 'lead', 'answer', 'q')
        for kwargs in (dict(body='different'), dict(sender='lead'), dict(reply_to='a')):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.post(**kwargs)

    def test_unknown_sender_rejected_without_message(self):
        with self.assertRaises(ValueError):
            self.post(sender='other')
        self.assertEqual(self.conversation.messages('t'), [])

    def test_wrong_checkout_rejected(self):
        with self.assertRaises(ValueError):
            self.conversation.post('t', 'q', 'worker', 'text', repo=self.root)

    def test_unknown_or_own_reply_target_rejected(self):
        self.post()
        for parent in ('absent', 'q'):
            with self.assertRaises(ValueError):
                self.post('next', reply_to=parent)

    def test_cross_task_reply_rejected(self):
        self.box.create('other', 'worker', 'lead', str(self.repo), 'other')
        self.box.claim('other', 'worker')
        self.conversation.post('other', 'foreign', 'lead', 'other answer')
        with self.assertRaises(ValueError):
            self.post(reply_to='foreign')

    def test_requires_claim_and_distinct_participants(self):
        for name, requester in (('unclaimed', 'lead'), ('same', 'worker')):
            self.box.create(name, 'worker', requester, str(self.repo), 'request')
            with self.assertRaises(ValueError):
                self.conversation.post(name, 'q', 'worker', 'question')

    def test_utf8_byte_limit_and_empty_content(self):
        for body in ('', ' \n', 'x' * (MAX_MESSAGE + 1), 'é' * (MAX_MESSAGE // 2 + 1)):
            with self.subTest(length=len(body)), self.assertRaises(ValueError):
                self.post(body=body)
        self.post(body='é' * (MAX_MESSAGE // 2))

    def test_invalid_message_id(self):
        for mid in ('../x', '', 'x\ny', 'x' * 121):
            with self.assertRaises(ValueError):
                self.post(mid)

    def test_envelope_corruption_detected(self):
        for column, value in (('body', 'corrupt'), ('sender', 'lead'), ('recipient', 'worker'), ('reply_to', 'q')):
            self.post()
            original = self.conversation.get('t', 'q')[column]
            self.box.db.execute('UPDATE task_messages SET ' + column + '=? WHERE id=?', (value, 'q'))
            with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                self.conversation.get('t', 'q')
            self.box.db.execute('UPDATE task_messages SET ' + column + '=? WHERE id=?', (original, 'q'))

    def test_ack_is_exact_recipient_hash_and_idempotent(self):
        message = self.post()
        for session, sha in (('worker', message['sha256']), ('lead', 'wrong'), ('stranger', message['sha256'])):
            with self.assertRaises(ValueError):
                self.conversation.acknowledge('t', 'q', session, sha)
        ack = self.conversation.acknowledge('t', 'q', 'lead', message['sha256'])
        self.assertEqual(ack, self.conversation.acknowledge('t', 'q', 'lead', message['sha256']))
        self.assertEqual(self.conversation.messages('t', 'lead', True), [])
        self.assertEqual(len(self.conversation.messages('t')), 1)

    def test_messages_do_not_change_task_revision_or_completion(self):
        before = self.box.get('t')
        self.post()
        self.assertEqual(self.box.get('t'), before)
        self.box.update(SimpleNamespace(id='t', session='worker', revision=before['revision'],
            state='complete', message='done', action_needed='', file=str(self.file)))
        terminal = self.box.get('t')
        self.post('later', 'lead', 'Explain the result', 'q')
        self.assertEqual(self.box.get('t'), terminal)
        self.assertFalse(self.box.claim('t', 'worker')['execute'])

    def test_ack_message_does_not_ack_outcome(self):
        m = self.post()
        self.conversation.acknowledge('t', 'q', 'lead', m['sha256'])
        self.assertEqual(self.box.db.execute('SELECT COUNT(*) FROM acknowledgments').fetchone()[0], 0)

    def test_reopen_retains_messages_and_acks(self):
        m = self.post()
        self.conversation.acknowledge('t', 'q', 'lead', m['sha256'])
        other = Mailbox(self.box.store)
        try:
            c = Conversation(other)
            self.assertEqual(c.messages('t'), self.conversation.messages('t'))
        finally:
            other.db.close()

    def test_concurrent_retries_insert_once(self):
        def writer(_):
            box = Mailbox(self.box.store)
            try:
                return Conversation(box).post('t', 'q', 'worker', 'same')['sequence']
            finally:
                box.db.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(len(set(pool.map(writer, range(24)))), 1)

    def test_concurrent_distinct_messages_are_all_retained(self):
        def writer(i):
            box = Mailbox(self.box.store)
            try:
                return Conversation(box).post('t', 'q' + str(i), 'worker', str(i))['sequence']
            finally:
                box.db.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            sequences = list(pool.map(writer, range(24)))
        self.assertEqual(len(set(sequences)), 24)
        self.assertEqual(len(self.conversation.messages('t', 'lead', True)), 24)

    def test_storage_full_post_rolls_back_and_same_id_can_be_retried(self):
        self.box.db.execute('PRAGMA max_page_count=' + str(self.box.db.execute('PRAGMA page_count').fetchone()[0]))
        for i in range(20):
            mid = 'full-' + str(i)
            before = self.box.get('t')
            try:
                self.post(mid, 'lead', 'x' * MAX_MESSAGE)
            except sqlite3.OperationalError as error:
                self.assertIn('full', str(error))
                break
        else:
            self.fail('fixture did not exhaust its bounded SQLite pages')
        self.assertIsNone(self.box.db.execute('SELECT id FROM task_messages WHERE task=? AND id=?', ('t', mid)).fetchone())
        self.assertIsNone(self.box.db.execute('SELECT message FROM message_delivery WHERE task=? AND message=?', ('t', mid)).fetchone())
        self.assertEqual(self.box.get('t'), before)
        self.box.db.execute('PRAGMA max_page_count=10000')
        self.assertEqual(self.post(mid, 'lead', 'x' * MAX_MESSAGE)['delivery']['state'], 'queued')
        self.assertEqual(self.box.db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')

    def test_eight_process_burst_retains_1024_exact_messages(self):
        script = self.root / 'burst.py'
        script.write_text('''import sys
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
from task_mailbox import Mailbox
from task_conversation import Conversation
box = Mailbox(sys.argv[2])
conversation = Conversation(box)
for i in range(128):
    mid = sys.argv[3] + '-' + str(i)
    conversation.post('t', mid, 'worker', mid + ': ' + 'payload ' * 32)
box.db.close()
''')
        children = []
        try:
            for i in range(8):
                children.append(subprocess.Popen([sys.executable, '-B', str(script),
                    str(ROOT / 'plugins/secondopinion/scripts'), str(self.box.store), str(i)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
            for child in children:
                stdout, stderr = child.communicate(timeout=45)
                self.assertEqual(child.returncode, 0, stdout + stderr)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)
        messages = self.conversation.messages('t', 'lead', True)
        self.assertEqual(len(messages), 1024)
        self.assertEqual(len({m['sequence'] for m in messages}), 1024)
        self.assertTrue(all(m['body'] == m['id'] + ': ' + 'payload ' * 32 for m in messages))
        self.assertEqual(self.box.db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
        self.assertEqual(self.box.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_hard_kill_during_send_retains_attempt_and_accepts_late_receipt(self):
        self.post('answer', 'lead', 'continue with original task')
        script = self.root / 'killed_sender.py'
        script.write_text('''import sys, time
from pathlib import Path
from unittest import mock
sys.dont_write_bytecode = True
sys.path.insert(0, sys.argv[1])
from task_mailbox import Mailbox
from task_conversation import Conversation, deliver
box = Mailbox(sys.argv[2])
def relay(*args, **kwargs):
    Path(sys.argv[3]).touch()
    time.sleep(60)
with mock.patch('worker_directory.Directory.rows', return_value=[dict(sessionId='worker', cwd=str(Path.cwd()), name='bench')]), mock.patch('relay_diagnostics.run_relay', side_effect=relay):
    deliver(Conversation(box), 't', 'answer', 'lead', sys.argv[4], 10)
''')
        ready = self.root / 'sending'
        with subprocess.Popen([sys.executable, '-B', str(script), str(ROOT / 'plugins/secondopinion/scripts'),
                               str(self.box.store), str(ready), CLI], cwd=self.repo, stderr=subprocess.PIPE) as child:
            try:
                deadline = time.monotonic() + 10
                while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(ready.exists())
                child.kill()
                self.assertEqual(child.wait(timeout=5), -signal.SIGKILL)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
        message = self.conversation.get('t', 'answer')
        self.assertEqual(message['delivery']['state'], 'sending')
        with mock.patch('relay_diagnostics.run_relay') as relay:
            with self.assertRaisesRegex(ValueError, 'ambiguous'):
                self.send()
            relay.assert_not_called()
            self.conversation.delivered('t', 'answer', message['delivery']['attempt'], 'late-native-receipt')
            self.assertEqual(self.send()['delivery']['state'], 'delivered')
            relay.assert_not_called()
        self.assertFalse(self.box.claim('t', 'worker')['execute'])

    def test_cli_worker_message_read_receive_ack_roundtrip(self):
        m = self.cli('message', 't', '--id', 'q', '--session', 'worker', '--file', self.file)
        self.file.unlink()
        self.assertEqual(self.cli('message-read', 't', 'q'), m)
        self.assertEqual(self.cli('receive', 't', '--session', 'lead', '--timeout', '0'), [m])
        self.cli('message-ack', 't', 'q', '--session', 'lead', '--sha256', m['sha256'])
        self.assertEqual(self.cli('receive', 't', '--session', 'lead', '--timeout', '0', rc=124), [])

    def test_foreground_wait_exposes_question_without_completion(self):
        self.post()
        result = self.cli('wait', 't', '--timeout', '0', rc=4)
        self.assertEqual(result['messages'][0]['id'], 'q')
        self.assertEqual(result['task']['state'], 'acknowledged')
        result = self.cli('wait-any', 't', '--consumer', 'lead', '--timeout', '0', rc=4)
        self.assertEqual(result['messages'][0]['id'], 'q')
        self.assertEqual(result['outcomes'], [])

    def test_status_reports_each_recipient_unread_count_without_consuming(self):
        q = self.post()
        self.post('answer', 'lead')
        before = self.box.get('t')
        self.assertEqual(self.cli('status', 't')['unread_messages'], dict(requester=1, worker=1))
        self.assertEqual(self.box.get('t'), before)
        self.assertIsNone(self.conversation.get('t', 'q')['acknowledged_utc'])
        self.conversation.acknowledge('t', 'q', 'lead', q['sha256'])
        self.assertEqual(self.cli('status', 't')['unread_messages'], dict(requester=0, worker=1))

    def test_candidate_schema_upgrade_preserves_conversation_and_acks(self):
        q = self.post()
        self.conversation.acknowledge('t', 'q', 'lead', q['sha256'])
        self.post('answer', 'lead', 'answer', 'q')
        history = self.conversation.messages('t')
        self.box.db.execute('PRAGMA user_version=2')
        upgraded = Mailbox(self.box.store)
        try:
            self.assertEqual(upgraded.db.execute('PRAGMA user_version').fetchone()[0], 3)
            self.assertEqual(Conversation(upgraded).messages('t'), history)
            self.assertFalse(upgraded.claim('t', 'worker')['execute'])
        finally:
            upgraded.db.close()

    def test_terminal_inbox_includes_unread_counts_without_nested_transaction(self):
        self.post()
        self.box.update(SimpleNamespace(id='t', session='worker', revision=self.box.get('t')['revision'],
            state='complete', message='done', action_needed='', file=str(self.file)))
        inbox = self.cli('inbox', '--consumer', 'lead')
        self.assertEqual(inbox[0]['state'], 'complete')
        self.assertEqual(inbox[0]['unread_messages'], dict(requester=1, worker=0))
        ready = self.cli('wait-any', 't', '--consumer', 'lead', '--timeout', 0, rc=4)
        self.assertEqual(ready['outcomes'][0]['unread_messages'], dict(requester=1, worker=0))
        self.assertEqual(ready['messages'][0]['id'], 'q')

    def test_cli_wrong_lead_thread_rejected_before_write(self):
        self.cli('message', 't', '--id', 'q', '--session', 'lead', '--file', self.file,
                 env=dict(self.env, CODEX_THREAD_ID='other'), rc=1)
        self.assertEqual(self.conversation.messages('t'), [])

    def test_cli_invalid_timeout_rejected_before_write(self):
        self.cli('message', 't', '--id', 'q', '--session', 'lead', '--file', self.file, '--delivery-timeout', '0', rc=1)
        self.assertEqual(self.conversation.messages('t'), [])

    def test_cli_fifo_does_not_block(self):
        fifo = self.root / 'fifo'
        os.mkfifo(fifo)
        self.cli('message', 't', '--id', 'q', '--session', 'worker', '--file', fifo, rc=1)

    def test_lead_delivery_receipt_does_not_ack_worker_consumption(self):
        self.post('answer', 'lead', 'Run parser tests')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=self.receipt) as relay:
            delivered = self.send()
            self.assertEqual(delivered['delivery']['state'], 'delivered')
            self.assertIsNone(delivered['acknowledged_utc'])
            self.assertEqual(self.send(), delivered)
            relay.assert_called_once()

    def test_missing_receipt_is_uncertain_and_not_automatically_retried(self):
        self.post('answer', 'lead')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', return_value=(None, 1)) as relay:
            message = self.send()
            self.assertEqual(message['delivery']['state'], 'uncertain')
            self.assertEqual(message['delivery_diagnostics']['reason'], 'relay_ended_without_receipt')
            self.assertEqual(self.box.status('t')['message_delivery_alerts'][0]
                             ['diagnostics']['reason'], 'relay_ended_without_receipt')
            with self.assertRaisesRegex(ValueError, 'ambiguous'):
                self.send()
            relay.assert_called_once()

    def test_receipt_arriving_during_retry_preparation_is_not_erased_or_relayed(self):
        self.post('answer', 'lead')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='old-attempt' WHERE message='answer'")
        def late_receipt(_task):
            other = Mailbox(self.box.store)
            try:
                Conversation(other).delivered('t', 'answer', 'old-attempt', 'late-native')
            finally:
                other.db.close()
            return self.rows()[0]
        with mock.patch('worker_directory.Directory.bound', side_effect=late_receipt), \
             mock.patch('relay_diagnostics.run_relay') as relay, \
             mock.patch('task_conversation.Path.cwd', return_value=self.repo):
            message = deliver(self.conversation, 't', 'answer', 'lead', CLI, 10, retry=True)
        relay.assert_not_called()
        self.assertEqual(message['delivery']['state'], 'delivered')
        self.assertEqual(message['delivery']['receipt'], 'late-native')

    def test_explicit_retry_after_inspection_uses_same_message(self):
        self.post('answer', 'lead')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', return_value=(None, 1)):
            self.send()
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=self.receipt):
            self.assertEqual(self.send(retry=True)['delivery']['state'], 'delivered')
        self.assertEqual(len(self.conversation.messages('t')), 1)

    def test_interrupted_relay_retains_uncertainty(self):
        self.post('answer', 'lead')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            self.send()
        self.assertEqual(self.conversation.get('t', 'answer')['delivery']['state'], 'uncertain')

    def test_late_receipt_after_relay_failure_reconciles(self):
        self.post('answer', 'lead')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', return_value=(None, 1)):
            message = self.send()
        self.conversation.delivered('t', 'answer', message['delivery']['attempt'], 'late-receipt')
        self.assertEqual(self.send()['delivery']['state'], 'delivered')

    def test_stale_or_conflicting_receipt_rejected(self):
        self.post('answer', 'lead')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=self.receipt):
            message = self.send()
        for attempt, receipt in (('wrong', 'native-receipt'), (message['delivery']['attempt'], 'different')):
            with self.assertRaises(ValueError):
                self.conversation.delivered('t', 'answer', attempt, receipt)

    def test_acknowledged_message_is_never_redelivered(self):
        m = self.post('answer', 'lead')
        self.conversation.acknowledge('t', 'answer', 'worker', m['sha256'])
        with mock.patch('relay_diagnostics.run_relay') as relay:
            self.assertIsNotNone(self.send()['acknowledged_utc'])
            relay.assert_not_called()

    def test_renamed_worker_is_addressed_by_its_current_name(self):
        # A restart renamed the bound session; another session now holds the old name.
        self.post('answer', 'lead')
        rows = [dict(self.rows()[0], name='bench-2'), dict(self.rows()[0], sessionId='other')]
        prompts = []

        def relay(cli, request, *args):
            prompts.append(Path(request).read_text())
            return self.receipt()
        with mock.patch('worker_directory.Directory.rows', return_value=rows), \
             mock.patch('relay_diagnostics.run_relay', side_effect=relay):
            message = self.send()
        self.assertEqual(message['delivery']['state'], 'delivered')
        self.assertIn('now named "bench-2"', prompts[0])
        self.assertIn('peer name "bench"', prompts[0])
        self.assertIn('including the old one', prompts[0])
        self.assertIn('"worker_renamed_from": "bench"', json.dumps(message['delivery_diagnostics']))
        self.assertEqual(self.box.get('t')['worker_name'], 'bench')  # the requester's binding is kept

    def test_replacement_wrong_checkout_or_ambiguous_worker_rejected(self):
        self.post('answer', 'lead')
        renamed = dict(self.rows()[0], name='renamed')
        for rows in ([], [dict(self.rows()[0], sessionId='replacement')],
                     [renamed, dict(renamed, sessionId='other')], [dict(self.rows()[0], cwd=str(self.root))],
                     self.rows() * 2, self.rows() + [dict(self.rows()[0], sessionId='other')]):
            with self.subTest(rows=rows), mock.patch('worker_directory.Directory.rows', return_value=rows), \
                 mock.patch('relay_diagnostics.run_relay') as relay, self.assertRaises(ValueError):
                self.send()
            relay.assert_not_called()
        self.assertEqual(self.conversation.get('t', 'answer')['delivery']['state'], 'queued')

    def test_worker_cannot_deliver_lead_message(self):
        self.post('answer', 'lead')
        with mock.patch('task_conversation.Path.cwd', return_value=self.repo), self.assertRaises(ValueError):
            deliver(self.conversation, 't', 'answer', 'worker', CLI, 10)

    def test_duplicate_delivery_cannot_start_another_relay(self):
        self.post('answer', 'lead')
        path = self.box.store / ('.message-delivery-' + digest('t') + '.lock')
        with path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with mock.patch('relay_diagnostics.run_relay') as relay, self.assertRaisesRegex(ValueError, 'same ID and content'):
                self.send()
            relay.assert_not_called()
        self.assertEqual(self.conversation.get('t', 'answer')['delivery']['state'], 'queued')

    def test_unsafe_delivery_lock_rejected_without_touching_target(self):
        self.post('answer', 'lead')
        path = self.box.store / ('.message-delivery-' + digest('t') + '.lock')
        target = self.root / 'protected'
        target.write_text('unchanged')
        path.symlink_to(target)
        with self.assertRaises(OSError):
            self.send()
        self.assertEqual(target.read_text(), 'unchanged')
        path.unlink()
        os.mkfifo(path)
        with self.assertRaises(ValueError):
            self.send()

    def test_outcome_ack_does_not_hide_unconsumed_message(self):
        self.post()
        current = self.box.get('t')
        final = self.box.update(SimpleNamespace(id='t', session='worker', revision=current['revision'],
            state='complete', message='done', action_needed='', file=str(self.file)))
        self.box.acknowledge('t', 'lead', final['revision'])
        result = self.cli('wait-any', 't', '--consumer', 'lead', '--timeout', '0', rc=4)
        self.assertEqual([m['id'] for m in result['messages']], ['q'])
        self.assertEqual(result['outcomes'], [])

    def test_legacy_task_with_same_participant_can_still_be_collected(self):
        self.box.create('legacy', 'same', 'same', str(self.repo), 'legacy request')
        self.cli('wait', 'legacy', '--timeout', '0', rc=124)

    def test_newer_lead_message_cannot_overtake_earlier_queued_message(self):
        earlier = self.post('earlier', 'lead', 'first direction')
        self.post('answer', 'lead', 'second direction')
        with mock.patch('relay_diagnostics.run_relay') as relay, self.assertRaisesRegex(ValueError, 'earlier lead message'):
            self.send()
        relay.assert_not_called()
        self.assertEqual(self.conversation.get('t', 'answer')['delivery']['state'], 'queued')
        self.conversation.acknowledge('t', 'earlier', 'worker', earlier['sha256'])
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=self.receipt):
            self.assertEqual(self.send()['delivery']['state'], 'delivered')

    def test_newer_lead_message_cannot_overtake_ambiguous_message(self):
        self.post('earlier', 'lead', 'first direction')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain' WHERE message='earlier'")
        self.post('answer', 'lead', 'second direction')
        with self.assertRaisesRegex(ValueError, 'earlier lead message'):
            self.send()

    def test_status_prominently_reports_blocking_delivery_and_recovery(self):
        self.post('earlier', 'lead', 'first direction')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='attempt-1',error='no receipt' "
                            "WHERE message='earlier'")
        status = self.box.status('t')
        self.assertEqual(status['unread_messages']['worker'], 1)
        self.assertEqual(len(status['message_delivery_alerts']), 1)
        alert = status['message_delivery_alerts'][0]
        self.assertEqual((alert['id'], alert['state'], alert['attempt']), ('earlier', 'uncertain', 'attempt-1'))
        self.assertTrue(alert['blocks_later_relay_messages'])
        self.assertIn('message-read t earlier', alert['inspect_command'])
        self.assertIn('message-retry t earlier', alert['retry_after_proving_non_delivery'])
        self.assertIn('message-reconcile t earlier', alert['reconcile_after_proving_acceptance'])
        self.assertIn('--attempt attempt-1', alert['reconcile_after_proving_acceptance'])
        self.assertIn('message-supersede t earlier', alert['supersede_with_complete_correction'])
        current = self.box.get('t')
        self.file.write_text('done')
        self.box.update(SimpleNamespace(id='t', session='worker', revision=current['revision'],
            state='complete', message='done', action_needed='', file=str(self.file)))
        self.assertEqual(self.box.status('t')['message_delivery_alerts'][0]['id'], 'earlier')

    def test_uncertain_warning_names_exact_safe_recovery(self):
        self.post('answer', 'lead')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='attempt-1' WHERE message='answer'")
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            warn_uncertain(self.conversation.get('t', 'answer'), 'lead')
        text = output.getvalue()
        self.assertIn('ATTENTION: delivery is uncertain for task t, message answer', text)
        self.assertIn('message-reconcile t answer --session lead --attempt attempt-1', text)
        self.assertIn('message-retry t answer --session lead --confirm-not-delivered', text)

    def test_superseding_ambiguous_message_is_atomic_and_excludes_stale_work(self):
        self.post('earlier', 'lead', 'temporary hold')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='old-attempt' WHERE message='earlier'")
        replacement = self.conversation.supersede('t', 'earlier', 'correction', 'lead',
                                                  'hold cleared', self.repo)
        old = self.conversation.get('t', 'earlier')
        self.assertEqual(old['delivery']['state'], 'superseded')
        self.assertEqual(old['superseded_by'], 'correction')
        self.assertEqual(replacement['supersedes'], ['earlier'])
        self.assertEqual([m['id'] for m in self.conversation.messages('t', 'worker', unread=True)],
                         ['correction'])
        self.assertEqual([key for key, _ in __import__('worker_hook').candidates(
            self.box, 'worker', str(self.repo), self.conversation)], [f'message:{replacement["sha256"]}'])
        alerts = self.box.status('t')['message_delivery_alerts']
        self.assertEqual([(a['id'], a['state']) for a in alerts], [('correction', 'queued')])

    def test_supersession_is_idempotent_but_cannot_change_correction(self):
        self.post('earlier', 'lead', 'temporary hold')
        first = self.conversation.supersede('t', 'earlier', 'correction', 'lead', 'cleared', self.repo)
        self.assertEqual(self.conversation.supersede(
            't', 'earlier', 'correction', 'lead', 'cleared', self.repo), first)
        with self.assertRaisesRegex(ValueError, 'different correction'):
            self.conversation.supersede('t', 'earlier', 'other', 'lead', 'changed', self.repo)

    def test_supersession_rejects_self_or_preexisting_replacement_id(self):
        self.post('earlier', 'lead', 'temporary hold')
        with self.assertRaisesRegex(ValueError, 'must differ'):
            self.conversation.supersede('t', 'earlier', 'earlier', 'lead', 'temporary hold', self.repo)
        self.post('already', 'lead', 'existing content')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.conversation.supersede('t', 'earlier', 'already', 'lead', 'existing content', self.repo)

    def test_superseded_message_cannot_retry_but_late_receipt_is_retained(self):
        self.post('earlier', 'lead', 'temporary hold')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='old-attempt' WHERE message='earlier'")
        self.conversation.supersede('t', 'earlier', 'correction', 'lead', 'cleared', self.repo)
        with mock.patch('task_conversation.Path.cwd', return_value=self.repo), \
             self.assertRaisesRegex(ValueError, 'superseded by correction'):
            deliver(self.conversation, 't', 'earlier', 'lead', CLI, 10, retry=True)
        reconciled = self.conversation.delivered('t', 'earlier', 'old-attempt', 'late-native-receipt')
        self.assertEqual(reconciled['delivery']['state'], 'superseded')
        self.assertEqual(reconciled['delivery']['receipt'], 'late-native-receipt')
        self.assertEqual(reconciled['superseded_by'], 'correction')

    def test_confirmed_delivered_or_consumed_message_cannot_be_superseded(self):
        self.post('earlier', 'lead', 'direction')
        self.box.db.execute("UPDATE message_delivery SET state='sending',attempt='a' WHERE message='earlier'")
        self.conversation.delivered('t', 'earlier', 'a', 'receipt')
        with self.assertRaisesRegex(ValueError, 'cannot be superseded'):
            self.conversation.supersede('t', 'earlier', 'correction', 'lead', 'changed', self.repo)

    def test_reconciled_acceptance_unblocks_next_message_without_resend(self):
        self.post('earlier', 'lead', 'first direction')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='attempt-1' WHERE message='earlier'")
        self.conversation.delivered('t', 'earlier', 'attempt-1', 'native-receipt')
        self.post('answer', 'lead', 'second direction')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=self.receipt):
            self.assertEqual(self.send()['delivery']['state'], 'delivered')

    def test_cli_reconcile_requires_bound_lead_and_records_proven_receipt(self):
        self.post('earlier', 'lead', 'first direction')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='attempt-1' WHERE message='earlier'")
        reconciled = self.cli('message-reconcile', 't', 'earlier', '--session', 'lead',
                              '--attempt', 'attempt-1', '--receipt', 'native-42', '--confirm-accepted')
        self.assertEqual(reconciled['delivery']['state'], 'delivered')
        self.assertEqual(reconciled['delivery']['receipt'], 'native-42')
        self.assertEqual(reconciled['delivery_diagnostics']['receipt_source'], 'lead_asserted')
        self.assertEqual(reconciled['delivery_diagnostics']['actor'], 'lead')
        self.assertEqual(self.box.status('t')['message_delivery_alerts'], [])
        bad = dict(self.env, CODEX_THREAD_ID='other')
        self.cli('message-reconcile', 't', 'earlier', '--session', 'lead', '--attempt',
                 'attempt-1', '--receipt', 'native-42', '--confirm-accepted', rc=1, env=bad)

    def test_active_delivery_cannot_be_superseded(self):
        self.post('earlier', 'lead', 'direction')
        self.box.db.execute("UPDATE message_delivery SET state='sending',attempt='active' WHERE message='earlier'")
        path = self.box.store / ('.message-delivery-' + digest('t') + '.lock')
        with path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError, 'delivery is active'):
                self.conversation.supersede('t', 'earlier', 'correction', 'lead', 'changed', self.repo)
        self.assertEqual([m['id'] for m in self.conversation.messages('t')], ['earlier'])

    def test_orphaned_sending_delivery_can_be_superseded_after_lock_release(self):
        self.post('earlier', 'lead', 'direction')
        self.box.db.execute("UPDATE message_delivery SET state='sending',attempt='orphan' WHERE message='earlier'")
        replacement = self.conversation.supersede(
            't', 'earlier', 'correction', 'lead', 'complete correction', self.repo)
        self.assertEqual(replacement['supersedes'], ['earlier'])
        self.assertEqual(self.conversation.get('t', 'earlier')['delivery']['state'], 'superseded')

    def test_incident_shape_one_correction_replaces_all_unresolved_lead_messages(self):
        self.post('commit-notice', 'lead', 'commit complete; keep hold')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='old' "
                            "WHERE message='commit-notice'")
        self.post('hold-clearance', 'lead', 'clear the hold')
        self.file.write_text('Complete current state: commit complete and all holds cleared.')
        args = SimpleNamespace(command='message-supersede', id='t', message_id='commit-notice',
            replacement_id='correction', session='lead', file=str(self.file), delivery_timeout=10,
            cli=CLI, expect_superseded='commit-notice,hold-clearance',
            confirm_ambiguous_prior_delivery=True)
        emitted = []
        with mock.patch('task_conversation.Path.cwd', return_value=self.repo), \
             mock.patch.dict(os.environ, {'CODEX_THREAD_ID': 'lead'}), \
             mock.patch('worker_hook.notify', return_value=dict(transport='worker_hook', state='queued')), \
             mock.patch('task_conversation.emit', side_effect=emitted.append):
            self.assertEqual(dispatch(self.box, args), 0)
        correction = emitted[0]
        self.assertEqual(correction['supersedes'], ['commit-notice', 'hold-clearance'])
        self.assertEqual([m['id'] for m in self.conversation.messages('t', 'worker', unread=True)],
                         ['correction'])
        self.assertEqual(self.conversation.get('t', 'commit-notice')['delivery']['state'], 'superseded')
        self.assertEqual(self.conversation.get('t', 'hold-clearance')['delivery']['state'], 'superseded')

    def test_worker_protocol_names_supersession_and_status_retains_stale_arrival(self):
        self.post('earlier', 'lead', 'temporary hold')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='old' WHERE message='earlier'")
        correction = self.conversation.supersede(
            't', 'earlier', 'correction', 'lead', 'complete correction', self.repo)
        old = self.conversation.get('t', 'earlier')
        self.assertIn('do_not_act', old['handling'])
        self.assertIn('delivery.state', worker_instructions(self.box.get('t'), old, CLI, self.box.store))
        text = worker_instructions(self.box.get('t'), correction, CLI, self.box.store)
        self.assertIn('complete correction superseding', text)
        self.assertIn('earlier', text)
        self.conversation.acknowledge('t', 'earlier', 'worker', old['sha256'])
        stale = [alert for alert in self.box.status('t')['message_delivery_alerts']
                 if alert['id'] == 'earlier'][0]
        self.assertIsNotNone(stale['acknowledged_utc'])
        self.conversation.delivered('t', 'earlier', 'old', 'late-receipt')
        alerts = self.box.status('t')['message_delivery_alerts']
        stale = [alert for alert in alerts if alert['id'] == 'earlier'][0]
        self.assertFalse(stale['blocks_later_relay_messages'])
        self.assertEqual(stale['risk'], 'superseded_message_reached_or_was_consumed_by_worker')
        self.assertEqual(stale['superseded_by'], 'correction')

    def test_supersede_expected_set_and_post_commit_delivery_failure_are_actionable(self):
        first = self.post('earlier', 'lead', 'old direction')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='old' WHERE message='earlier'")
        self.file.write_text('complete correction')
        args = SimpleNamespace(command='message-supersede', id='t', message_id='earlier',
            replacement_id='correction', session='lead', file=str(self.file), delivery_timeout=10,
            cli=CLI, expect_superseded='earlier', confirm_ambiguous_prior_delivery=True)
        with self.assertRaisesRegex(ValueError, 'set changed'):
            self.conversation.supersede('t', 'earlier', 'correction', 'lead',
                                        'complete correction', self.repo, ['wrong'])
        emitted = []
        with mock.patch('task_conversation.Path.cwd', return_value=self.repo), \
             mock.patch.dict(os.environ, {'CODEX_THREAD_ID': 'lead'}), \
             mock.patch('worker_hook.notify', return_value=dict(transport='worker_hook', state='not_listening')), \
             mock.patch('worker_directory.Directory.bound', side_effect=ValueError('worker unavailable')), \
             mock.patch('task_conversation.emit', side_effect=emitted.append), \
             contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(dispatch(self.box, args), 1)
        self.assertTrue(emitted[0]['supersession_committed'])
        self.assertEqual(emitted[0]['message']['id'], 'correction')
        self.conversation.acknowledge('t', 'earlier', 'worker', first['sha256'])
        with mock.patch('task_conversation.Path.cwd', return_value=self.repo), \
             mock.patch.dict(os.environ, {'CODEX_THREAD_ID': 'lead'}), \
             mock.patch('worker_hook.notify', return_value=dict(transport='worker_hook', state='queued')), \
             mock.patch('task_conversation.emit', side_effect=emitted.append):
            self.assertEqual(dispatch(self.box, args), 0)
        self.assertEqual(emitted[-1]['id'], 'correction')

    def test_receipt_provenance_is_first_writer_and_stale_attempt_is_retained(self):
        self.post('earlier', 'lead', 'direction')
        self.box.db.execute("UPDATE message_delivery SET state='sending',attempt='first' WHERE message='earlier'")
        first = self.conversation.delivered('t', 'earlier', 'first', 'receipt',
                                            source='relay_callback', actor='relay')
        self.conversation.delivered('t', 'earlier', 'first', 'receipt',
                                    source='lead_asserted', actor='lead')
        current = self.conversation.get('t', 'earlier')
        self.assertEqual(first['delivery_diagnostics']['receipt_source'], 'relay_callback')
        self.assertEqual(current['delivery_diagnostics']['receipt_source'], 'relay_callback')
        self.box.db.execute("UPDATE message_delivery SET state='uncertain',attempt='second',receipt=NULL "
                            "WHERE message='earlier'")
        with self.assertRaisesRegex(ValueError, 'replaced'):
            self.conversation.delivered('t', 'earlier', 'first', 'late-old-receipt')
        diagnostic = self.conversation.get('t', 'earlier')['delivery_diagnostics']
        self.assertEqual(diagnostic['stage'], 'stale_attempt_receipt')
        self.assertEqual(diagnostic['current_attempt'], 'second')

    def test_confirmed_earlier_delivery_allows_next_message(self):
        self.post('earlier', 'lead', 'first direction')
        self.box.db.execute("UPDATE message_delivery SET state='sending',attempt='fixture' WHERE message='earlier'")
        self.conversation.delivered('t', 'earlier', 'fixture', 'receipt-1')
        self.post('answer', 'lead', 'second direction')
        with mock.patch('worker_directory.Directory.rows', return_value=self.rows()), \
             mock.patch('relay_diagnostics.run_relay', side_effect=self.receipt):
            self.assertEqual(self.send()['delivery']['state'], 'delivered')


class WakeConversationTests(unittest.TestCase):
    setUp = wake_tests.WakeTests.setUp
    create = wake_tests.WakeTests.create
    register = wake_tests.WakeTests.register
    update = wake_tests.WakeTests.update

    def question(self, mid='q', task='a'):
        if self.wake.box.get(task)['state'] == 'created':
            self.wake.box.claim(task, 'worker-' + task)
        return self.wake.conversation.post(task, mid, 'worker-' + task, 'Question ' + mid)

    def test_idle_notification_contains_exact_message_and_no_task_ack(self):
        m = self.question()
        self.register()
        self.wake.tick('route')
        output = self.server.sent[0]['toolOutput']
        self.assertEqual(output['name'], 'secondopinion_message')
        self.assertEqual(json.loads(output['output'])['message']['sha256'], m['sha256'])
        self.assertIsNone(self.wake.conversation.get('a', 'q')['acknowledged_utc'])
        self.assertEqual(self.wake.status('route')['message_notifications'][0]['state'], 'recorded')

    def test_multiple_messages_survive_later_task_completion(self):
        self.question('q1')
        self.question('q2')
        self.register()
        self.wake.collect('route')
        self.update()
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 3)
        self.assertEqual(sum(s['toolOutput']['name'] == 'secondopinion_message' for s in self.server.sent), 2)

    def test_queued_questions_arrive_before_completion(self):
        self.question('q1')
        self.question('q2')
        self.update()
        self.register()
        self.wake.tick('route')
        outputs = [s['toolOutput'] for s in self.server.sent]
        self.assertEqual([o['name'] for o in outputs],
                         ['secondopinion_message', 'secondopinion_message', 'secondopinion_result'])
        self.assertEqual([json.loads(o['output'])['message']['id'] for o in outputs[:2]], ['q1', 'q2'])

    def test_uncertain_question_holds_same_task_but_not_other_worker(self):
        self.create('b')
        self.question('q1')
        self.register(('a', 'b'))
        self.server.failure = 'before'
        with self.assertRaises(TimeoutError):
            self.wake.tick('route')
        first = self.wake.status('route')['message_notifications'][0]
        self.question('q2')
        self.update()
        self.question(task='b')
        self.server.failure = None
        self.wake.tick('route')
        self.assertEqual([json.loads(s['toolOutput']['output']).get('task') for s in self.server.sent], ['b'])
        self.wake.retry('route', first['id'])
        self.wake.tick('route')
        outputs = [s['toolOutput'] for s in self.server.sent[1:]]
        self.assertEqual([o['name'] for o in outputs],
                         ['secondopinion_message', 'secondopinion_message', 'secondopinion_result'])
        self.assertEqual([json.loads(o['output'])['message']['id'] for o in outputs[:2]], ['q1', 'q2'])

    def test_polling_does_not_rewrite_or_reload_consumed_history(self):
        self.register()
        for i in range(150):
            self.question('old-' + str(i))
        self.wake.collect('route')
        for m in self.wake.conversation.messages('a'):
            self.wake.conversation.acknowledge('a', m['id'], self.thread, m['sha256'])
        self.wake.collect('route')
        changes = self.wake.db.total_changes
        with mock.patch.object(self.wake.conversation, 'get', wraps=self.wake.conversation.get) as get:
            self.wake.collect('route')
            self.assertEqual(get.call_count, 0, 'idle polling must not reload consumed message bodies')
        self.assertEqual(self.wake.db.total_changes, changes, 'consumed history must not be rewritten every poll')
        self.question('new')
        self.wake.tick('route')
        self.assertEqual([json.loads(s['toolOutput']['output'])['message']['id'] for s in self.server.sent], ['new'])

    def test_recorded_unacknowledged_history_is_not_reloaded_every_poll(self):
        self.question()
        self.register()
        self.wake.tick('route')
        with mock.patch.object(self.wake.conversation, 'get', wraps=self.wake.conversation.get) as get:
            self.wake.tick('route')
            self.assertEqual(get.call_count, 0)
        self.assertEqual(len(self.server.sent), 1)

    def test_consumption_marks_only_exact_message(self):
        m = self.question('q1')
        self.question('q2')
        self.register()
        self.wake.tick('route')
        self.wake.conversation.acknowledge('a', 'q1', self.thread, m['sha256'])
        self.wake.tick('route')
        states = {e['message_id']: e['state'] for e in self.wake.status('route')['message_notifications']}
        self.assertEqual(states, dict(q1='consumed', q2='recorded'))
        self.assertEqual(len(self.server.sent), 2)

    def test_service_restart_does_not_replay_recorded_message(self):
        self.question()
        self.register()
        self.wake.tick('route')
        resumed = Wakeup(self.wake.box.store, self.server)
        try:
            resumed.tick('route')
            self.assertEqual(len(self.server.sent), 1)
        finally:
            resumed.db.close()

    def test_lost_rpc_response_reconciles_from_history(self):
        self.question()
        self.register()
        self.server.failure = 'after'
        with self.assertRaises(EOFError):
            self.wake.tick('route')
        self.server.failure = None
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 1)
        self.assertEqual(self.wake.status('route')['message_notifications'][0]['state'], 'recorded')

    def test_uncertain_message_does_not_block_other_worker(self):
        self.create('b')
        self.question()
        self.register(('a', 'b'))
        self.server.failure = 'before'
        with self.assertRaises(TimeoutError):
            self.wake.tick('route')
        self.server.failure = None
        self.question(task='b')
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 1)
        self.assertEqual(json.loads(self.server.sent[0]['toolOutput']['output'])['task'], 'b')

    def test_post_completion_discussion_can_reconcile_an_uncertain_result(self):
        final = self.update()
        self.register()
        self.server.failure = 'before'
        with self.assertRaises(TimeoutError):
            self.wake.tick('route')
        self.question('explanation')
        self.server.failure = None
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 1)
        output = self.server.sent[0]['toolOutput']
        self.assertEqual(output['name'], 'secondopinion_message')
        self.assertEqual(json.loads(output['output'])['message']['id'], 'explanation')
        self.assertEqual(self.wake.status('route')['notifications'][0]['state'], 'uncertain')
        self.assertEqual(self.wake.box.get('a'), final)
        self.assertFalse(self.wake.box.claim('a', 'worker-a')['execute'])

    def test_no_notification_to_a_different_task_consumer(self):
        self.create('b', requester='other-lead')
        self.question(task='b')
        with self.assertRaisesRegex(ValueError, 'requester differs'):
            self.register(('b',))
        # Even a route row that reaches another lead's task never surfaces its messages.
        self.register()
        with self.wake.box.transaction():
            self.wake.db.execute("INSERT INTO wake_tasks VALUES ('route','b')")
        self.wake.tick('route')
        self.assertEqual(self.server.sent, [])

    def test_lead_messages_not_echoed_back_to_lead(self):
        self.question()
        self.wake.conversation.post('a', 'a1', self.thread, 'answer', 'q')
        self.register()
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 1)

    def test_closed_or_approval_waiting_thread_retains_messages(self):
        self.question()
        self.register()
        for runtime in (dict(type='notLoaded'), dict(type='active', activeFlags=['waitingOnApproval'])):
            self.server.thread['status'] = runtime
            self.wake.tick('route')
            self.assertEqual(self.server.sent, [])
        self.server.thread['status'] = dict(type='idle')
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 1)

    def test_ack_before_delivery_prevents_notification(self):
        m = self.question()
        self.register()
        self.wake.collect('route')
        self.wake.conversation.acknowledge('a', 'q', self.thread, m['sha256'])
        self.wake.tick('route')
        self.assertEqual(self.server.sent, [])

    def test_message_outbox_corruption_blocks_delivery(self):
        self.question()
        self.register()
        self.wake.collect('route')
        self.wake.db.execute("UPDATE wake_message_outbox SET payload='corrupt'")
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            self.wake.tick('route')
        self.assertEqual(self.server.sent, [])

    def test_explicit_uncertain_notification_retry(self):
        self.question()
        self.register()
        self.server.failure = 'before'
        with self.assertRaises(TimeoutError):
            self.wake.tick('route')
        event = self.wake.status('route')['message_notifications'][0]
        self.server.failure = None
        self.wake.retry('route', event['id'])
        self.wake.tick('route')
        self.assertEqual(len(self.server.sent), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
