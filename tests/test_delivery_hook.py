#!/usr/bin/env python3
"""Failure-before-tool-use and inference-free worker mailbox notification."""
import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest import mock
import uuid

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / 'plugins/secondopinion/scripts'
sys.path.insert(0, str(SCRIPTS))
from task_mailbox import Mailbox, delegate, parser
from task_conversation import Conversation, deliver
from relay_diagnostics import observe, record
from worker_directory import Directory
import worker_hook as hook


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='secondopinion-hook-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        subprocess.run(['git','init','-q',str(self.root)],check=True)
        self.box = Mailbox(self.root / 'store')
        self.addCleanup(self.box.db.close)
        self.worker = str(uuid.uuid4())
        self.task = self.box.create('task', self.worker, 'lead', str(self.root), 'review only', 'peer')
        self.cli = ROOT / 'plugins/secondopinion/bin/secondopinion'
        self.request = self.root / 'request.md'
        self.request.write_text('review only')

    def args(self):
        return parser().parse_args(['--store', str(self.box.store), '--cli', str(self.cli), 'delegate',
            '--id', 'task', '--worker', self.worker, '--worker-name', 'peer', '--requester', 'lead',
            '--file', str(self.request), '--timeout', '0', '--delivery-timeout', '1'])

    def log(self, events):
        exchange = 'fixture-delivery'
        directory = self.box.store / 'exchanges' / exchange
        directory.mkdir(parents=True, exist_ok=True)
        logs = self.box.store / 'responder-logs'
        logs.mkdir(exist_ok=True)
        path = logs / 'fixture.log'
        path.write_text('\n'.join(json.dumps(e) for e in events) + '\n')
        (directory / 'meta').write_text('responder_log=' + str(path) + '\n')
        return exchange

    def api_failure(self):
        return self.log([dict(type='system', subtype='init', tools=['ListAgents', 'SendMessage'])] +
                        [dict(type='system', subtype='api_retry', error='unknown')] * 10)

    def test_retry_before_first_tool_is_observed_without_claiming_api_root_cause(self):
        result = observe(self.box.store, self.api_failure(), 124)
        self.assertEqual(result['stage'], 'relay_inference')
        self.assertEqual(result['reason'], 'claude_api_retries_before_tool_use')
        self.assertEqual(result['tools_called'], [])
        self.assertEqual(result['api_retries'], 10)

    def test_no_log_is_unknown_not_proof_no_tools_ran(self):
        self.assertIsNone(observe(self.box.store, 'missing', 124)['tools_called'])

    def test_malformed_log_prevents_absence_claim(self):
        exchange = self.api_failure()
        with (self.box.store / 'responder-logs/fixture.log').open('a') as stream:
            stream.write('{truncated')
        result = observe(self.box.store, exchange, 124)
        self.assertIsNone(result['tools_called'])
        self.assertEqual(result['stage'], 'relay_unknown')

    def test_symlink_log_is_not_read(self):
        exchange = self.api_failure()
        path = self.box.store / 'responder-logs/fixture.log'
        path.unlink()
        path.symlink_to(self.request)
        self.assertFalse(observe(self.box.store, exchange, 124)['log_complete'])

    def test_missing_tools_distinguished_from_model_failure(self):
        for tools, reason in (([], 'list_agents_unavailable'), (['ListAgents'], 'send_message_unavailable')):
            result = observe(self.box.store, self.log([dict(type='system',subtype='init',tools=tools)]), 0)
            self.assertEqual(result['reason'], reason)

    def test_native_send_and_receipt_gap_are_separate(self):
        for error, reason in ((True, 'send_message_rejected'), (False, 'send_result_observed_without_receipt')):
            exchange = self.log([dict(type='assistant',message=dict(content=[dict(type='tool_use',id='send',name='SendMessage')])),
                dict(type='user',message=dict(content=[dict(type='tool_result',tool_use_id='send',is_error=error)]))])
            self.assertEqual(observe(self.box.store, exchange, 0)['reason'], reason)

    def test_diagnostics_survive_reopen_and_status_inside_transaction(self):
        record(self.box, 'task', observe(self.box.store, self.api_failure(), 124))
        other = Mailbox(self.box.store)
        self.addCleanup(other.db.close)
        with other.transaction():
            self.assertEqual(other.status('task')['delivery_diagnostics']['api_retries'], 10)
        self.assertEqual(other.get('task')['revision'], self.task['revision'])

    def test_timeout_attempts_late_hook_fallback_and_same_id_claims_once(self):
        exchange = self.api_failure()
        absent = dict(transport='worker_hook',state='not_listening')
        queued = dict(transport='worker_hook',state='queued')
        with mock.patch('task_mailbox.Path.cwd',return_value=self.root), \
             mock.patch('relay_diagnostics.run_relay',return_value=(exchange,124)) as run, \
             mock.patch.object(Directory,'bound',return_value={}), \
             mock.patch.object(hook,'notify',side_effect=[absent,queued,queued]), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(delegate(self.box,self.args()),124)
            details = self.box.status('task')['delivery_diagnostics']
            self.assertEqual(details['tools_called'],[])
            self.assertEqual(details['fallback']['state'],'queued')
            self.assertTrue(details['worker_directory_match'])
            self.assertEqual(delegate(self.box,self.args()),124)
            self.assertEqual(run.call_count,1)
        task = self.box.get('task')
        self.assertEqual(task['state'],'created')
        self.assertIsNone(task['delivery_receipt'])
        self.assertIsNone(task['delivered_utc'])
        self.assertTrue(self.box.claim('task',self.worker)['execute'])
        self.assertFalse(self.box.claim('task',self.worker)['execute'])

    def test_real_ask_deadline_before_tools_retains_task_and_diagnostics(self):
        env = dict(os.environ, SECONDOPINION_DIR=str(self.box.store), CODEX_THREAD_ID='lead',
                   SECONDOPINION_CLAUDE=str(ROOT/'tests/fake_task_claude.py'), TASK_FIXTURE_SCENARIO='api-timeout')
        result = subprocess.run([str(self.cli),'delegate','--id','task','--worker',self.worker,
            '--worker-name','peer','--requester','lead','--file',str(self.request),'--timeout','0',
            '--delivery-timeout','1'],env=env,cwd=self.root,text=True,capture_output=True,timeout=20)
        self.assertEqual(result.returncode,1,result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(data['diagnostics']['stage'],'relay_inference',result.stderr)
        self.assertEqual(data['diagnostics']['tools_called'],[])
        self.assertEqual(data['diagnostics']['fallback']['state'],'not_listening')
        self.assertIsNone(data['task']['delivery_receipt'])
        self.assertTrue(self.box.claim('task',self.worker)['execute'])
        self.assertFalse(self.box.claim('task',self.worker)['execute'])

    def test_message_relay_retains_exchange_and_structured_failure_diagnostics(self):
        self.box.claim('task', self.worker)
        conversation = Conversation(self.box)
        conversation.post('task', 'm', 'lead', 'Direction')
        exchange = self.api_failure()
        with mock.patch('task_conversation.Path.cwd', return_value=self.root), \
             mock.patch.object(Directory, 'bound', return_value={'name': 'peer'}), \
             mock.patch('relay_diagnostics.run_relay', return_value=(exchange, 124)):
            message = deliver(conversation, 'task', 'm', 'lead', str(self.cli), 1)
        self.assertEqual(message['delivery']['state'], 'uncertain')
        self.assertEqual(message['delivery_diagnostics']['exchange'], exchange)
        self.assertEqual(message['delivery_diagnostics']['stage'], 'relay_inference')
        self.assertEqual(message['delivery_diagnostics']['api_retries'], 10)

    def test_hook_emits_fixed_notice_without_claim_or_receipt(self):
        hook.make_available(self.box,self.task)
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.1),2)
        self.assertIn('task instructions task',output.getvalue())
        self.assertNotIn('review only',output.getvalue())
        self.assertEqual(self.box.get('task')['state'],'created')
        self.assertIsNone(self.box.get('task')['delivery_receipt'])
        self.assertFalse(hook.ready(self.box,self.task))

    def test_wrong_session_or_checkout_cannot_discover_task(self):
        hook.make_available(self.box,self.task)
        conversation = Conversation(self.box)
        self.assertEqual(hook.candidates(self.box,str(uuid.uuid4()),str(self.root),conversation),[])
        self.assertEqual(hook.candidates(self.box,self.worker,str(self.root/'other'),conversation),[])

    def test_hook_finds_unread_messages_and_excludes_consumed_ones(self):
        self.box.claim('task',self.worker)
        conversation = Conversation(self.box)
        message = conversation.post('task','m','lead','Question')
        self.assertEqual(len(hook.candidates(self.box,self.worker,str(self.root),conversation)),1)
        conversation.acknowledge('task','m',self.worker,message['sha256'])
        self.assertEqual(hook.candidates(self.box,self.worker,str(self.root),conversation),[])

    def test_hook_notice_cooldown_does_not_block_later_new_task(self):
        hook.make_available(self.box,self.task)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.02),2)
            self.assertEqual(hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.02),0)
            hook.make_available(self.box,self.box.create('second',self.worker,'lead',str(self.root),'next','peer'))
            self.assertEqual(hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.02),2)

    def test_queued_conversation_uses_hook_without_relay_or_fake_receipt(self):
        self.box.claim('task',self.worker)
        conversation = Conversation(self.box)
        conversation.post('task','m','lead','Question')
        with mock.patch('task_conversation.Path.cwd',return_value=self.root), \
             mock.patch.object(hook,'notify',return_value=dict(transport='worker_hook',state='queued')), \
             mock.patch('relay_diagnostics.run_relay') as run:
            message = deliver(conversation,'task','m','lead',str(self.cli),1)
        run.assert_not_called()
        self.assertEqual(message['notification']['state'],'queued')
        self.assertIsNone(message['delivery']['receipt'])

    def test_public_binding_refuses_replacement_and_duplicate_name(self):
        row = dict(sessionId=self.worker,name='peer',cwd=str(self.root))
        renamed = dict(row,name='changed')
        for rows in ([dict(row,sessionId=str(uuid.uuid4()))], [row,dict(row,sessionId=str(uuid.uuid4()))],
                     [dict(row,cwd=str(self.root/'other'))], [renamed,dict(renamed,sessionId=str(uuid.uuid4()))],
                     [dict(renamed,cwd=str(self.root/'other'))]):
            with mock.patch.object(Directory,'rows',return_value=rows), self.assertRaises(ValueError):
                Directory(self.box).bound(self.task)

    def test_public_binding_follows_a_rename_of_the_same_session(self):
        # Same UUID and checkout; a restart gave it a new unique name, even with the old name reused.
        row = dict(sessionId=self.worker,name='peer',cwd=str(self.root))
        for rows in ([dict(row,name='peer-2')], [dict(row,name='peer-2'),dict(row,sessionId=str(uuid.uuid4()))]):
            with mock.patch.object(Directory,'rows',return_value=rows):
                current = Directory(self.box).bound(self.task)
            self.assertEqual((current['name'],current['renamed_from']),('peer-2','peer'))
        with mock.patch.object(Directory,'rows',return_value=[row]):
            self.assertNotIn('renamed_from',Directory(self.box).bound(self.task))
        self.assertEqual(self.box.get('task')['worker_name'],'peer')

    def test_delegate_retry_relays_to_the_current_name_after_rename(self):
        rows = [dict(sessionId=self.worker,name='peer-2',cwd=str(self.root))]
        prompts = []

        def relay(cli, request, *args):
            prompts.append(Path(request).read_text())
            return None, 1
        with mock.patch('task_mailbox.Path.cwd',return_value=self.root), \
             mock.patch('relay_diagnostics.run_relay',side_effect=relay), \
             mock.patch.object(Directory,'rows',return_value=rows), \
             mock.patch.object(hook,'notify',return_value=dict(transport='worker_hook',state='not_listening')), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            delegate(self.box,self.args())
        self.assertIn('now named "peer-2"',prompts[0])
        self.assertIn('peer name "peer"',prompts[0])
        details = self.box.status('task')['delivery_diagnostics']
        self.assertEqual((details['worker_name'],details['worker_renamed_from']),('peer-2','peer'))

    def test_hook_notice_after_rename_is_queued_and_reports_both_names(self):
        rows = [dict(sessionId=self.worker,name='peer-2',cwd=str(self.root))]
        with mock.patch.object(hook,'ready',return_value=True), mock.patch.object(Directory,'rows',return_value=rows):
            notice = hook.notify(self.box,self.task)
        self.assertEqual((notice['state'],notice['worker_name'],notice['worker_renamed_from']),('queued','peer-2','peer'))

    def test_draft_task_never_notifies_before_requester_gates(self):
        with contextlib.redirect_stderr(io.StringIO()) as output:
            self.assertEqual(hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.02),0)
        self.assertEqual(output.getvalue(),'')
        self.assertEqual(self.box.get('task')['state'],'created')

    def test_available_notice_requires_original_lead_and_never_claims(self):
        env = dict(os.environ,SECONDOPINION_DIR=str(self.box.store),CODEX_THREAD_ID='lead')
        def available(session):
            return subprocess.run([str(self.cli),'task','available','task','--session',session],
                cwd=self.root,env=env,capture_output=True,text=True,timeout=10)
        self.assertEqual(available('foreign').returncode,1)
        self.assertFalse(self.box.db.execute("SELECT 1 FROM sqlite_master WHERE name='worker_hook_tasks'").fetchone())
        result = available('lead')
        self.assertEqual(json.loads(result.stdout)['delivery'],'queued')
        self.assertEqual(self.box.get('task')['state'],'created')
        self.assertIsNone(self.box.get('task')['delivered_utc'])
        self.assertEqual(self.box.db.execute('SELECT count(*) FROM worker_hook_tasks').fetchone()[0],1)

    def start_watcher(self):
        child = subprocess.Popen([sys.executable,'-B',str(SCRIPTS/'worker_hook.py')],cwd=self.root,
            env=dict(os.environ,SECONDOPINION_DIR=str(self.box.store)),stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        child.stdin.write(json.dumps(dict(hook_event_name='SessionStart',session_id=self.worker,cwd=str(self.root))))
        child.stdin.close()
        child.stdin = None
        def cleanup():
            if child.poll() is None:
                child.terminate()
            child.communicate(timeout=5)
        self.addCleanup(cleanup)
        return child

    def test_process_lease_one_watcher_and_notification_without_relay(self):
        first = self.start_watcher()
        deadline = time.monotonic()+5
        while not hook.ready(self.box,self.task) and time.monotonic()<deadline:
            time.sleep(.05)
        self.assertTrue(hook.ready(self.box,self.task))
        second = self.start_watcher()
        self.assertEqual(second.wait(timeout=5),0)
        with mock.patch.object(Directory,'bound',return_value={}) as bound:
            self.assertEqual(hook.notify(self.box,self.task)['state'],'queued')
        bound.assert_called_once_with(self.task)
        hook.make_available(self.box,self.task)
        _, stderr = first.communicate(timeout=5)
        self.assertEqual(first.returncode,2)
        self.assertIn('task instructions task',stderr)
        self.assertFalse(hook.ready(self.box,self.task))
        with mock.patch.object(Directory,'bound',return_value={}):
            self.assertEqual(hook.notify(self.box,self.task)['observation'],'reminder_emitted')
        self.assertEqual(self.box.get('task')['state'],'created')

    def test_stale_or_future_heartbeat_never_authorizes_hook_transport(self):
        with contextlib.redirect_stderr(io.StringIO()):
            hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.01)
        for timestamp in (time.time()-100,time.time()+100):
            self.box.db.execute('UPDATE worker_hooks SET heartbeat=?',(timestamp,))
            self.assertFalse(hook.ready(self.box,self.task))

    def test_unversioned_old_watcher_is_refused_and_heartbeat_schema_fails_closed(self):
        self.box.db.execute('CREATE TABLE worker_hooks (session TEXT PRIMARY KEY, repo TEXT NOT NULL, heartbeat REAL NOT NULL)')
        self.box.db.execute('INSERT INTO worker_hooks VALUES (?,?,?)',
                            (self.worker, str(self.root), time.time()))
        self.box.db.execute('CREATE TABLE worker_hook_notices '
                            '(session TEXT NOT NULL, notice TEXT NOT NULL, emitted REAL NOT NULL, '
                            'PRIMARY KEY(session,notice))')
        self.box.db.execute('INSERT INTO worker_hook_notices VALUES (?,?,?)',
                            (self.worker, 'task:task', time.time()))
        path = hook.lock_path(self.box, self.worker)
        with path.open('a') as lease:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertFalse(hook.ready(self.box, self.task))
            self.assertEqual(hook.notify(self.box, self.task)['state'], 'not_listening')
            migrated = Mailbox(self.box.store)
            migrated.db.close()
            columns = [row['name'] for row in self.box.db.execute('PRAGMA table_info(worker_hooks)')]
            self.assertIn('protocol', columns)
            with self.assertRaises(sqlite3.OperationalError):
                self.box.db.execute('INSERT OR REPLACE INTO worker_hooks VALUES (?,?,?)',
                                    (self.worker, str(self.root), time.time()))
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(hook.watch(self.box, self.worker, str(self.root), self.cli, lifetime=.01), 0)
        columns = [row['name'] for row in self.box.db.execute('PRAGMA table_info(worker_hooks)')]
        self.assertIn('protocol', columns)
        self.assertEqual(self.box.db.execute('SELECT protocol FROM worker_hooks').fetchone()[0],
                         hook.HOOK_PROTOCOL)

    def test_symlink_worker_lease_cannot_be_opened_for_writing(self):
        hook.lock_path(self.box,self.worker).symlink_to(self.request)
        with self.assertRaises(OSError):
            hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.01)
        self.assertEqual(self.request.read_text(),'review only')

    def test_async_requester_rejection_leaves_draft_invisible_to_hook(self):
        args = self.args()
        args.async_delivery = True
        with mock.patch('task_mailbox.Path.cwd',return_value=self.root), \
             mock.patch('codex_wakeup.automatic_registration',side_effect=ValueError('wrong requester')), \
             self.assertRaisesRegex(ValueError,'wrong requester'):
            delegate(self.box,args)
        with contextlib.redirect_stderr(io.StringIO()) as output:
            self.assertEqual(hook.watch(self.box,self.worker,str(self.root),self.cli,lifetime=.01),0)
        self.assertEqual(output.getvalue(),'')


if __name__ == '__main__':
    unittest.main(verbosity=2)
