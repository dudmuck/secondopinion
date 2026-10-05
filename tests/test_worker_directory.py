#!/usr/bin/env python3
"""Fresh public discovery, exact routing, and failure isolation; no model calls."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins/secondopinion/scripts"))
from codex_wakeup import Wakeup
from task_mailbox import delegate, parser
from worker_directory import Directory


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="secondopinion-directory-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.wake = Wakeup(self.root / "store")
        self.addCleanup(self.wake.db.close)
        self.directory = Directory(self.wake.box)
        self.row = dict(sessionId=str(uuid.uuid4()), name="named worker", cwd=str(self.root), kind="interactive")
        self.run = mock.patch("worker_directory.subprocess.run")
        self.public = self.run.start()
        self.addCleanup(self.run.stop)
        self.output([self.row])

    def output(self, rows):
        self.public.return_value = SimpleNamespace(returncode=0, stdout=json.dumps(rows))

    def test_public_listing_is_fixed_read_only_command(self):
        self.assertEqual(self.directory.rows(), [self.row])
        self.public.assert_called_once_with(["claude", "agents", "--json"], capture_output=True, text=True, timeout=3)

    def test_service_mapping_visible_when_local_process_view_empty(self):
        with self.wake.lock("service"):
            self.directory.refresh()
            self.output([])
            self.assertEqual(self.directory.resolve(self.row["name"], self.root), self.row)
            self.assertEqual(self.public.call_count, 1)

    def test_dead_service_snapshot_not_trusted(self):
        self.directory.refresh()
        self.output([])
        self.assertEqual(self.directory.rows(), [])

    def test_stale_snapshot_not_trusted(self):
        with self.wake.lock("service"):
            self.directory.refresh()
            self.wake.db.execute("UPDATE public_worker_directory SET checked=?", (time.time()-11,))
            self.output([])
            self.assertEqual(self.directory.rows(), [])

    def test_future_snapshot_not_trusted(self):
        with self.wake.lock("service"):
            self.directory.refresh()
            self.wake.db.execute("UPDATE public_worker_directory SET checked=?", (time.time()+100,))
            self.output([])
            self.assertEqual(self.directory.rows(), [])

    def test_missing_name_refused(self):
        with self.assertRaisesRegex(ValueError, "absent or ambiguous"):
            self.directory.resolve("no such worker", self.root)

    def test_duplicate_name_even_in_other_checkout_refused(self):
        self.output([self.row, dict(self.row, sessionId=str(uuid.uuid4()), cwd="/different")])
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            self.directory.resolve(self.row["name"], self.root)

    def test_wrong_checkout_refused(self):
        with self.assertRaisesRegex(ValueError, "different checkout"):
            self.directory.resolve(self.row["name"], self.root / "other")

    def test_headless_responder_is_not_worker(self):
        self.output([dict(self.row, kind="headless")])
        self.assertEqual(self.directory.rows(), [])

    def test_background_session_is_a_worker(self):
        self.output([dict(self.row, kind="background")])
        self.assertEqual(self.directory.rows(), [dict(self.row, kind="background")])

    def bound_task(self):
        old, new = str(uuid.uuid4()), str(uuid.uuid4())
        self.wake.box.create("chan", old, "lead", str(self.root), "standing channel", "named worker")
        return old, new

    def test_forked_session_continues_an_exited_worker(self):
        old, new = self.bound_task()
        self.output([dict(self.row, sessionId=new, kind="background")])
        with self.assertRaisesRegex(ValueError, "worker-continue --from " + old):
            self.directory.bound(self.wake.box.get("chan"))
        result = self.directory.continue_session(old, new, self.root, "test")
        self.assertEqual((result["workers"], result["tasks"]), ([old], ["chan"]))
        current = self.directory.bound(self.wake.box.get("chan"))
        self.assertEqual((current["sessionId"], current["continued_from"]), (new, old))
        self.assertEqual(self.wake.box.get("chan")["worker"], old)  # claims and messages keep the bound ID
        self.assertEqual([tuple(r) for r in self.wake.db.execute("SELECT previous, host FROM worker_continuation_log")],
                         [(old, new)])

    def test_chained_resume_follows_to_the_latest_session(self):
        old, new = self.bound_task()
        newer = str(uuid.uuid4())
        self.output([dict(self.row, sessionId=new)])
        self.directory.continue_session(old, new, self.root, "first")
        self.output([dict(self.row, sessionId=newer)])
        self.assertEqual(self.directory.continue_session(new, newer, self.root, "second")["workers"], sorted([old, new]))
        self.assertEqual(self.directory.bound(self.wake.box.get("chan"))["sessionId"], newer)

    def test_continuation_refused_unless_safe(self):
        old, new = self.bound_task()
        other = self.root / "other"
        other.mkdir()
        for rows, message in (([dict(self.row, sessionId=old), dict(self.row, sessionId=new, name="b")], "still running"),
                              ([], "not running"), ([dict(self.row, sessionId=new, cwd=str(other))], "not running")):
            self.output(rows)
            with self.subTest(message=message, rows=len(rows)), self.assertRaisesRegex(ValueError, message):
                self.directory.continue_session(old, new, self.root, "test")
        self.output([dict(self.row, sessionId=new)])
        for previous, message in ((str(uuid.uuid4()), "no task"), (new, "itself"), ("not-a-uuid", "UUID")):
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.directory.continue_session(previous, new, self.root, "test")
        self.assertEqual(self.wake.db.execute("SELECT count(*) FROM worker_continuations").fetchone()[0], 0)

    def test_private_or_unneeded_metadata_not_published(self):
        self.output([dict(self.row, pid=123, arbitrary="do not persist", startedAt=1234)])
        self.assertEqual(self.directory.rows(), [self.row])

    def test_bad_identity_rejected(self):
        for changes in (dict(sessionId="not-uuid"), dict(cwd="relative"), dict(name="line\nbreak"),
                        dict(name=""), dict(sessionId=None), dict(name="x"*201)):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.output([dict(self.row, **changes)])
                self.directory.rows()

    def test_bad_output_rejected(self):
        for data in ({}, [None], [self.row]*4097):
            with self.subTest(kind=type(data).__name__), self.assertRaises(ValueError):
                self.output(data)
                self.directory.rows()

    def test_failed_refresh_invalidates_previous_good_mapping(self):
        with self.wake.lock("service"):
            self.directory.refresh()
            self.public.side_effect = subprocess.TimeoutExpired("claude", 3)
            self.directory.refresh()
            with self.assertRaisesRegex(ValueError, "unavailable"):
                self.directory.rows()

    def test_failed_command_not_empty_success(self):
        self.public.return_value.returncode = 1
        with self.assertRaises(ValueError):
            self.directory.rows()

    def test_unsafe_lease_rejected(self):
        (self.wake.box.store / ".wake-service.lock").symlink_to(self.root / "elsewhere")
        with self.assertRaises(OSError):
            self.directory.service_running()

    def args(self):
        request = self.root / "request.md"
        request.write_text("report only")
        return parser().parse_args(["--store", str(self.wake.box.store), "--cli", "/unused", "delegate",
            "--id", "named-test", "--worker-name", self.row["name"], "--file", str(request)])

    def test_name_only_delegation_records_uuid_before_delivery(self):
        self.row["cwd"] = str(Path.cwd().resolve())
        self.output([self.row])
        args = self.args()
        self.wake.box.create(args.id, self.row["sessionId"], args.requester, self.row["cwd"], "report only", self.row["name"])
        self.wake.box.claim(args.id, self.row["sessionId"])
        with mock.patch.object(self.wake.box, "wait", return_value=0) as wait:
            self.assertEqual(delegate(self.wake.box, args), 0)
        self.assertEqual(self.wake.box.get(args.id)["worker"], self.row["sessionId"])
        wait.assert_called_once()

    def test_restart_same_name_cannot_retarget_existing_task(self):
        self.row["cwd"] = str(Path.cwd().resolve())
        args = self.args()
        self.wake.box.create(args.id, str(uuid.uuid4()), args.requester, self.row["cwd"], "report only", self.row["name"])
        self.output([self.row])
        with self.assertRaises(ValueError):
            delegate(self.wake.box, args)

    def test_absent_name_does_not_create_task(self):
        self.output([])
        with self.assertRaises(ValueError):
            delegate(self.wake.box, self.args())
        self.assertEqual(self.wake.db.execute("SELECT count(*) FROM tasks").fetchone()[0], 0)

    def test_just_started_worker_waits_for_next_public_refresh(self):
        with mock.patch.object(self.directory, "rows", side_effect=[[], [self.row]]), \
                mock.patch.object(self.directory, "service_running", return_value=True), \
                mock.patch("worker_directory.time.sleep") as sleep:
            self.assertEqual(self.directory.resolve(self.row["name"], self.root), self.row)
            sleep.assert_called_once_with(0.2)

    def test_absent_name_wait_is_bounded(self):
        with mock.patch.object(self.directory, "rows", return_value=[]), \
                mock.patch.object(self.directory, "service_running", return_value=True), \
                mock.patch("worker_directory.time.monotonic", side_effect=[0, 1, 6]), \
                mock.patch("worker_directory.time.sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "absent"):
                self.directory.resolve(self.row["name"], self.root)
            sleep.assert_called_once()

    def test_ambiguity_does_not_wait_for_a_different_peer(self):
        with mock.patch.object(self.directory, "rows", return_value=[self.row, self.row]), \
                mock.patch("worker_directory.time.sleep") as sleep:
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                self.directory.resolve(self.row["name"], self.root)
            sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
