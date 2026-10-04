#!/usr/bin/env python3
"""Deterministic delivery, routing and crash-recovery tests (no model calls)."""
import copy
import json
import os
from pathlib import Path
import socket
import sqlite3
import struct
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
from codex_wakeup import Wakeup, AutomaticUnavailable, encode, thread_uuid, automatic_registration
from codex_rpc import Client, ProtocolError, RpcError, socket_path, socket_target, MAX_FRAME


class FakeServer:
    def __init__(self, thread, repo):
        self.thread = dict(id=thread, cwd=str(repo), ephemeral=False, status={"type": "idle"})
        self.calls, self.items, self.sent = [], [], []
        self.failure = None
        self.before_send = None
        self.pages = None

    def __call__(self, path):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def call(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "thread/read":
            return {"thread": copy.deepcopy(self.thread)}
        if method == "thread/items/list":
            if self.pages is not None:
                return self.pages(params)
            return {"data": copy.deepcopy(self.items), "nextCursor": None}
        if method != "turn/start":
            raise AssertionError("unexpected mutation: " + method)
        if self.before_send:
            self.before_send()
        if self.failure == "before":
            raise TimeoutError("lost connection before acceptance")
        if isinstance(self.failure, Exception):
            raise self.failure
        self.sent.append(copy.deepcopy(params))
        turn = "turn-" + str(len(self.sent))
        self.items.insert(0, {"item": dict(type="functionCallOutput", **params["toolOutput"]), "turnId": turn})
        if self.failure == "after":
            raise EOFError("accepted, but response lost")
        return {"turn": {"id": turn}}


class WakeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="secondopinion-wake-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "checkout with spaces"
        self.repo.mkdir()
        self.thread = str(uuid.uuid4())
        self.socket = self.root / "control.sock"
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(self.socket))
        self.socket.chmod(0o600)
        self.addCleanup(self.listener.close)
        self.server = FakeServer(self.thread, self.repo)
        self.wake = Wakeup(self.root / "store", self.server)
        self.addCleanup(self.wake.db.close)
        self.report = self.root / "result"
        self.report.write_text("verified result")
        self.create("a")

    def create(self, task, requester=None, repo=None):
        return self.wake.box.create(task, "worker-" + task, requester or self.thread,
                                    str(repo or self.repo), "authorized reporting-only task")

    def update(self, task="a", state="complete"):
        if self.wake.box.get(task)["state"] == "created":
            self.wake.box.claim(task, "worker-" + task)
        return self.wake.box.update(SimpleNamespace(id=task, session="worker-" + task,
            revision=self.wake.box.get(task)["revision"], state=state, message="test outcome",
            action_needed="owner approval" if state == "needs_attention" else "",
            file=str(self.report) if state == "complete" else None))

    def register(self, tasks=("a",), **kw):
        return self.wake.register(kw.get("name", "route"), kw.get("path", str(self.socket)),
                                  kw.get("thread", self.thread), tasks)

    def events(self):
        return self.wake.status("route")["notifications"]

    def test_register_is_read_only_at_codex_and_does_not_send(self):
        self.register()
        self.assertEqual([m for m, _ in self.server.calls], ["thread/read", "thread/items/list"])
        self.assertEqual(self.events(), [])

    def test_exact_uuid_not_name_or_abbreviation(self):
        for bad in ("named-session", self.thread.upper(), self.thread[:8], "../x"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                thread_uuid(bad)

    def test_private_socket_required(self):
        self.socket.chmod(0o666)
        with self.assertRaises(ValueError):
            self.register()

    def test_regular_file_rejected_directly_or_through_link(self):
        link = self.root / "alias"
        link.symlink_to(self.report)
        for path in (link, self.report):
            with self.assertRaises(ValueError):
                socket_path(str(path))

    def test_unknown_task_is_atomic(self):
        with self.assertRaises(ValueError):
            self.register(("a", "unknown"))
        self.assertEqual(self.wake.db.execute("SELECT count(*) FROM wake_routes").fetchone()[0], 0)

    def test_task_binds_by_requester_not_checkout(self):
        # Lead channels live in the lead's checkout while the Codex conversation works elsewhere.
        self.create("b", repo=self.root)
        self.assertEqual(self.register(("a", "b"))["tasks"], ["a", "b"])
        self.create("c", requester=str(uuid.uuid4()))
        with self.assertRaisesRegex(ValueError, "requester differs"):
            self.register(("c",))
        self.assertEqual(self.wake.status("route")["tasks"], ["a", "b"])

    def test_cli_register_refuses_foreign_task_and_rebind_needs_confirmation(self):
        self.create("c", requester=str(uuid.uuid4()))
        script = Path(__file__).resolve().parents[1] / "plugins/secondopinion/scripts/codex_wakeup.py"

        def run(*args):
            return subprocess.run([sys.executable, "-B", str(script), "--store", str(self.root / "store"), *args],
                                  capture_output=True, text=True, timeout=30)
        refused = run("register", "route", "--socket", str(self.socket), "--thread", self.thread, "--task", "c")
        self.assertEqual((refused.returncode, "requester differs" in refused.stderr), (1, True))
        unconfirmed = run("rebind", "route")
        self.assertEqual((unconfirmed.returncode, "--confirm-folder-change" in unconfirmed.stderr), (2, True))
        self.assertEqual(self.wake.db.execute("SELECT count(*) FROM wake_routes").fetchone()[0], 0)

    def test_wrong_runtime_identity_rejected(self):
        for key, bad in (("id", str(uuid.uuid4())), ("ephemeral", True), ("cwd", None), ("cwd", ""),
                         ("cwd", "relative/checkout")):
            old = self.server.thread[key]
            self.server.thread[key] = bad
            with self.subTest(key=key, bad=bad), self.assertRaises(ValueError):
                self.register()
            self.server.thread[key] = old

    def test_route_pins_the_conversation_folder_not_the_callers(self):
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        self.server.thread["cwd"] = str(elsewhere)
        self.assertEqual(self.register()["repo"], str(elsewhere))
        self.update()
        self.wake.tick("route")
        self.assertEqual(len(self.server.sent), 1)

    def test_moved_conversation_fails_closed_until_rebind(self):
        self.register()
        self.update()
        moved = self.root / "moved"
        moved.mkdir()
        self.server.thread["cwd"] = str(moved)
        with self.assertRaisesRegex(ValueError, "wake rebind route --confirm-folder-change"):
            self.wake.tick("route")
        self.assertEqual(self.server.sent, [])
        with self.assertRaisesRegex(ValueError, "moved"):
            self.register()  # re-registration never silently re-pins
        self.assertEqual(self.wake.status("route")["repo"], str(self.repo))
        status = self.wake.rebind("route")
        self.assertEqual((status["repo"], status["status"]), (str(moved), "registered"))
        self.wake.tick("route")
        self.assertEqual(len(self.server.sent), 1)
        self.assertEqual(self.wake.rebind("route")["repo"], str(moved))

    def test_route_written_by_older_release_still_delivers(self):
        # 1.2.x stored the caller's checkout, which registration required to equal the thread's folder.
        with self.wake.box.transaction():
            self.wake.db.execute("INSERT INTO wake_routes VALUES ('route',?,?,?,1,'registered','',?)",
                                 (str(self.socket), self.thread, str(self.repo.resolve()), "2026-09-17T00:00:00+00:00"))
            self.wake.db.execute("INSERT INTO wake_tasks VALUES ('route','a')")
        self.update()
        self.wake.tick("route")
        self.assertEqual(len(self.server.sent), 1)

    def test_foreign_task_reaching_a_route_is_never_surfaced(self):
        self.register()
        self.create("b", requester=str(uuid.uuid4()))
        with self.wake.box.transaction():
            self.wake.db.execute("INSERT INTO wake_tasks VALUES ('route','b')")
        self.update("b")
        self.wake.tick("route")
        self.assertEqual((self.server.sent, self.events()), ([], []))

    def test_cannot_register_closed_thread(self):
        self.server.thread["status"] = {"type": "notLoaded"}
        with self.assertRaises(ValueError):
            self.register()

    def test_cannot_retarget_or_alias_registration(self):
        self.register()
        with self.assertRaises(ValueError):
            self.register(thread=str(uuid.uuid4()))
        with self.assertRaises(sqlite3.IntegrityError):
            self.register(name="another")

    def test_add_tasks_idempotently(self):
        self.register()
        self.create("b")
        self.assertEqual(self.register(("a", "b", "b"))["tasks"], ["a", "b"])

    def test_task_limit_rolls_back(self):
        self.register()
        for i in range(256):
            self.create("t" + str(i))
        with self.assertRaises(ValueError):
            self.register(tuple("t" + str(i) for i in range(256)))
        self.assertEqual(self.wake.status("route")["tasks"], ["a"])

    def test_one_watcher_lock_and_status(self):
        self.register()
        with self.wake.lock("route"):
            self.assertTrue(self.wake.status("route")["watcher_running"])
            with self.assertRaises(BlockingIOError):
                with self.wake.lock("route"):
                    self.fail("second watcher acquired lock")
        self.assertFalse(self.wake.status("route")["watcher_running"])

    def test_four_outcomes_correct_binding_without_implicit_ack(self):
        for task in ("b", "c", "d"):
            self.create(task)
        self.register(("a", "b", "c", "d"))
        for task, state in zip("abcd", ("complete", "needs_attention", "failed", "refused")):
            self.update(task, state)
        self.wake.tick("route")
        self.assertEqual([e["state"] for e in self.events()], ["recorded"] * 4)
        self.assertEqual(len(self.wake.box.pending(self.thread)), 4)
        for sent, task in zip(self.server.sent, "abcd"):
            self.assertEqual(sent["input"], [])
            payload = json.loads(sent["toolOutput"]["output"])
            self.assertEqual((payload["id"], payload["worker"], payload["requester"]),
                             (task, "worker-" + task, self.thread))
            self.assertEqual(set(sent), {"threadId", "input", "toolOutput"})

    def test_repeated_poll_and_restart_dont_resend(self):
        self.register()
        self.update()
        self.wake.tick("route")
        self.wake.tick("route")
        reopened = Wakeup(self.root / "store", self.server)
        try:
            reopened.tick("route")
        finally:
            reopened.db.close()
        self.assertEqual(len(self.server.sent), 1)

    def test_lost_response_reconciles_without_resending(self):
        self.register()
        self.update()
        self.server.failure = "after"
        with self.assertRaises(EOFError):
            self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "uncertain")
        self.server.failure = None
        self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "recorded")
        self.assertEqual(len(self.server.sent), 1)

    def test_missing_response_and_history_do_not_allow_blind_retry(self):
        self.register()
        self.update()
        self.server.failure = "before"
        with self.assertRaises(TimeoutError):
            self.wake.tick("route")
        self.server.failure = None
        self.wake.tick("route")
        self.assertEqual(self.wake.status("route")["status"], "needs_reconciliation")
        self.assertEqual(self.server.sent, [])
        self.wake.retry("route", self.events()[0]["id"])
        self.wake.tick("route")
        self.assertEqual(len(self.server.sent), 1)

    def test_crash_after_send_commit_is_ambiguous_not_replayed(self):
        self.register()
        self.update()
        self.wake.collect("route")
        self.wake.db.execute("UPDATE wake_outbox SET state='sending'")
        self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "uncertain")
        self.assertEqual(self.server.sent, [])

    def test_definitive_overload_is_safe_to_retry(self):
        self.register()
        self.update()
        self.server.failure = RpcError({"code": -32001, "message": "overloaded"})
        with self.assertRaises(RpcError):
            self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "prepared")
        self.server.failure = None
        self.wake.tick("route")
        self.assertEqual(self.events()[0]["attempts"], 2)

    def test_permanent_rpc_error_is_not_retried(self):
        self.register()
        self.update()
        self.server.failure = RpcError({"code": -32602, "message": "unsupported"})
        with self.assertRaises(RpcError):
            self.wake.tick("route")
        self.server.failure = None
        self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "blocked")
        self.assertEqual(self.server.sent, [])

    def test_waits_for_approval_or_session_without_resuming(self):
        self.register()
        self.update()
        for status in ({"type": "notLoaded"}, {"type": "systemError"},
                       {"type": "active", "activeFlags": ["waitingOnApproval"]}):
            self.server.thread["status"] = status
            self.wake.tick("route")
            self.assertEqual(self.wake.status("route")["status"], "waiting_for_session")
        self.assertEqual(self.server.sent, [])
        self.server.thread["status"] = {"type": "idle"}
        self.wake.tick("route")
        self.assertEqual(len(self.server.sent), 1)

    def test_disable_prevents_further_sends(self):
        self.register()
        self.update()
        self.wake.disable("route")
        self.wake.tick("route")
        self.assertEqual(self.server.sent, [])
        self.assertEqual(self.wake.status("route")["status"], "disabled")

    def test_prepared_attention_is_superseded_but_delivered_attention_is_retained(self):
        self.register()
        self.update(state="needs_attention")
        self.wake.collect("route")
        self.update()
        self.wake.tick("route")
        self.assertEqual([e["state"] for e in self.events()], ["superseded", "recorded"])
        self.assertEqual(len(self.server.sent), 1)

    def test_attention_then_completion_both_arrive(self):
        self.register()
        self.update(state="needs_attention")
        self.wake.tick("route")
        self.update()
        self.wake.tick("route")
        self.assertEqual([e["state"] for e in self.events()], ["recorded", "recorded"])

    def test_report_snapshot_and_context_limit(self):
        self.register()
        self.report.write_text("x" * 20000)
        self.update()
        self.report.write_text("changed outside mailbox")
        self.wake.tick("route")
        payload = json.loads(self.server.sent[0]["toolOutput"]["output"])
        self.assertEqual(len(payload["result"]), 16384)
        self.assertTrue(payload["result_truncated"])
        self.assertEqual(self.wake.box.get("a")["result"], "x" * 20000)

    def test_tampered_outbox_fails_closed(self):
        self.register()
        self.update()
        self.wake.collect("route")
        self.wake.db.execute("UPDATE wake_outbox SET payload='tampered'")
        with self.assertRaises(ValueError):
            self.wake.tick("route")
        self.assertEqual(self.server.sent, [])

    def test_conflicting_history_id_is_blocked(self):
        self.register()
        self.update()
        self.server.failure = "after"
        with self.assertRaises(EOFError):
            self.wake.tick("route")
        item = self.server.items[0]["item"]
        payload = json.loads(item["output"])
        payload["result"] = "conflicting payload"
        item["output"] = encode(payload)
        with self.assertRaises(ValueError):
            self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "blocked")

    def test_paginated_reconciliation(self):
        self.register()
        self.update()
        self.server.failure = "after"
        with self.assertRaises(EOFError):
            self.wake.tick("route")
        self.server.pages = lambda p: ({"data": self.server.items, "nextCursor": None}
            if p.get("cursor") == "older" else {"data": [], "nextCursor": "older"})
        self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "recorded")

    def test_disabling_during_send_is_not_overwritten_by_late_status(self):
        self.register()
        self.update()
        self.server.before_send = lambda: self.wake.disable("route")
        self.wake.tick("route")
        self.assertEqual(self.wake.status("route")["status"], "disabled")
        self.assertEqual(self.wake.status("route")["enabled"], 0)

    def test_active_thread_accepts_without_settings_or_user_input(self):
        self.register()
        self.update()
        self.server.thread["status"] = {"type": "active", "activeFlags": []}
        self.wake.tick("route")
        self.assertEqual(len(self.server.sent), 1)
        self.assertEqual(set(self.server.sent[0]), {"threadId", "input", "toolOutput"})

    def test_reconciliation_rejects_cursor_loops(self):
        self.register()
        self.update()
        self.server.failure = "before"
        with self.assertRaises(TimeoutError):
            self.wake.tick("route")
        self.server.pages = lambda p: {"data": [], "nextCursor": "same"}
        with self.assertRaises(ProtocolError):
            self.wake.tick("route")

    def test_automatic_registration_rejects_foreign_caller(self):
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": str(uuid.uuid4())}):
            with self.assertRaises(ValueError):
                automatic_registration(self.root / "store", "a", self.thread)
        self.assertEqual(self.server.calls, [])

    def test_automatic_registration_requires_live_service_before_any_delivery(self):
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": self.thread}):
            with self.assertRaisesRegex(ValueError, "service is unavailable"):
                automatic_registration(self.root / "store", "a", self.thread)
        self.assertEqual(self.wake.box.get("a")["state"], "created")

    def test_sandboxed_registration_fails_clearly_without_registering(self):
        denied = PermissionError(13, "Permission denied", "/tmp/codex-daemon-1000/hash")
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": self.thread}), \
                mock.patch.object(Wakeup, "service_running", return_value=True), \
                mock.patch("codex_rpc.socket_target", side_effect=denied):
            with self.assertRaisesRegex(ValueError, "inside the sandbox") as caught:
                automatic_registration(self.root / "store", "a", self.thread)
        # Not AutomaticUnavailable: that would quietly fall back to a foreground wait.
        self.assertNotIsInstance(caught.exception, AutomaticUnavailable)
        self.assertIn("wake register codex-" + self.thread, str(caught.exception))
        self.assertIn("--task a", str(caught.exception))
        self.assertEqual(self.wake.db.execute("SELECT count(*) FROM wake_routes").fetchone()[0], 0)
        self.assertEqual(self.wake.box.get("a")["state"], "created")

    def test_running_tasks_do_not_cause_codex_rpc_polling(self):
        self.register()
        self.server.calls.clear()
        self.wake.tick("route")
        self.assertEqual(self.server.calls, [])

    def test_ambiguous_notification_does_not_hide_other_workers(self):
        self.register()
        self.update()
        self.server.failure = "before"
        with self.assertRaises(TimeoutError):
            self.wake.tick("route")
        self.create("b")
        self.register(("b",))
        self.update("b")
        self.server.failure = None
        self.wake.tick("route")
        self.assertEqual([e["state"] for e in self.events()], ["uncertain", "recorded"])
        self.assertEqual(json.loads(self.server.sent[0]["toolOutput"]["output"])["id"], "b")

    def test_explicit_consumption_supersedes_an_unneeded_notification(self):
        self.register()
        task = self.update()
        self.wake.collect("route")
        self.wake.box.acknowledge("a", self.thread, task["revision"])
        self.wake.tick("route")
        self.assertEqual(self.events()[0]["state"], "consumed")
        self.assertEqual(self.server.sent, [])


class SocketLinkTests(unittest.TestCase):
    """The managed Codex daemon publishes a stable link to a per-start private socket."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="so-link-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.control = self.root / "control"  # $CODEX_HOME/app-server-control
        self.daemon = self.root / "daemon"    # /tmp/codex-daemon-$UID
        for directory in (self.control, self.daemon):
            directory.mkdir()
            directory.chmod(0o700)
        self.link = self.control / "app-server-control.sock"
        self.listener = None
        self.start("a1")

    def start(self, name):
        """Simulate a daemon (re)start: a fresh hashed socket and a retargeted link."""
        if self.listener:
            self.listener.close()
            os.unlink(os.readlink(self.link))
            self.link.unlink()
        target = self.daemon / name
        self.listener = socket.socket(socket.AF_UNIX)
        self.addCleanup(self.listener.close)
        self.listener.bind(str(target))
        self.listener.listen(1)
        self.listener.settimeout(1)
        target.chmod(0o600)
        self.link.symlink_to(target)
        return target

    def test_private_daemon_link_registers_unresolved(self):
        self.assertEqual(socket_path(str(self.link)), str(self.link))
        self.assertEqual(socket_target(str(self.link)), str(self.daemon / "a1"))

    def test_link_rejected_when_another_user_could_swap_it(self):
        for directory in (self.control, self.daemon):
            for mode in (0o770, 0o777, 0o1777):
                with self.subTest(directory=directory.name, mode=oct(mode)):
                    directory.chmod(mode)
                    try:
                        with self.assertRaisesRegex(ValueError, "directories"):
                            socket_target(str(self.link))
                    finally:
                        directory.chmod(0o700)

    def test_link_rejected_for_foreign_owner(self):
        real = Path.lstat
        for foreign in (self.control, self.link, self.daemon, self.daemon / "a1"):
            def lstat(path, foreign=foreign, **kw):
                info = real(path, **kw)
                if path != foreign:
                    return info
                fields = list(info)
                fields[4] = os.getuid() + 1  # st_uid
                return os.stat_result(fields)
            with self.subTest(owned_by_other=foreign.name), mock.patch.object(Path, "lstat", lstat):
                with self.assertRaises(ValueError):
                    socket_target(str(self.link))

    def test_only_a_direct_absolute_link_is_followed(self):
        chained = self.control / "chained"
        chained.symlink_to(self.link)
        relative = self.control / "relative"
        relative.symlink_to(os.path.join("..", "daemon", "a1"))
        via = self.root / "via"
        via.symlink_to(self.daemon)
        for path in (chained, relative, via / "a1", self.control / ".." / "daemon" / "a1"):
            with self.subTest(path=str(path)), self.assertRaises(ValueError):
                socket_target(str(path))

    def test_link_target_must_be_a_private_socket(self):
        target = self.daemon / "a1"
        target.chmod(0o660)
        with self.assertRaisesRegex(ValueError, "private"):
            socket_target(str(self.link))
        self.listener.close()
        target.unlink()
        target.write_text("not a socket")
        target.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "private"):
            socket_target(str(self.link))

    def test_stopped_daemon_is_unavailable_not_invalid(self):
        # automatic_registration reports this as AutomaticUnavailable.
        self.listener.close()
        (self.daemon / "a1").unlink()
        with self.assertRaises(FileNotFoundError):
            socket_target(str(self.link))

    def test_connect_follows_link_after_daemon_restart(self):
        for restart in (False, True):
            if restart:
                self.start("b2")
            with self.subTest(restart=restart):
                with self.assertRaises(TimeoutError):  # silent daemon: handshake times out
                    Client(str(self.link), timeout=0.2)
                self.listener.accept()[0].close()

    def test_registration_survives_daemon_restart(self):
        thread = str(uuid.uuid4())
        repo = self.root / "repo"
        repo.mkdir()
        server, connected = FakeServer(thread, repo), []

        def factory(path):
            connected.append(socket_target(path))
            return server
        wake = Wakeup(self.root / "store", factory)
        self.addCleanup(wake.db.close)
        wake.box.create("a", "worker-a", thread, str(repo), "authorized reporting-only task")
        self.assertEqual(wake.register("route", str(self.link), thread, ["a"])["socket"], str(self.link))
        self.start("b2")
        # A stored resolved path would now be stale and refuse as a retarget.
        wake.register("route", str(self.link), thread, ["a"])
        report = self.root / "result"
        report.write_text("verified result")
        wake.box.claim("a", "worker-a")
        wake.box.update(SimpleNamespace(id="a", session="worker-a", revision=wake.box.get("a")["revision"],
            state="complete", message="test outcome", action_needed="", file=str(report)))
        wake.tick("route")
        self.assertEqual(len(server.sent), 1)
        self.assertEqual(connected, [str(self.daemon / "a1")] + [str(self.daemon / "b2")] * 2)


class WireTests(unittest.TestCase):
    def setUp(self):
        a, self.peer = socket.socketpair()
        self.client = Client.__new__(Client)
        self.client.sock, self.client.timeout, self.client.counter = a, 0.1, 0
        self.client.deadline = time.monotonic() + 0.1
        self.addCleanup(a.close)
        self.addCleanup(self.peer.close)

    def frame(self, payload, opcode=1, final=True):
        if isinstance(payload, dict):
            payload = json.dumps(payload).encode()
        prefix = bytes([(128 if final else 0) | opcode])
        size = len(payload)
        prefix += bytes([size]) if size < 126 else b"\x7e" + struct.pack("!H", size)
        self.peer.sendall(prefix + payload)

    def test_ping_between_fragmented_json_frames(self):
        self.frame(b'{"id":1,', final=False)
        self.frame(b"alive", opcode=9)
        self.frame(b'"result":{"ok":true}}', opcode=0)
        self.assertEqual(self.client.call("read", {}), {"ok": True})
        outgoing = self.peer.recv(4096)
        self.assertIn(b"\x8a", outgoing)  # pong emitted, masked as client frames must be

    def test_approval_request_is_never_answered(self):
        self.frame({"id": 1, "method": "item/permissions/requestApproval", "params": {}})
        self.frame({"id": 1, "result": {"ok": True}})
        self.assertEqual(self.client.call("thread/read", {}), {"ok": True})
        data = self.peer.recv(4096)
        size, mask = data[1] & 127, data[2:6]
        decoded = bytes(b ^ mask[i % 4] for i, b in enumerate(data[6:6 + size]))
        self.assertEqual(json.loads(decoded)["method"], "thread/read")
        self.assertEqual(len(data), 6 + size)  # one request, zero approval replies

    def test_server_mask_flags_rejected(self):
        self.peer.sendall(b"\x81\x80")
        with self.assertRaises(ProtocolError):
            self.client._receive()

    def test_oversized_frame_rejected_before_payload_read(self):
        self.peer.sendall(b"\x81\x7f" + struct.pack("!Q", MAX_FRAME + 1))
        with self.assertRaises(ProtocolError):
            self.client._receive()

    def test_illegal_continuation_rejected(self):
        self.frame(b"{}", opcode=0)
        with self.assertRaises(ProtocolError):
            self.client._receive()

    def test_binary_message_rejected(self):
        self.frame(b"{}", opcode=2)
        with self.assertRaises(ProtocolError):
            self.client._receive()

    def test_fragmented_control_rejected(self):
        self.frame(b"x", opcode=9, final=False)
        with self.assertRaises(ProtocolError):
            self.client._receive()

    def test_close_is_not_success(self):
        self.frame(b"", opcode=8)
        with self.assertRaises(EOFError):
            self.client.call("turn/start", {})

    def test_bad_json_and_nonobject_envelopes_fail_closed(self):
        self.frame(b"[]")
        with self.assertRaises(ProtocolError):
            self.client._receive()
        self.frame(b"not-json")
        with self.assertRaises(ValueError):
            self.client._receive()

    def test_silent_peer_hits_bounded_deadline(self):
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            self.client.call("thread/read", {})
        self.assertLess(time.monotonic() - start, 1)

    def test_missing_result_is_not_success(self):
        self.frame({"id": 1})
        with self.assertRaises(ProtocolError):
            self.client.call("thread/read", {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
