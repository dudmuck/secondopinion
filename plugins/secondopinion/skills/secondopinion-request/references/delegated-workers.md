# Existing Claude workers

Use this mode for an authorized handoff to an existing Claude session on the
same machine and Unix account, with access to the same checkout and mailbox.
Workers with the Claude plugin can receive mailbox reminders through its
`asyncRewake` hook without a relay model call. Relay delivery requires the local
Claude runtime to expose `ListAgents` and `SendMessage`.
Their presence and worker reachability are runtime capabilities;
an environment variable does not prove them. Never edit Claude's private inboxes.

To enable worker hooks, run `./plugins/secondopinion/scripts/install.sh --claude`
from the plugin clone, then restart/resume each worker to load the updated plugin.
Use `--claude` for future updates too: the plain installer intentionally removes
the optional Claude plugin. Confirm the resumed UUID with `secondopinion workers`.
Skill-only installs and `--safe-mode` do not load these hooks. Native hook wakeup
is validated on Claude Code 2.1.274. Each watcher lasts at most 23 hours; ordinary
session, prompt, tool and stop events rearm it. After an idle watcher expires,
relay delivery remains available; resume interaction to rearm the hook.

The hook discovers only tasks/messages for its own session UUID and checkout.
It emits fixed mailbox-reading instructions, never claims a task or writes a
native delivery receipt. A duplicate notice still requires the original atomic
claim or exact message acknowledgment. Worker model access is needed to process
the notice, even though notification itself does not depend on inference.

`delegate` makes the task available after requester/return-registration checks.
Merely running `task create` does not notify a worker. To queue a fixed notice
for an existing unclaimed task, its original lead can run from its checkout:

```bash
secondopinion task available TASK --session REQUESTER_ID
```

This works before claim, accepts no arbitrary message text, and preserves task
state and authority. It returns exit 1 if no hook is currently listening; the
durable notice can still be discovered when that worker loads/rearms its hook.
After a failed relay, `task status TASK` includes `delivery_diagnostics`: exchange
ID, observed stage, API retry count, tool calls/availability, public-directory
match and fallback state. Null/unknown means the evidence was unavailable;
`claude_api_retries_before_tool_use` does not establish the underlying API cause.

1. Use the user's exact worker name with `--worker-name`; the plugin resolves its
   UUID and verifies the checkout automatically. `secondopinion workers` shows
   the public mapping when needed. The installed service publishes a fresh public
   Claude listing for PID-namespaced callers: do not substitute a sandboxed
   `claude agents --json` empty list or ask the user for a UUID/host command.
   Absent, duplicate, stale or wrong-checkout mappings fail closed; never guess a
   peer or start a replacement. An explicitly verified UUID can still be supplied
   using `--worker UUID`. The recorded task binding cannot be retargeted on retry.
2. Write the authorized request into a private file. Include task scope, any
   existing execution authority, required evidence, and restoration obligations.
   Choose a stable task ID; retain it in the owning repository's work record.
3. Run from the target checkout:

   ```bash
   secondopinion delegate --async --id TASK --worker-name EXACT_NAME --file request.md --timeout 900
   ```

   The installer configures the local return service. `--async` automatically
   binds this calling Codex thread and the task before notifying the worker,
   then returns when a hook notice is queued, native delivery is accepted, or
   the worker has independently claimed the task. Do not ask the user to register
   sockets, UUIDs or watchers. Worker results and blockers arrive as
   `secondopinion_result` tool output, including when this Codex thread is idle.
   If the host has no service or this conversation is not connected to the local
   server, the command automatically waits in the foreground instead. Keep that
   command session alive; no manual user setup is needed for fallback collection.
   The bounded relay defaults to 120 seconds (`--delivery-timeout`), and fallback
   waits up to `--timeout` seconds for the worker. The total bound includes both phases
   and the relay's ten-second termination window. The relay is explicitly
   allowed to use the native messaging tools. The worker receives absolute,
   shell-quoted CLI/store paths and instructions to claim and report directly
   to the mailbox. It must claim with `execute=true` before starting work.
4. Read the JSON. With `notification=automatic`, exit 0 means tracking is active:
   `delivery=queued` means a hook reminder is pending, `accepted` means a native
   receipt exists, and `worker_acknowledged` means the worker claimed independently.
   None means the worker completed. The registered service handles later
   results; Codex may continue other authorized work or yield while it waits.
   Without that field (foreground/fallback), exit 0 means a hash-validated
   report (`state=complete`). Exit 3 means refusal, failure, or `needs_attention`;
   report its reason/action_needed. Exit 124 means the foreground wait expired,
   not that the worker stopped. Exit 1 means delivery is unconfirmed or a local
   error occurred. Inspect the retained task and relay exchange before retrying.
   Exit 4 means worker conversation messages are ready; consume, acknowledge and
   answer them as described below, then resume collection of the same task.
5. Consume the result and record its conclusion/evidence in the owning project.
   Then explicitly acknowledge the exact revision:

   ```bash
   secondopinion task ack TASK --consumer CODEX_THREAD_ID --revision N
   ```

   Use your actual thread ID (or a stable, explicit consumer ID). `result` and
   `wait` never acknowledge implicitly, so a caller dying before consuming the
   result cannot make it disappear. Repeating an ack is idempotent. The relay's
   sealed exchange can separately be read and archived through normal commands.

## Ongoing conversation

Once the worker has claimed a task, either participant can send questions,
answers, updates or direction without replacing the task's original request:

```bash
secondopinion task message TASK --id MESSAGE_ID --session YOUR_SESSION_ID --file message.md
secondopinion task message TASK --id REPLY_ID --session YOUR_SESSION_ID --reply-to MESSAGE_ID --file reply.md
```

For the lead, `YOUR_SESSION_ID` is the original calling `CODEX_THREAD_ID`; for
the worker, it is the assigned Claude session UUID. Run from the task's exact
checkout. IDs are unique within a task. Retry with the SAME ID, sender, reply
target and file contents; changing any of them is rejected. Message files must
be nonempty UTF-8, at most 16 KiB. The mailbox snapshots them immediately.

Lead messages use a listening worker hook or a bounded relay to the same existing worker. The plugin
rechecks its UUID, unique name and checkout on every attempt. Worker messages
are stored immediately and automatically notify the registered lead, including
when idle, as `secondopinion_message`. Workers waiting for answers can end their
turn; the lead's reply notifies that same worker through its hook or native relay.

With `notification.transport=worker_hook`, a successful send means the durable
message is queued for worker discovery. Its native receipt stays null; recipient
acknowledgment remains the proof of consumption. The hook reads unread messages
in sequence, including messages from an uncertain earlier relay.

Read and consume messages separately from task completion:

```bash
secondopinion task messages TASK
secondopinion task messages TASK --session YOUR_SESSION_ID --unread
secondopinion task message-read TASK MESSAGE_ID
secondopinion task message-ack TASK MESSAGE_ID --session YOUR_SESSION_ID --sha256 EXACT_SHA256
secondopinion task receive TASK --session YOUR_SESSION_ID --timeout 900
```

The recipient acknowledges only after consuming the exact message. `sha256`
binds its task, ID, sender, recipient, reply target and body. Repeated acks are
idempotent. Reading/delivery does not acknowledge implicitly. To answer a worker
question, consume it, acknowledge it, then send your answer with `--reply-to`.
If the work needs owner approval, retain that blocker until approval is given.

Without automatic return, foreground `delegate`, `task wait`, and `wait-any`
return **exit 4** with a JSON `messages` list. `wait`/`delegate` also include the
current `task`; `wait-any` includes any ready `outcomes`. Handle both, then resume
waiting. `receive` waits only for messages (0 when ready, 124 on timeout). It
does not consume them. Existing task result acknowledgments remain separate.

Message delivery exit 0 means the message is stored (worker to lead), or a hook
notice is queued, native delivery is accepted, or consumption is confirmed
(lead to worker). Inspect `notification` and the stored delivery/acknowledgment
fields to distinguish them. It does not mean the work is complete.
Exit 1 also covers a contended send: if another delivery is active,
wait for it to finish and inspect `message-read`. A still-queued message can be
retried with the original ID and content. Scripts must inspect stored delivery
state rather than infer ambiguity from exit 1 alone. `task status` exposes unread
counts for the requester and worker without consuming anything.

An interrupted/unconfirmed lead relay retains an ambiguous attempt;
inspect `message-read` and the worker before explicitly retrying:

```bash
secondopinion task message-retry TASK MESSAGE_ID --session LEAD_SESSION_ID --confirm-not-delivered
```

Only use that confirmation after establishing that delivery did not occur.
`task status TASK` lists every unresolved worker-message delivery under
`message_delivery_alerts`, with retained relay diagnostics and exact recovery
commands. If independent evidence proves native acceptance, reconcile the
recorded attempt without resending:

```bash
secondopinion task message-reconcile TASK MESSAGE_ID --session LEAD_SESSION_ID \
  --attempt ATTEMPT --receipt ACTUAL_RECEIPT --confirm-accepted
```

If old direction is obsolete, send one complete correction while atomically
suppressing every unresolved lead message for that task:

```bash
secondopinion task message-supersede TASK OLD_MESSAGE_ID --id CORRECTION_ID \
  --session LEAD_SESSION_ID --file /absolute/correction.md \
  --expect-superseded OLD_MESSAGE_ID,NEWER_BLOCKED_MESSAGE_ID \
  --confirm-ambiguous-prior-delivery
```

That confirmation is required because an ambiguous old send may already have
arrived. Old messages remain auditable but cannot be retried and are excluded
from the worker's unread work queue. The correction identifies the full replaced
set and has its own delivery state; inspect it before sending anything later.
A live `sending` attempt is rejected; an orphaned `sending` state becomes
replaceable after its OS delivery lock is gone. Late receipts or acknowledgments
for superseded content remain visible in status as non-blocking risk alerts.
Copy the ordered expected set from `message_delivery_alerts`; the command fails
without changing either message if another unresolved lead message appeared.
An acknowledged message or recorded receipt is never automatically redelivered.
Messages and their notifications survive restarts and later task state changes;
questions cannot be overwritten by subsequent progress. Conversation can discuss
a terminal result, but cannot reopen a terminal task or grant another execution.
No automatic message pruning. Delivery/consumption is not promised exactly once
across arbitrary failures. Existing approval and sandbox boundaries still apply.

## Several existing workers

Start independent interactive sessions in separate terminals in the target checkout:

```bash
claude --name worker-a
claude --name worker-b
```

Each command stays in its own terminal. The plugin verifies the public
UUID/name/checkout mapping before sending. Use as many existing workers as the
authorized task needs and the host supports; do not infer permission to create
more workers or edit shared files merely from their availability.

Give each distinct assignment its own stable task ID and request file. Launch
the authorized `delegate` calls concurrently using the host's managed command
sessions, keeping those sessions alive. Do not background them with an unowned
shell `&` inside a sandbox. Separate file ownership or use independent checkouts
for concurrent writers; the mailbox prevents duplicate claims, not edit conflicts
or two different task IDs triggering the same external action.

To collect ready outcomes without waiting behind the slowest worker:

```bash
secondopinion task wait-any TASK_A TASK_B TASK_C --consumer COORDINATOR_ID --timeout 900
```

This returns a JSON list of unacknowledged outcomes for **only those explicit
task IDs**, ordered by update time. Exit 0 means the returned outcomes are all
complete; exit 3 means at least one needs attention, failed, or refused. Neither
code means every selected task is finished. Exit 124 returns an empty list when
no new outcome arrives before the deadline. Unknown task IDs are errors, not
silently omitted. Duplicate IDs are deduplicated; at most 256 distinct IDs can
be selected, which is an input bound, not a qualified live-worker capacity.

Consume each returned outcome and acknowledge its exact revision with the same
consumer ID, then call `wait-any` again while other tasks remain outstanding.
Handle a blocker's required action separately; it does not prevent collecting
the other workers. A later completion becomes visible even if the earlier
attention revision was acknowledged. After all selected tasks are terminal and
consumed, stop waiting. Repeated waits do not acknowledge or execute anything.

After resolving and acknowledging an approval blocker, use `wait-any` with that
same consumer to wait for the next outcome. Plain `task wait` deliberately returns
the currently recorded needs_attention immediately, even if a continuation message
has just been delivered; delivery does not prove the worker has processed it yet.

Without acknowledgment, the same outcome will be returned again intentionally.
Use one stable consumer ID for a logical coordinator across recovery. Different
consumer IDs each see their own unconsumed outcomes; acknowledgments are not a
distributed leader-election or exactly-once downstream-processing mechanism.

### Harmless manual acceptance test

In a disposable checkout, open four named sessions (`worker-a` through `worker-d`)
as above. After installing this release and starting a new Codex thread, ask:

> Use these four existing workers for reporting-only tests. Give each a stable
> task ID. A should return A_OK after a short delay; B should return B_OK after a
> longer delay; C should report needs_attention because it needs my approval;
> D should refuse. Collect and acknowledge outcomes as they arrive without
> waiting for the slowest worker. Touch no hardware or production files.

Then authorize C's harmless reporting continuation, and retry A using its
original task ID/request/worker binding. Verify C's later completion appears,
A is not executed again, and no result is attributed to another worker. To test
coordinator recovery, stop a wait before completion and use the saved task IDs
and consumer ID in a new wait; unacknowledged results must remain available.

## Recovery and monitoring

```bash
secondopinion task status TASK
secondopinion task wait TASK --timeout 900
secondopinion task result TASK
secondopinion task events TASK
secondopinion task inbox --consumer CODEX_THREAD_ID
```

`inbox` shows unacknowledged terminal/attention tasks for the current exact
checkout; `--repo PATH` selects another. Events and reports survive relay exit,
worker exit, and monitor restart. Worker reports are snapshotted inside the
mailbox; a disappearing `/tmp` report file cannot destroy the published result.

A delivery receipt records acceptance only. `acknowledged` means a worker
claimed the task; `running` means it reported execution; `complete` requires a
report. Timestamp age is observable; process liveness is not inferred from it.
A stale claim is never automatically released. A restarted worker must reconcile
actual executor state before updating that same task; it must not rerun it.

After unconfirmed delivery, reusing the same `delegate` ID, request, worker,
checkout, and requester can retry delivery. Once delivery or a worker claim is
recorded, `delegate` only waits. Duplicate delivery cannot grant another claim.
Across a new Codex thread, use `task wait` or specify the original `--requester`
when deliberately retrying delivery. Do not create a new ID to bypass a claim.

Automatic idle wakeup is supported for saved conversations connected to the
configured local Codex CLI server. Codex owns that server: the installer runs
`codex app-server daemon start` (reusing a running server, else Codex's managed,
self-updating one) and supervises only `secondopinion-wakeup.service`. It retires
the `codex-local-app-server.service` unit earlier releases wrote, whose pinned
binary hid Codex updates and new models. No model, sandbox or approval override
is supplied on notification turns. A closed/unloaded conversation is not resumed
by the watcher; reopening it permits retained notifications to be delivered.
Other clients, disabled services and legacy skill installations use foreground
collection. If fallback times out while monitoring remains authorized, keep
collecting the same task IDs; do not yield and leave the user to discover reports.
Neither mode grants execution authority or bypasses a worker's approval blocker.

Registration is the only step that needs the Codex server socket, which Codex's
sandbox hides on purpose. If `delegate --async` reports that it cannot reach the
server from inside the sandbox, rerun that same command once with escalation, or
ask the user to run the `wake register` command it prints; never widen the sandbox.
A registration binds tasks to this conversation by requester and pins the
conversation's folder at that moment, so tasks in another checkout are fine. If
the conversation later moves to another folder, deliveries stop fail-closed until
the user confirms with `secondopinion wake rebind codex-CODEX_THREAD_ID
--confirm-folder-change`.

For diagnostics, use `secondopinion wake status codex-CODEX_THREAD_ID`. The outbox
distinguishes prepared, accepted, recorded and ambiguous delivery; recorded means
present in Codex history, not consumed. Service restarts reconcile positive history
evidence before sending. Ambiguous sends with no confirming evidence are retained
for inspection, not blindly retried; `wake reconcile` and explicit `wake retry`
are advanced recovery commands, not normal setup. Delivery/consumption cannot be
promised exactly once across arbitrary failures. Acknowledge after consumption.
If the user asks to stop automatic notifications, disable that route with
`secondopinion wake disable codex-CODEX_THREAD_ID`; it does not cancel a worker or
retract an in-flight notification. Duplicate claims prevent cooperative reruns,
but external effects cannot be committed atomically with mailbox state.

For hosts with their own supported delivery mechanism, `task create`,
`task instructions`, and `task relay-prompt` expose the same protocol without
launching a relay. Sending through such a mechanism still requires the user's
delegation authority; a mailbox record alone does not grant bench authority.
