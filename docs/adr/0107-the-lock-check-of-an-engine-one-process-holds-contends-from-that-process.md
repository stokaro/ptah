# ADR 0107: The lock check of an engine one process holds contends from that process

Status: Proposed

Contract: 25.31 (no change of its own)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Amends: [ADR 0092](0092-a-provider-is-checked-by-a-suite-that-acts-as-a-strict-host.md)

## Context

The conformance suite's `live.locks` check takes one lock from two sessions:
the first of the provider process the suite described, the second of a second
process it starts. It requires the second session to find the held lock busy,
a lock ID to be used once, releasing an unused ID to fence it, and closing a
session to release its locks (ADR 0092).

The DuckDB provider links its engine into the provider process. DuckDB lets
one process open a database file for writing at a time, which is the
exclusion a migration lock exists for between processes. The second process
of the check cannot open the database, so its session never asks for the lock
and the check fails, although the provider's lock service keeps out the other
sessions of the process that holds the database (the DuckDB provider's gap
list, item 8). No configuration let an author run the check for such an
engine.

## Decision

`conformance.Live` gains `InProcessLocks`. When it is set, the second session
of the lock check is a session of the same provider process, and the steps
are the ones the check runs across processes: the contender finds the held
lock busy, lock IDs are single-use, an unused ID is fenced, and closing the
contender's session releases its lock.

The mode is valid only for an engine that keeps other processes out of the
database. Where each connection of the suite is a process of its own, an
executable target or a wire target, the check first opens a session from a
second process while the first session is open, and requires that open to
fail. A provider whose database a second process opens fails the check with
a text that names the mode, so the mode cannot hide a lock that another
process could contend for. An in-process target serves every connection from
one server and has no second process, so this part does not apply to it.

The command reads the setting from its fixture file as
`live.in_process_locks`, beside `live.lock_name`; it takes no new flag. The
default is the check across processes, unchanged.

## Alternatives considered

**Report `live.locks` as not applicable for such an engine.** The check would
then prove nothing about the lock service, which still has to keep out the
other sessions of the process that holds the database.

**Contend from a second session of the same process, with no check of a
second process.** It is simpler. An author could then set the mode for an
engine that does admit other processes, and a lock that fails across
processes would pass.

**Declare the exclusion in the provider's description**, as an engine
property the host and the suite read. It needs a contract addition for what
only the suite reads. A suite configuration is the author's claim, and the
check of a second process tests it.

## Consequences

- An engine linked into its provider, such as DuckDB, can run every live
  check of the suite.
- An author who sets the mode for an engine that admits a second process
  gets a failure that says so, on an executable or a wire target.
- The suite's command and its report gain no field; the fixture file gains one
  key.

## Acceptance evidence

- `TestCheck_InProcessLocks_HappyPath` in `provider-sdk/conformance` passes
  the memory provider in process and behind a wire service whose engine one
  process holds at a time. Ignoring the setting fails the second row.
- `TestCheck_InProcessLocks_FailurePath` fails `live.locks` for a lock two
  sessions of one process hold, and for a wire target whose database a second
  process opens. Without the check of a second process the second row passes.
- `TestCheck_ExclusiveEngineAcrossProcesses_FailurePath` shows the check
  across processes failing for the engine one process holds.
- `TestConformanceCommandInProcessLocks_FailurePath` in
  `provider-sdk/integration` runs the command with `live.in_process_locks`
  against the fixture provider, whose database file a second process opens,
  and the command exits 1 on `live.locks`. Without the fixture key the command
  exits 0.
