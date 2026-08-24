# ADR 0004: Capability broker, artifact digests, and constrained mutation

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1487](https://github.com/stokaro/ptah/issues/1487), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Supersedes: nothing
- Superseded in part: section 2.1 by
  [ADR 0006](0006-one-authorized-agent-runtime.md), which extends the capability
  broker from the artifact operations to every operation the agent surface
  exposes. The digest binding, the preview-and-apply shape and the gate design
  stand.

## 1. Context

[ADR 0003](0003-agent-surface-trust-boundaries.md) ended with a trigger:

> A path or URL policy becomes required before any of these three: a mutation
> tool, a non-stdio transport, or a tool that returns row data.

This record answers the first of the three. It decides how an AI-driven caller
is allowed to change a file in a Ptah project, and what makes that different
from handing the same caller a file-writing tool.

Everything measured below was measured on the tree this record lands on.

### 1.1 The competitor evidence, because it shapes the burden of proof

Atlas ships no MCP server. The absence is deliberate and public: in May 2025 the
Atlas team wrote that they were working on one but that "it is proving difficult
to provide a controlled and safe result", and by April 2026 that had become a
general argument that instructions to an agent are "a suggestion, not a
guardrail" and that enforcement belongs in a deterministic tool. What Atlas
ships instead is `atlas copilot`, a Pro-tier, account-bound assistant, and a set
of skill files that teach a general-purpose agent to drive the CLI.

Two things follow. The first is that a safe write surface is a real product
difference rather than catch-up. The second is the harder one: Atlas's stated
objection is the standard this design has to meet. If any control here can be
satisfied by the model choosing to comply, the objection lands.

The rest of the field is not reassuring either. Measured across the
database-adjacent MCP servers in use today, read-only is the loudest
recommendation and a minority default -- the commercially-backed Postgres server
ships `AccessMode.UNRESTRICTED`, and the one community MCP server for Atlas
itself exposes `migrate apply` with no gate at all. Two filed defects say what
goes wrong: Supabase's `apply_migration` mints its own version server-side and
produced "37 orphan remote-only migrations before noticing", and a serverpod
issue from 2026-08-19 asks for a way to make the force flag unreachable because
"the AI agents often have no issue with doing force migrations".

### 1.2 What the protocol decided for us

Two constraints came from the specification rather than from preference, and
both were measured against the vendored SDK.

**A tool list may not change as a side effect.** Revision 2026-07-28 states the
tool set "MUST NOT vary per-connection or as a side effect of other requests on
the connection". The obvious broker design -- write tools appear once a
capability is granted -- is therefore unavailable.

**A server may not interrupt a call to ask.** Driving the surface with a
server-initiated elicitation returns:

```text
"elicitation/create" cannot be sent while serving a request on protocol
version 2026-07-28: return an InputRequests map instead (multi round-trip
requests, SEP-2322)
```

The same revision removed protocol sessions and prescribes the replacement:
"servers that need cross-call state use explicit, server-minted handles passed
as ordinary tool arguments".

### 1.3 What the tree already had

The filesystem work was largely done, and none of it was written for this
feature:

| package | what it already guarantees |
| --- | --- |
| `internal/pathguard` | a directory bound to an `os.Root` handle at open time; renaming the pathname afterwards does not retarget it |
| `internal/fsdurable` | conditional publication -- the caller's expectation about the destination is bound inside the rename, and a lost race yields `ErrDestinationChanged` with the rival's bytes intact |
| `internal/migrateops` | `Rehash`, the integrity refresh `ptah migrations edit` already runs |
| `internal/atlasmigrate` | the cross-process migration-directory lock every verb touching that directory takes |

Two gaps were found rather than assumed. `pathguard` folds no case -- its
containment check is a byte comparison -- so `Users.sql` and `users.sql` are two
plan entries and one file on APFS and NTFS. And `migratesum`'s own write path is
unconditional, so it must not be reached outside the directory lock.

## 2. Decision

### 2.1 Authority is resolved before the model, from operator-controlled inputs

Every operation names a capability. Capabilities resolve to `allow`, `ask` or
`deny` through four layers, resolved once at startup: the builtin defaults, the
operator's user configuration, the invocation's flags, and the project's own
file. Nothing in the resolution reads a tool argument.

`ask` with nobody to ask is a refusal. There is no promotion to `allow` in a
non-interactive run, which is the reading of `--non-interactive` that would be
unsafe in CI.

### 2.2 A project-carried policy may only narrow

`.ptah/agent-policy` lives in the repository the model is reading and proposing
patches to. Treating it as a grant would close the loop: content the attacker
controls would decide what the attacker's next tool call may do. A rule of its
that would widen is ignored, and reported rather than dropped, because a policy
file whose lines do nothing should say so to whoever wrote it.

### 2.3 The write surface is gated by refusal, not by absence

Under a workspace the four artifact tools are always listed, and a write the
policy refuses is a tool error naming the flag that would grant it. This is
§1.2's first constraint, turned into a property: the tool set depends on the
process's configuration, which is fixed before a client connects.

The coarser switch remains the operator's: without `--workspace` there are no
artifact tools at all, and without `--allow-write` there is no writing.

### 2.4 A patch is whole files, bound to two digests

A patch names an artifact class, the digest of that class's directory it was
composed against, and whole-file changes. Hunks were rejected: a hunk has to be
applied to something, and "something" may have changed since the model read it,
so the applier would be reconstructing content the reviewer never saw.

The directory digest is checked at plan time and again at apply time. The
file-level expectation is enforced by the operating system through
`fsdurable.Destination`, so the window between the check and the rename is not a
window.

### 2.5 Verification is Ptah's, and it decides on the difference

The applier writes, refreshes the integrity file, runs the gates, and keeps or
undoes. The gates are not offered to the model and their results are reported
whether or not anybody asked. They run twice -- before and after -- and the
apply fails on what the patch *introduced*, so an already-broken directory does
not make every patch unappliable and an incomplete repair is not refused for
being incomplete.

### 2.6 An approval is an input request, bound to one patch

The preview mints a single-use, expiring token bound to one patch. The apply
consumes it and cross-checks the patch id the caller echoes back. Where the
policy says to ask, the apply returns an input request and the client retries
with the answer; for a client on an older revision the SDK's middleware performs
the elicitation and reinvokes the handler.

The prompt is composed by Ptah from the capability, the paths and the digests.
The patch's own summary -- written by the party asking for permission -- is
recorded and never rendered as Ptah's account of what the patch does.

### 2.7 The tools drop the server's own prefix

The four tools ADR 0002 shipped were named `ptah_validate_schema` and so on.
They are renamed to `validate_schema`, `render_schema`, `schema_lineage` and
`read_database`, and the new ones follow: `describe_workspace`,
`read_artifact`, `preview_patch`, `apply_patch`.

Measured across the database-adjacent MCP servers in use today -- Supabase,
Neon, ClickHouse, MongoDB, dbt, the Postgres servers -- the convention is bare
`verb_noun` with no server prefix, because the client already namespaces: Claude
Code addresses a tool as `mcp__ptah__<name>` and displays it under the server's
own heading, so the prefix renders as `ptah - ptah_apply_patch`.

The cost is real and accepted: these names shipped in
[#1486](https://github.com/stokaro/ptah/issues/1486), and an allowlist written
against them stops matching. Ptah is pre-v1 and owes no compatibility with its
own previous spellings; the alternative is carrying the stutter to v1, where it
would become permanent. The contract version moves in the same change, which is
what makes the rename visible to a client that checks.

### 2.8 Nothing here reaches a database

`migration.apply` is denied at every layer and no operation implements it. That
is §1.1's two filed defects answered structurally: a flag the model cannot set
is safer than a flag the model is asked not to set.

## 3. Alternatives

**Host-owned edits, Ptah validates afterwards** (#1487's approach A). The
external agent writes files with its own tools and calls Ptah to check them.
Cheapest to build, and it is what a coding agent already does. Rejected as the
*only* path because it gives up the properties this record exists to provide:
no digest binding, so no conflict detection; no atomicity; and no guarantee that
the content reviewed is the content written. It remains available -- an agent
with its own file tools can still use them, and Ptah's reading tools still
validate the result -- which is #1487's approach C, and the documentation says
so rather than pretending Ptah constrains tools it does not own.

**A generic `write_file` confined to a directory.** Simpler, and the tool
description could say what belongs there. Rejected on §1.1: the confinement
would be real and everything else would be advice. It also loses the artifact
class, which is what lets a deletion carry a different capability from a write
and lets the migration integrity file be refreshed rather than authored.

**Capability grants as a separate tool that unlocks the write tools.** The
shape the epic's text suggests. Rejected on measurement: the specification
forbids the tool set from varying that way, and the same revision prescribes the
handle-passed-as-argument replacement adopted in §2.6.

**Session grants persisted across runs.** Rejected for now: a durable grant
needs revocation semantics, a storage location, and a way for a person to see
what they granted. A session grant that dies with the process needs none of
those and covers the case that matters, which is not asking about forty files
one at a time.

## 4. Consequences

- **A concurrent edit refuses an apply, and that is the feature.** Anything
  changing in the artifact directory between preview and apply -- including an
  unrelated file, including another agent -- makes the digest stale. The caller
  re-reads and composes again. A quieter design would be one that could not
  detect the case at all.

- **The undo is in-process.** A patch interrupted by a process kill leaves what
  it published, and the next operation refuses because the digest no longer
  matches. That is visible rather than silent, and it is weaker than the
  journaled batch `internal/atlasmigrate` already has for migration publication.
  Generalizing that journal is the follow-up this record names rather than
  implies.

- **One directory per artifact class.** Two would make `migrations/0001.sql` an
  ambiguous address unless the patch also named a root, and an unambiguous
  address is the property the design is built on. A project needing two needs
  two workspaces.

- **Gates that need a database are absent.** `migrations lint` and the test
  runners need a scratch database, and a gate that is skipped where none exists
  reads as a gate that passed. They join when a phase can supply that database
  out of band, which is the same rule ADR 0002 applied to the reading verbs.

- **A capability the policy denies is invisible until it is tried.**
  `describe_workspace` reports the whole table so the trying is not
  necessary, but an agent that does not call it first will meet the refusal
  instead. That is the cost of §2.3, accepted for the protocol constraint that
  produced it.
