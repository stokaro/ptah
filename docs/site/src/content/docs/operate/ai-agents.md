---
title: AI agents over MCP
description: Connect an AI client to Ptah with ptah mcp, and decide what it may read, propose, and write.
---

`ptah mcp` serves Ptah's operations to an AI client over the Model Context
Protocol, on stdin and stdout. A client starts the process and speaks the
protocol to it; it is not a command to run by hand.

Two groups of tools. The reading tools are always served. The artifact tools —
which propose and apply changes to migration, schema, and test files — are
served only when you start the server with `--workspace`, and they refuse to
write until you name the classes they may write to.

Nothing on this surface applies a migration to a database, at any setting.

Every tool asks Ptah's capability policy before it does anything. A session
always has a policy; a workspace only adds the artifact half. What that means in
practice is that a server started with no flags can reach nothing: you name the
directories a schema may be read from, and you name the database that may be
inspected. Both refuse until you do.

The server offers tools and nothing else — no MCP resources and no prompts. A
client looking for them will find none.

## Connect a client

Claude Code and Cursor read `mcpServers`:

```json
{
  "mcpServers": {
    "ptah": {
      "command": "ptah",
      "args": ["mcp"]
    }
  }
}
```

VS Code reads the same object under `servers`:

```json
{
  "servers": {
    "ptah": {
      "command": "ptah",
      "args": ["mcp"]
    }
  }
}
```

Claude Code can add it from the command line. The `--` separates Claude Code's
own options from the command it runs:

```bash
claude mcp add ptah -- ptah mcp
```

That configuration serves the reading tools. To reach the artifact tools, give
the server a workspace and a target dialect:

```json
{
  "mcpServers": {
    "ptah": {
      "command": "ptah",
      "args": [
        "mcp",
        "--workspace", "/path/to/project",
        "--migrations-dir", "./migrations",
        "--dialect", "postgres",
        "--allow-write", "migrations"
      ]
    }
  }
}
```

## The reading tools

| tool | answers |
| --- | --- |
| `describe_session` | what this session may do, and what it can reach |
| `validate_schema` | structural problems in a declared schema, for one dialect, with no database |
| `render_schema` | the DDL a declared schema becomes, in dependency order |
| `schema_lineage` | which base columns feed each view column |
| `read_database` | the schema a configured database currently holds |

An agent starts with `describe_session`. It reports two different things, and
keeping them apart is the point: **what the policy permits**, as the whole
capability table with refusals included, and **what this process can reach** —
the schema source directories, the configured databases by name and class, and
the workspace when there is one. A row saying `database.inspect:dev ask` beside
an empty database list is not a contradiction. One is authority; the other is
whether anything is there.

### Where a schema may be read from

A schema source is a path the model chooses, so the operator chooses the
directories:

```bash
ptah mcp --schema-source-root ./schema --schema-source-root ./models
```

`--workspace` is a root when you pass one. Without any root, the schema tools
refuse: a server told no directory has not been told what an agent may read.

A source that would be fetched rather than opened — `oci://`, `http://`,
anything with a scheme — is refused. Fetching is a network operation, and no
capability on this surface grants one.

Three of Ptah's own reading verbs are deliberately absent: `schema inspect`,
`schema diff`, and `migrations lint`. Each needs a scratch database that Ptah
resets destructively, and a destructive capability must not sit behind a
read-only name on a surface an agent drives. They return when a later phase can
supply that database out of band rather than from the caller.

## The artifact tools

| tool | answers |
| --- | --- |
| `read_artifact` | one artifact directory, or one file inside it, with digests |
| `preview_patch` | what a proposed change would do: a diff per file and the resulting digest |
| `apply_patch` | apply a previewed patch, verify the result, undo it if the write broke something |

Artifact paths are relative to the directories `describe_session` reports.

A patch has to carry the artifact digest by the time it is applied.
`preview_patch` accepts a patch without one and reports the digest to carry, but
`apply_patch` refuses a patch that has none — and it spends the preview token
first, so the next attempt starts from a new preview. Passing
`expected_digest` to `preview_patch` is the way to find that out at preview
time rather than at apply time.

A change lands in three steps:

```text
preview_patch    validate, diff, and mint a preview token; write nothing
       ↓
you approve           where policy says to ask
       ↓
apply_patch      apply, recompute integrity, run the gates, keep or undo
```

The preview token is single-use, expires after fifteen minutes, and belongs to
exactly one patch. An apply that names a different patch id is refused, and so
is an apply of a token that was already spent.

## Decide what an agent may do

Three flags, in increasing order of what they permit:

```bash
# Reading tools only, and nothing configured for them to read.
ptah mcp

# Reading a declared schema.
ptah mcp --schema-source-root ./schema

# Artifact tools, read-only: an agent can describe, read, and preview.
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres

# Writing to the migration directory, asking you before each patch.
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres \
  --allow-write migrations

# Writing without asking, for a client that cannot show a prompt.
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres \
  --allow-write migrations --auto-approve
```

`--allow-write` takes any of `migrations`, `schema`, and `tests`, and each needs
its own directory flag: `--migrations-dir`, `--schema-dir`, `--tests-dir`. A
class you do not name is a class no tool can write to, whatever the model asks
for.

### Let an agent read a live database

The database is yours to configure, not the agent's to name:

```bash
ptah mcp \
  --database-url "postgres://reader@db.internal:5432/app?sslmode=require" \
  --database-class dev \
  --allow-database-inspect ask
```

The model never sees that URL. `describe_session` reports the target's name and
class, `read_database` names the target, and Ptah opens the connection.

Configuring a database and permitting an agent to read it are separate
decisions. Without `--allow-database-inspect` the builtin table decides:

| class | what happens |
| --- | --- |
| `ephemeral` | allowed; a throwaway database Ptah itself created |
| `dev` | asked about, per read |
| `target` | asked about, per read |
| `production` | denied, and no flag on this surface widens it |
| `unclassified` | denied; this is what a database with no `--database-class` is |

The class comes from you and from nowhere else. A database named `production`
on a host named `prod` is `unclassified` until you classify it, because a label
somebody chose is not a fact about trust.

`--auto-approve` is about patches. It grants nothing here.

An approval is bound to the exact database, not to its class: approving one dev
database for the rest of a session does not carry to another, and repointing the
configuration at a different URL invalidates the approval.

`--dialect` names the target the verification gates validate and lint against,
and `--server-version` pins the release within that dialect so a rule gated on a
capability the family gained later answers for the server you run.

### Ask, allow, or refuse

Every operation names a capability, and every capability resolves to one of
three verdicts before the model is reached:

| verdict | what happens |
| --- | --- |
| `allow` | the operation proceeds |
| `ask` | the call returns an input request, and the client re-issues it carrying your answer |
| `deny` | the operation is refused, and the refusal names what would grant it |

`ask` does not block a call while somebody thinks. The protocol revision this
server speaks does not let a server interrupt a tool call to ask a question, so
the call ends with a request for input and the client calls again with the
answer.

`ask` with nobody to ask is a refusal, never a promotion. A client that cannot
present a prompt is told to grant the capability outright, because naming a
class without `--auto-approve` produces exactly the state that could not be
resolved.

An approval prompt shows the capability, the artifact, the paths, the digest
before and after, and the patch's own content address. Approving covers that
patch and nothing else. You can also approve a capability for the rest of the
session; that grant lives in the process and dies with it.

`describe_session` reports the whole table, refusals included, so an agent can
tell you what it may do without trying — and every row it reports is a row the
matching operation obeys.

### Narrow the policy from the repository

A project can carry `.ptah/agent-policy`, one rule per line:

```text
# Nobody's agent session may touch the test directory in this project.
artifact.write:tests deny
```

That file can only take permissions away. A rule in it that would grant more
than the flags did is ignored, and `describe_session` reports it under
`ignored_policy_rules`, because the file lives in the repository the model is
reading — treating it as a grant would let project content decide what the next
tool call may do. Reporting it is what makes the attempt visible rather than
merely ineffective.

## What Ptah checks, and what it does not

Ptah runs its own verification after every write, on the bytes that reached the
disk, whether or not the model asked:

| artifact | gates |
| --- | --- |
| `migrations` | the directory matches its integrity file; every SQL file parses and lints |
| `schema` | the schema loads, validates, and renders for the target dialect |
| `tests` | every declarative test file parses |

The gates run before and after the write, and the apply decides on the
difference. A directory that was already failing does not make every patch
unappliable, and a patch that introduces an error is undone: the files go back
to what they were, the integrity file is recomputed, and the diagnostics come
back to the agent to act on.

A patch to the migration directory also refreshes `ptah.sum` itself. A patch may
not write that file: a caller that could supply both a migration and the
checksum over it would produce a directory that verifies against itself.

Ptah guarantees the boundaries of Ptah-provided operations, and only those. An
MCP client may give its model shell, filesystem, and network tools of its own,
and Ptah cannot constrain those.

## What a session records

With a workspace, the server appends one JSON line per decision to
`<workspace>/.ptah/agent-audit.jsonl`, or to the path `--audit-log` names. Each
record carries the capability requested, the policy layer that answered, whether
a human approved, the paths, the digests before and after, and which gates ran.

Refusals are recorded alongside permissions. A log written only when something
succeeded would show a clean session for exactly the run worth reading.

Ptah's own fields carry no file content, no database URL and no credential: a
database is recorded by the identity and class of the configured target.

One field is the model's words. `caller_summary` is the summary the model wrote
for its own patch, kept verbatim so the record says what was claimed. A model
can put anything there, including file content, and it is excluded from the
patch identity for exactly that reason.

## Safety boundary

The model is not trusted, and neither is the repository it reads. A schema
comment, a column name, or a README is a place somebody can write a sentence
addressed to the model, and that text arrives as tool output in the same context
the model chooses its next arguments from.

What that means in practice:

- Paths come from the model, and containment does not. Every artifact directory
  is bound to an operating-system handle when the server starts, so a path that
  leaves it is refused rather than resolved, and renaming the directory
  afterwards does not retarget anything.
- A patch cannot name an absolute path, a `..` segment, a backslash, a drive
  letter, or a Windows device name — on any platform, because a rule about what
  a file may be called must not depend on the machine reading it.
- Two paths one filesystem cannot tell apart, such as `Users.sql` and
  `users.sql`, are refused while the patch is being planned rather than
  discovered half-applied.
- A patch is applied only if the directory still has the digest the preview
  reported. If anything changed in between, the apply refuses and the agent has
  to read and propose again.
- Artifact content comes back labeled as data, with a notice saying so. That is
  a cheap layer, not a control: the controls are the capability broker, the path
  containment, and the gates.

## Related

- [Native commands](../../reference/native-commands/) — the whole `ptah` command tree.
- [Migration integrity and safety](../../versioned/integrity-and-safety/) — what
  `ptah.sum` protects and how.
