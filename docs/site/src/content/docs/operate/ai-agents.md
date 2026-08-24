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
| `validate_schema` | structural problems in a declared schema, for one dialect, with no database |
| `render_schema` | the DDL a declared schema becomes, in dependency order |
| `schema_lineage` | which base columns feed each view column |
| `read_database` | the schema a live database currently holds |

Each takes its schema source or database URL as a tool argument, and reads it
with this process's own permissions. The server holds no credentials, stores
none, and opens no connection except the one a tool argument names.

Three of Ptah's own reading verbs are deliberately absent: `schema inspect`,
`schema diff`, and `migrations lint`. Each needs a scratch database that Ptah
resets destructively, and a destructive capability must not sit behind a
read-only name on a surface an agent drives. They return when a later phase can
supply that database out of band rather than from the caller.

## The artifact tools

| tool | answers |
| --- | --- |
| `describe_workspace` | which artifact directories exist, their digests, and what this session may do |
| `read_artifact` | one artifact directory, or one file inside it, with digests |
| `preview_patch` | what a proposed change would do: a diff per file and the resulting digest |
| `apply_patch` | apply a previewed patch, verify the result, undo it if the write broke something |

An agent starts with `describe_workspace`. Artifact paths are relative to
the directories it reports, and a patch has to carry the digest it reports.

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
# Reading tools only. No workspace, no artifact tools.
ptah mcp

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

`--dialect` names the target the verification gates validate and lint against,
and `--server-version` pins the release within that dialect so a rule gated on a
capability the family gained later answers for the server you run.

### Ask, allow, or refuse

Every operation names a capability, and every capability resolves to one of
three verdicts before the model is reached:

| verdict | what happens |
| --- | --- |
| `allow` | the operation proceeds |
| `ask` | the operation waits for you to approve this exact patch |
| `deny` | the operation is refused, and the refusal names what would grant it |

`ask` with nobody to ask is a refusal, never a promotion. A client that cannot
present a prompt gets a message naming `--allow-write` and `--auto-approve`
rather than a write nobody approved.

An approval prompt shows the capability, the artifact, the paths, the digest
before and after, and the patch's own content address. Approving covers that
patch and nothing else. You can also approve a capability for the rest of the
session; that grant lives in the process and dies with it.

`describe_workspace` reports the whole table, refusals included, so an
agent can tell you what it may do without trying.

### Narrow the policy from the repository

A project can carry `.ptah/agent-policy`, one rule per line:

```text
# Nobody's agent session may touch the test directory in this project.
artifact.write:tests deny
```

That file can only take permissions away. A rule in it that would grant more
than the flags did is ignored and reported in
`describe_workspace`'s `ignored_policy_rules`, because the file lives in
the repository the model is reading — treating it as a grant would let project
content decide what the next tool call may do.

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

The record carries no file content, no database URL, and no credential.

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
