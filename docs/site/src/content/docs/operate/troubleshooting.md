---
title: Troubleshooting
description: Fix common Ptah command, database, Graphviz, hash, config, and conformance problems.
type: troubleshooting
audience:
  - "platform-engineer"
readerQuestion: "How do I diagnose a Ptah failure from its stable symptom or error text?"
goal: "Diagnose a Ptah failure from its stable symptom or error text."
sourceOfTruth:
  - "cmd"
  - "dbschema"
  - "migration"
generated: false
owns:
  - cli-ptah-db-drop-all
overlaps: []
disposition: keep
sourceMode: command-specific
---

Start with the exact error line, the command that produced it, and the inputs
that select the database, project environment, or migration directory. The
entries below use stable Ptah-authored text; operating-system details after that
text may differ.

Inference migration failures have their own
[symptom index](../../inference/troubleshooting/). MCP and patch-workflow
failures are under [AI agent troubleshooting](../ai-agent-troubleshooting/).

## SVG output says Graphviz is required

**Symptom**

```text
Graphviz dot is required for --format svg; install graphviz or use --format dot
```

**Likely cause**

`ptah viz --format svg` uses the Graphviz `dot` executable, and the process
cannot find it.

**Diagnose**

```bash
dot -V
```

**Resolve**

Install Graphviz, or choose output that does not invoke it:

```bash
ptah viz --root-dir ./models --format mermaid
ptah viz --root-dir ./models --format dot
```

**Verify**

Re-run `ptah viz`. The command exits `0` and writes the selected format instead
of the error above.

## Database connection fails

**Symptom**

The command exits before reading or changing the schema and reports a driver,
authentication, address, or database-selection error.

**Likely cause**

The database URL is malformed, points at an unavailable server, or lost to a
higher-precedence flag or project environment.

**Diagnose**

Use the smallest read-only command with the same URL:

```bash
ptah db read --db-url "$DATABASE_URL"
```

If the failing command also reads project config, name the environment and the
migration directory explicitly while diagnosing it:

```bash
ptah migrations status --env dev --migrations-dir ./migrations
```

Explicit flags outrank environment variables and config files.

**Resolve**

Correct the URL or the configuration source that should own it. A SQLite file
URL needs an absolute path:

```bash
ptah db read --db-url sqlite:////tmp/app.db
```

For a server database, include the database name and required credentials, and
confirm that the server is listening before retrying.

**Verify**

`ptah db read` exits `0` and prints the live schema. Re-run the original command
with the same resolved URL.

## `invalid boolean value` names a `PTAH_*` variable

**Symptom**

```text
invalid boolean value "..." for PTAH_...
```

**Likely cause**

The named value is not accepted by its CLI flag. Whitespace and quotes are part
of the value; Ptah does not trim them.

**Diagnose**

Print delimiters around the value so whitespace remains visible:

```bash
printf '<%s>\n' "$PTAH_DRY_RUN"
```

**Resolve**

Set an accepted Go boolean spelling, such as `true`, `false`, `1`, or `0`, or
remove the variable:

```bash
export PTAH_DRY_RUN=true
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations
```

An explicit CLI flag wins without reading its environment twin. Ptah validates
a malformed non-empty value before command hooks or database work.

**Verify**

Re-run the command. It must pass environment validation before it reaches any
filesystem or database operation.

## Hash validation fails

**Symptom**

```text
migration directory does not match ptah.sum
```

**Likely cause**

A migration changed, appeared, or disappeared after `ptah.sum` was written.

**Diagnose**

Review the version-control diff before changing the checksum. Then run:

```bash
ptah migrations validate --dir ./migrations
```

**Resolve**

Restore an accidental edit. If the migration change is intentional and
reviewed, regenerate the checksum:

```bash
ptah migrations hash --dir ./migrations
```

:::danger
Do not regenerate `ptah.sum` to hide an unexplained edit. The checksum proves
which migration bytes were reviewed; replacing it accepts the new bytes.
:::

**Verify**

Validate the directory again:

```bash
ptah migrations validate --dir ./migrations
```

The command exits `0` and prints that the directory matches `ptah.sum`.

## A development database has to be emptied

**Symptom**

A throwaway or development database is in a state no migration reaches: a
half-applied change, an object created by hand, or a schema that no longer
matches any revision.

**What to run**

`ptah db drop-all` drops every schema object the database holds. Its scope is
the database and not a schema Ptah declares, so objects Ptah never created go
too. Run it against a database you are willing to lose.

```bash
ptah db drop-all --db-url "$DATABASE_URL" --dry-run
```

`--dry-run` connects, reports how many objects would be dropped, and changes
nothing. Without it the command asks for two confirmations; `--auto-approve`
skips both and is the one flag here with no `PTAH_*` variable, so no
environment can turn the prompts off by accident.

**What it leaves behind**

SQLite keeps Ptah's revision table. Everything else in the database goes, so
`ptah migrations status` afterwards still reports the old version and
`ptah migrations up` finds nothing pending against an emptied database. Put the
recorded history back in step with
[`ptah migrations baseline`](../../versioned/maintain-history/).

**Not for a shared database**

There is no undo. Use it on a database whose contents you can recreate, and
reach for [rollback](../../versioned/rollback/) or a forward repair on anything
else.

## A dialect capability is unsupported

**Symptom**

Rendering or applying a schema reports that the selected dialect does not
support an authored construct.

**Likely cause**

Ptah recognizes the construct but refuses to weaken or silently discard it for
that dialect.

**Diagnose**

Compare the same schema source across the relevant dialects:

```bash
ptah schema render --root-dir ./models --dialect sqlite
ptah schema render --root-dir ./models --dialect postgres
```

Use [Capabilities](../../reference/capabilities/) for the exact gate and
`ptah db capabilities` for a live target.

**Resolve**

Change the schema to a construct the target can represent, or select a database
that supports the required semantics. Do not replace the construct with a
weaker one unless that is the intended schema.

**Verify**

Render the desired schema for the target dialect, then compare it with the live
database before applying it.

## `atlas.hcl` fails to load

**Symptom**

```text
unsupported atlas.hcl construct ...
```

**Likely cause**

The project uses an `atlas.hcl` construct outside Ptah's documented subset.

**Diagnose**

Match the reported block or attribute against the
[Atlas project config reference](../../atlas/project-config/). Confirm that the
command selected the intended `--env`.

**Resolve**

Replace the construct with the documented Ptah equivalent, simplify the project
environment, or keep that workflow on Atlas until Ptah implements the required
semantics.

**Verify**

Re-run the same read-only or planning command with the same `--env`. The config
must load without the unsupported-construct error before any apply.

## `atlas.hcl` says a construct has no effect

**Symptom**

```text
warning: atlas.hcl attribute "project" at atlas.hcl:2 is ignored for Atlas compatibility and has no effect
```

**Likely cause**

Ptah accepted a name that Atlas CE accepts without acting on, so the command can
succeed while that setting changes nothing.

With several such names, the warning gives their number instead, as
`warning: 3 atlas.hcl settings are ignored for Atlas compatibility and have no
effect; PTAH_LOG_LEVEL=debug lists them`.

**Diagnose**

Read the named setting in the
[Atlas project config reference](../../atlas/project-config/) and confirm what
the invocation expected it to change. With several, re-run with
`PTAH_LOG_LEVEL=debug` to list them with their locations.

**Resolve**

Correct a typo, replace the construct with a supported setting, or remove it
when Atlas compatibility requires the no-op.

**Verify**

Re-run the command and confirm that the warning is gone and the expected input
is visible in the planned or inspected result.

## Warnings about many objects

**Symptom**

```text
warning: 47 names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read that way; PTAH_LOG_LEVEL=debug lists them
```

**Likely cause**

A warning that can concern many objects is reported once per run, with how
many objects it concerns, after the command's own output. A run that reads the
same source twice counts each object once, and a `PRIMARY KEY` or `UNIQUE`
constraint counts once, although the server stores its index under the same
name. A warning that concerns one object is printed as that object's own line.
`ptah` and `ptah-compat` follow the same rule on every command that reads such
an object, whatever the command prints on stdout. It covers names a
PostgreSQL-family server cuts to 63 bytes, constraints a SQL file drops and
adds again with another definition, `atlas.hcl` settings Atlas compatibility
ignores, and schema-file constructs that are accepted and dropped. On
`ptah-compat` it also covers SQL files in an Atlas migration directory that
`atlas.sum` does not cover.

A refusal is never folded into such a line. Two different names that the server
would store as one still fail the command and name both, also under
`IF NOT EXISTS` or `OR REPLACE`, where the server would keep one of the two
without an error.

**Diagnose**

Set `PTAH_LOG_LEVEL=debug` to list the objects at debug level on stderr. Add
`PTAH_LOG_FORMAT=json` to read them as JSON records, each with a `category`
field and the object's own fields. With JSON, the warning itself is a record
too, at warning level, with `category` and `count`, so the stream holds no
text line for it. The record is written at any log level, as the text line is.

`migrations status`, `migrations up` and `migrations down` take `--log-level`
and `--log-format` and log through a logger of their own. The objects and the
warning join that log: at the level the flag sets, with its `correlation_id`
and `command` fields, and under `--log-format json` on the stream the command
writes its log to. Their warning ends with
`PTAH_LOG_LEVEL=debug or --log-level debug lists them`.

The strict compatibility profile, `PTAH_ATLAS_STRICT_COMPAT=1`, refuses both
variables, as the community binary has neither, and its warning says where the
list is:

```text
warning: 47 names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read that way; PTAH_LOG_LEVEL=debug lists them when PTAH_ATLAS_STRICT_COMPAT is off
```

Run the command again without the profile and with `PTAH_LOG_LEVEL=debug`. The
default profile reads the same objects and lists them.

**Resolve**

Nothing is wrong when the objects are expected, such as the long names an ORM
writes. Otherwise, act on the listed objects as each warning's own section
describes.

**Verify**

Re-run the command. The count changes with the objects.

## An Atlas-compatible command says runtime behavior is not implemented

**Symptom**


```text
Runtime behavior is not implemented yet.
```

**Likely cause**

The Atlas-compatible path is registered for surface measurement, but the
command body is still an explicit boundary stub. Registration alone is not
runtime parity.

**Diagnose**

Check the command in [Atlas-compatible commands](../../reference/atlas-commands/)
and the current [conformance evidence](../../atlas/conformance/).

**Resolve**

Use the native Ptah command when the reference names an equivalent. Otherwise,
keep the workflow on Atlas or contribute the missing implementation; do not
treat the registered stub as a successful operation.

**Verify**

Run the documented replacement against a disposable target and verify its
result. A help page or registered command path is not that verification.

## Conformance reports look red

**Symptom**

The full-conformance report contains non-OK results while a regression-budget
gate is green.

**Likely cause**

The reports answer different questions. A regression budget detects new gaps;
full conformance measures all gaps in the current corpus.

**Diagnose**

Read the report kind and its exact measured commit. Then open the affected row
in [Conformance](../../atlas/conformance/) and the
[feature matrix](../../atlas/feature-matrix/).

**Resolve**

No documentation-only action turns an expected conformance gap green. Fix the
measured behavior or update evidence only after the corresponding probe changes.

**Verify**

A green regression gate means no new measured gaps crossed its budget. It does
not mean complete Atlas CE parity. A green full-conformance report would still
prove only the recorded corpus and commit.
