---
title: Rollout definition and state
description: Every key of a ptah migrations rollout definition, the validation rules, the order targets run in, and the stage, target and run statuses the state file records.
type: reference
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "What can a rollout definition say, and what does each status in a rollout state file mean?"
goal: "Write a valid rollout definition and read every field of a rollout state file."
sourceOfTruth:
  - "internal/migrationrollout"
  - "internal/cli/migraterollout"
generated: false
overlaps:
  - /operate/staged-rollout/
disposition: keep
---

This page lists what `ptah migrations rollout` reads from a definition file and
writes to a state file. [Roll a release out in stages](../../operate/staged-rollout/)
is the workflow.

## Definition file

A definition is one YAML document with two top-level keys, `targets` and
`stages`. Decoding is strict: an unknown key, a repeated key, a value of the
wrong type, an empty file and a second document are refused.

### Target keys

| Key | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Identifies the target in every report. Letters, digits, `.`, `_` and `-`, up to 64 characters, starting with a letter or digit. |
| `url` | one of `url` and `url_env` | The database URL, in the form `ptah migrations up --db-url` takes. |
| `url_env` | one of `url` and `url_env` | The environment variable holding the database URL. It must be set when the definition is read. |
| `config` | no | A project config file for this target's run, passed as `--config`. Relative to the definition file. |
| `expect_sequence` | no | This target's approved migration sequence, passed as `--expect-sequence`. Relative to the definition file. |
| `allow_destructive` | no | `true` passes `--allow-destructive` to this target's run. Defaults to `false`. |

A target's URL never reaches a report. Reports show it with its password and
secret parameters replaced, as `ptah migrations up` displays one, and take the
same secrets out of every line a target's run writes.

### Stage keys

| Key | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Identifies the stage. The same characters as a target name. |
| `targets` | yes | The members, by target name, in the order they start. |
| `depends_on` | no | Stages that must succeed before this one starts. |
| `canary` | no | `true` marks the canary stage. |
| `concurrency` | no | How many members run at once. Defaults to `1`. |
| `on_failure` | no | `stop`, the default, or `continue`. |
| `require` | no | How many members must succeed: `all`, the default, a count such as `2`, or a percentage such as `50%`, rounded up. |

## Validation rules

`ptah migrations rollout plan` and every `run` and `resume` check the whole
definition before anything runs, and report every broken rule in one error:

- target and stage names are words as described above, each declared once;
- a target sets exactly one of `url` and `url_env`, and the variable is set;
- `config` and `expect_sequence` name files that exist;
- two targets do not address one database endpoint with the same connection
  options and the same `config`; credentials do not count;
- a stage names only targets in the inventory, and names at least one;
- every target belongs to exactly one stage;
- `depends_on` names existing stages, never the stage itself, and forms no
  cycle; a cycle is reported with its path, such as `x -> y -> x`;
- `concurrency` is at least one, `on_failure` is `stop` or `continue`, and a
  count in `require` is no more than the stage's members;
- at most one stage is the canary, the canary depends on no stage, and every
  other stage depends on the canary directly or through another stage.

## Order

Stages run one at a time. A stage runs after every stage it depends on; between
stages ready at the same time, the one declared first runs first. Within a
stage, members start in the order `targets` lists them, at most `concurrency`
at once, each as soon as a slot frees.

A stage starts only when every stage in its `depends_on` succeeded. Otherwise
it is `blocked`, and so is every member that has not run. A stage outside the
dependency chain of a failed stage still runs.

## Target statuses

| Status | Meaning |
| --- | --- |
| `not-started` | The rollout has not started this target, or a resume will run it again. |
| `running` | Its run is in progress, or was when the rollout stopped. A resume reads its revision table. |
| `succeeded` | Its run applied every migration it selected, or a resume found every migration of the release applied. |
| `no-op` | Its run found nothing pending. |
| `failed` | Its run stopped before it sent a statement, or at a migration that committed nothing. |
| `partial` | Its failed migration committed some statements and not the rest. |
| `unknown` | Nothing says what happened: no run document, an unreadable revision table, or a dirty row without counts. |
| `skipped` | Not started because its stage stops after a failure. |
| `blocked` | Not started because a stage its stage depends on did not succeed. |

`succeeded` and `no-op` count toward a stage's `require`. `failed`, `partial`
and `unknown` are failures, and a stage with `on_failure: stop` starts nothing
further after one. The status comes from the run document of
`ptah migrations up --json`, whose outcome reads the revision table after the
run; the process exit status is not consulted.

## Stage statuses

| Status | Meaning |
| --- | --- |
| `not-started` | The rollout has not reached the stage. |
| `running` | Members are running, or an interrupt left members not started. |
| `succeeded` | Every member has an outcome, and the successes meet `require`. |
| `failed` | Every member has an outcome, and the successes do not meet `require`. |
| `blocked` | A stage it depends on did not succeed. `blocked_by` names it. |

## Run statuses

| Status | Meaning |
| --- | --- |
| `running` | The run is in progress, or its process ended without recording an outcome. |
| `succeeded` | Every stage succeeded. The command exits `0`. |
| `failed` | Every stage the run could reach is decided, and one failed or was blocked. The command exits `1`. |
| `interrupted` | A signal, or a state file that could not be written, stopped the run before every stage was decided. |

## State file

The state file is one JSON document, written when `run` starts and replaced
after every change, so a process that dies leaves the last thing it recorded.
`--json` prints the same document on standard output. Its top-level fields:

| Field | Meaning |
| --- | --- |
| `contract_version` | `1`. A reader refuses a version it does not know. |
| `status` | The run status. |
| `definition` | The definition path the last invocation read. |
| `plan` | The plan digest: a SHA-256 over every stage and target setting, with each URL in its display form. |
| `release` | `kind` (`directory` or `oci`), `source` as named, `digest`, and for an artifact the digest-pinned `reference`. |
| `ptah_version` | The version of the `ptah` binary of the last invocation. Every target run of an invocation uses that binary. |
| `invocations` | The run and its resumptions. |
| `stages` | Every stage in execution order, with its settings, `status`, `succeeded`, `blocked_by` and `targets`. |

Each target carries its settings, `status`, `attempts`, `started_at` and
`finished_at`, the version keys its last run `planned` and `applied`, the
`version` its revision table records, and the run's `error` and `failure`
objects as `ptah migrations up --json` reports them. `note` says why a status
is what it is when the run's own outcome does not: a skip, a block, or what a
resume read back.

## Release identity

| Release | Identity |
| --- | --- |
| Local directory | The content digest of every file the run captured, by path and bytes. It is the digest Ptah Operator binds a plan to, so it allows at most 1000 files. |
| `oci://` reference | The manifest digest the reference named when the run started. Each target pulls that digest, not the tag. |

`resume` binds again and refuses a release whose kind or digest differs from the
recorded one. Naming the same artifact by its digest where the run named a tag
is the same release.

## Environment

Each target's run inherits the rollout's environment and working directory. The
rollout refuses to start while any of these is set, whatever its value:
`PTAH_ALLOW_DIRTY`, `PTAH_SKIP_CHECKS`, `PTAH_ALLOW_DESTRUCTIVE`,
`PTAH_EXPECT_SEQUENCE`, `PTAH_DRY_RUN`, `PTAH_LIMIT` and `PTAH_TO_VERSION`.
Each would waive a target's guard or change what its run does for every target
at once.

The rollout passes the database URL in the run's environment rather than on its
command line, and passes `--migrations-dir`, `--json`, `--log-format=text` and
an empty `--metrics-addr` as flags, which outrank the corresponding variables.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Every stage succeeded. |
| `1` | The rollout ran, and a stage failed or was blocked. The state file says which target did what. |
| `2` | Refused before any target ran: an invalid definition, a refused variable, a state file that exists for `run` or is missing for `resume`, a held lock, or a plan or release that does not match the state. |
| `128+N` | Interrupted by a signal. The state file says `interrupted`. |
