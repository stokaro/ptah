---
title: Roll a release out in stages
description: Apply one versioned migration release to many databases with ptah migrations rollout, a canary first and each stage gated on the one before, then read every target's outcome and resume after a failure.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I apply one migration release to many databases, a canary first, with a limit on concurrent work and a way to resume?"
goal: "Define the targets and stages of a rollout, run it against one bound release, read every target's outcome, and resume it after a failure or an interruption."
sourceOfTruth:
  - "internal/migrationrollout"
  - "internal/cli/migraterollout"
  - "integration/migrations_rollout_unix_e2e_test.go"
  - "examples/staged-rollout"
generated: false
searchAliases:
  - "canary deployment"
  - "database per tenant"
  - "fleet rollout"
overlaps:
  - /versioned/apply/
  - /reference/rollout-definition/
disposition: keep
owns:
  - cli-ptah-migrations-rollout-plan
  - cli-ptah-migrations-rollout-run
  - cli-ptah-migrations-rollout-resume
---

`ptah migrations rollout` applies one versioned migration release to many
databases in stages: a canary first, then the stages that depend on it, each
with a limit on how many targets run at once, a response to a failure, and a
number of targets that must succeed before the next stage may start. Every
outcome is written to a state file, and a run that failed or was interrupted
continues from it.

Each target runs `ptah migrations up` in a process of its own. The rollout
decides which target runs when; it does not change what happens inside a
target's run. The integrity check, the migration lock, the dirty-revision
guard, pre-migration checks and the approved sequence apply to each target as
they do when you run `ptah migrations up` against it by hand.

| Shape | Command | Targets | Order and failure |
| --- | --- | --- | --- |
| Single target | `ptah migrations up` | one database | one run, which stops at the first failed migration |
| Sequential multi-target | `ptah-compat migrate apply --env <name>` | the instances of one `atlas.hcl` env | one target at a time, stopping at the first failure |
| Staged rollout | `ptah migrations rollout run` | the inventory of a rollout definition | stages with dependencies, concurrency, failure policy and success requirement; resumable |

## Prerequisites

- A migration release: a migration directory, ideally sealed with
  `ptah migrations hash`, or an OCI artifact published with
  `ptah migrations push`.
- Every target database reachable from the machine that runs the rollout, with
  the same access `ptah migrations up` needs.
- The `ptah` binary. Nothing else runs the rollout: no service, no account and
  no external orchestrator. A CI job or a scheduled task that runs the commands
  below is enough.

[The staged rollout example](https://github.com/stokaro/ptah/tree/master/examples/staged-rollout)
runs the same workflow against five SQLite files.

## Write the definition

A definition lists the targets, then the stages that group them:

```yaml
targets:
  - name: canary
    url_env: CANARY_DATABASE_URL
  - name: tenant-a
    url_env: TENANT_A_DATABASE_URL
  - name: tenant-b
    url_env: TENANT_B_DATABASE_URL
    expect_sequence: approvals/tenant-b.json
  - name: tenant-c
    url_env: TENANT_C_DATABASE_URL
  - name: tenant-d
    url_env: TENANT_D_DATABASE_URL
stages:
  - name: canary
    canary: true
    targets: [canary]
  - name: wave-1
    depends_on: [canary]
    targets: [tenant-a, tenant-b, tenant-c]
    concurrency: 2
    on_failure: continue
    require: 2
  - name: wave-2
    depends_on: [wave-1]
    targets: [tenant-d]
```

`url_env` names the variable that holds a target's URL, so the file carries no
credentials and can be reviewed and committed. The canary stage runs first, and
every other stage has to depend on it, directly or through another stage.
`wave-1` runs two targets at a time, keeps going when one fails, and lets
`wave-2` start when at least two of its three targets succeed.

`expect_sequence` is an approval for one target: the migrations its run must
select under its migration lock, in the format that
`ptah migrations up --expect-sequence` reads. Approvals are per target on
purpose, and so is `allow_destructive`. There is no setting that approves a
migration for the whole rollout.
[Rollout definition and state](../../reference/rollout-definition/) lists every
key.

## Check the plan

`plan` validates the definition and prints the stages in the order they run,
with their members in the order they start. It contacts no database and no
registry:

```bash
ptah migrations rollout plan --definition rollout.yaml
```

```text
Rollout plan sha256:05e9aaed…

Stage 1: canary (canary)
  concurrency: 1, on failure: stop, requires: all (1 of 1)
  - canary  postgres://app:***@db-canary.internal/app  (url from $CANARY_DATABASE_URL)

Stage 2: wave-1
  after: canary
  concurrency: 2, on failure: continue, requires: 2 (2 of 3)
  - tenant-a  postgres://app:***@db-a.internal/app  (url from $TENANT_A_DATABASE_URL)
  ...
```

Every rule a definition breaks is reported in one run: a repeated target, a
stage naming a target that is not in the inventory, a target in two stages or in
none, a dependency cycle with its path, and a stage that could start before the
canary succeeds. `plan` reads the variables `url_env` names, so run it where
they are set, such as the job that runs the rollout, before the rollout starts.

## Run the rollout

```bash
ptah migrations rollout run \
  --definition rollout.yaml \
  --migrations-dir ./migrations \
  --state-file rollout-state.json
```

The run binds the release before any target starts. A local directory is
captured once and every target reads a private copy of that capture, so an edit
to the directory during the rollout changes nothing it runs. An `oci://`
reference is resolved to a digest once and every target pulls that digest, so a
tag moved during the rollout changes nothing either. The state file records the
digest, and `resume` refuses content that does not match it.

Each line a target's run writes is relayed to standard error with the target's
name in front of it, and its credentials taken out. The rollout's own progress
goes to standard output:

```text
Stage canary: running, at most 1 at once
  canary: running against postgres://app:***@db-canary.internal/app
  canary: succeeded: applied 2, at version 2
Stage canary: succeeded: 1 of 1 targets succeeded, 1 required
Stage wave-1: running, at most 2 at once
```

The command exits `0` when every stage succeeded, and `1` when the rollout ran
and a stage failed or was blocked. Exit `2` means it refused before running
any target, for an invalid definition or a state file that already exists, for
example.

## Read the outcome

The state file is the record. It names the plan digest, the release, the Ptah
version that ran, and the outcome of every stage and target:

```bash
jq '.stages[] | {stage: .name, status, targets: [.targets[] | {name, status}]}' rollout-state.json
```

A target's status is one of `not-started`, `running`, `succeeded`, `no-op`,
`failed`, `partial`, `unknown`, `skipped` and `blocked`. Its outcome comes from
the run document of `ptah migrations up --json`, which reads the target's
revision table after the run, not from the process exit status. `--json` prints
the same state document on standard output when the run ends.
[Rollout definition and state](../../reference/rollout-definition/#target-statuses)
says what each status means.

## When a target fails

With `on_failure: stop`, the default, a failure starts no further target of the
stage. Targets already running finish, and their outcomes are kept. The rest of
the stage is `skipped`. With `on_failure: continue`, every target of the stage
runs.

Either way, the stage succeeds only when its successes meet its `require`, and
every stage that depends on a stage that did not succeed is `blocked`, with each
of its targets left untouched. A stage that does not depend on the failed one
still runs.

The target's error is in the state file. Decide what it needs before resuming:

- A refusal before anything ran, such as a stale approval, a pre-migration
  check, a destructive migration the lint gate stopped, or a lock held by
  another runner, leaves a clean revision table. Fix the cause and resume with
  `--retry-failed`.
- A migration that failed leaves a dirty revision row in that target's revision
  table. The rollout never retries it. Recover that database by hand, with
  `ptah migrations up --allow-dirty` for a verified retry or
  `ptah migrations repair`, and then resume.

The rollout does not roll anything back. Rolling a target back is
`ptah migrations down` against that target, and it stays your decision.

## Resume

Pass the same flags to `resume`. Add `--retry-failed` only to run a failed
target again:

```bash
ptah migrations rollout resume \
  --definition rollout.yaml \
  --migrations-dir ./migrations \
  --state-file rollout-state.json \
  --retry-failed
```

The definition must resolve to the plan the run started with; a changed member,
stage setting or database is refused. A rotated password is not a change. The
release must hold the recorded content. To roll out a changed definition or a
new release, start a new run with a new state file. Targets already at the
release's head come out `no-op` there.

Before anything runs, every target the run started is read back from its own
revision table with `ptah migrations status`:

| What the revision table holds | What the resume does |
| --- | --- |
| Every migration of the release applied | Counts the target as succeeded and does not run it, whatever was recorded |
| A dirty revision row | Records `failed`, `partial` or `unknown` as the row says, and does not run it |
| Nothing readable | Records `unknown`, and does not run it |
| Pending migrations after a recorded failure | Runs it again only with `--retry-failed` |
| Pending migrations after any other status | Runs it again |

A run that is started again selects only the migrations its revision table does
not record, under its migration lock, so no statement runs twice. A target the
run never started is read by its own run when its turn comes. Skipped and
blocked targets start once their stage may run. `--retry-failed` takes no
environment variable: a resume retries failures only when whoever starts it
asks.

## Interrupt a rollout

Ctrl-C, or a SIGTERM from a CI runner, stops the rollout from starting further
targets. On Linux and macOS each target run in progress is asked to stop once,
as Ctrl-C asks `ptah migrations up`, and stops at a safe point. On Windows the
rollout sends a run nothing: a Ctrl-C in the console reaches it directly, and
otherwise it finishes. The outcomes of the runs in progress are recorded, the
state file says `interrupted`, and the command exits with the signal status.
`resume` continues from there.

Two rollouts on one state file at a time are refused: the first holds a lock
file beside the state, `rollout-state.json.lock`. A process killed outright
leaves the lock behind, and the refusal names the file to remove once you know
no rollout is running.

## Concurrency and locks on a shared server

Each target run takes the migration lock `ptah migrations up` takes, named
`ptah_migrate`, on the server its URL names. How far stage concurrency gets
depends on where that lock lives:

| Engine | Lock scope | Targets on one server |
| --- | --- | --- |
| PostgreSQL family | one database | Targets in different databases run in parallel. Targets in one database, such as one schema per tenant, take the same lock and run one after another. |
| MySQL and MariaDB | the whole server | `GET_LOCK` names are server-wide, so every target on one server runs one after another, whatever the stage concurrency. |
| SQL Server | one database | Targets in different databases run in parallel. |
| SQLite | none | Each file is its own target. |

A target waiting for the lock waits as `ptah migrations up` does: without limit,
or as long as `--migration-lock-timeout` allows, after which its run fails with
a lock timeout and leaves a clean revision table. Set the timeout the way you
set it for a single run, in the target's project config or with
`PTAH_MIGRATION_LOCK_TIMEOUT`, and size `concurrency` to the number of
simultaneous schema changes the server can carry.

Two targets that name one database with the same connection options and config
are refused as duplicates. A different `search_path`, or a config that names
another revision table, makes them two targets.

## What a rollout passes to each target

A target's run starts with the rollout's environment and working directory, so
`./ptah.yaml`, `PTAH_LOCK_TIMEOUT` and the other settings of
`ptah migrations up` reach every target. The rollout sets the database URL,
`--migrations-dir`, `--json` and text logs itself, and adds a target's `config`,
`expect_sequence` and `allow_destructive` from the definition.

A variable that would waive a target's guard, or change what its run does, for
every target at once refuses the rollout instead: `PTAH_ALLOW_DIRTY`,
`PTAH_SKIP_CHECKS`, `PTAH_ALLOW_DESTRUCTIVE`, `PTAH_EXPECT_SEQUENCE`,
`PTAH_DRY_RUN`, `PTAH_LIMIT` and `PTAH_TO_VERSION`. Unset it, and set the
per-target key where one target needs it.

## Coverage

| Area | Covered |
| --- | --- |
| Engines | Every engine `ptah migrations up` applies to, since each target runs it. |
| Releases | A local migration directory in any format `ptah migrations up` reads, or an `oci://` artifact. |
| Component versions | One `ptah` binary runs the rollout and every target of an invocation; the state file records its version in `ptah_version`, and a resume records the binary that ran it. |
| Kubernetes | Ptah Operator reconciles each database resource on its own and is not involved; run the rollout from a Job or a CI step. |

## Limitations

- Only versioned migrations are rolled out. Direct schema changes are not: a
  schema plan is computed for one database, and an approval of it authorizes
  nothing on another.
- A rollout never rolls a fleet back and never retries a dirty revision row.
- A local release holds at most 1000 files, the limit of the content digest the
  run binds to. Publish a larger directory with `ptah migrations push` and roll
  out the artifact.
- Stages run one at a time. Two stages that depend on the same canary run in
  the order the definition declares them, not side by side.
- The rollout passes no `--provider-config` or `--provider-engine` to a target's
  run, so a target that needs either is not covered. A provider installed with
  `ptah providers install` serves a target from its URL, as it serves
  `ptah migrations up`.

## Next steps

- Look up every definition key and state field in
  [Rollout definition and state](../../reference/rollout-definition/).
- Publish the release you roll out as an artifact with
  [OCI registry artifacts](../oci-registry/).
- Gate each target's run on its own approval with
  [Apply migrations](../../versioned/apply/).
