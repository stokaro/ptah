# Staged rollout example

## What this example demonstrates

This example rolls one migration release out to five SQLite databases in
stages: a canary, a wave of three run two at a time, and a last target. The
canary's approved sequence is stale, so the canary refuses to run and both
waves stay blocked. With the approval corrected, a resume runs the canary and
then every target the rollout still owes.

Each target is a SQLite file, so the example needs no database server. The
rollout runs `ptah migrations up` for each target, with its own integrity
check, migration lock and approval.

## Prerequisites

- A `ptah` binary on `PATH`, built from this repository with `make build`.
- A shell in this directory, `examples/staged-rollout`.

Nothing else: no server, no account and no orchestrator.

## Run

The approval is written first and names only the first migration, the way an
approval made before the second migration was added would:

```bash
mkdir -p databases approvals
printf '{"migrations": [{"version": 1, "version_key": "1"}]}\n' > approvals/canary.json
ptah migrations rollout plan --definition rollout.yaml
ptah migrations rollout run --definition rollout.yaml --migrations-dir migrations --state-file rollout-state.json || echo "rollout exit status: $?"
printf '{"migrations": [{"version": 1, "version_key": "1"}, {"version": 2, "version_key": "2"}]}\n' > approvals/canary.json
ptah migrations rollout resume --definition rollout.yaml --migrations-dir migrations --state-file rollout-state.json --retry-failed
```

## Expected result

`plan` prints three stages in the order they run. The first `run` stops at the
canary and exits `1`, and nothing behind the canary is opened:

```text
  canary: failed: the migrations selected under the migration lock are [1 2], and the approved sequence is [1]: the history moved after the sequence was approved, so nothing was run
Stage canary: failed: 0 of 1 targets succeeded, 1 required
Stage wave-1: blocked by canary
Stage wave-2: blocked by wave-1
```

The `resume` reads the canary back, finds its revision table clean, and runs it
again because `--retry-failed` asks for that. The waves follow, two targets at
a time in `wave-1`, and the command exits `0`:

```text
Stage canary: succeeded: 1 of 1 targets succeeded, 1 required
Stage wave-1: succeeded: 3 of 3 targets succeeded, 2 required
Stage wave-2: succeeded: 1 of 1 targets succeeded, 1 required

Rollout succeeded
```

## Verify

```bash
ptah migrations status --db-url sqlite://databases/tenant-d.db --migrations-dir migrations
grep -c '"status": "succeeded"' rollout-state.json
```

The last target reports `Current Version: 2` and no pending migrations. The
state file records every stage and every target as `succeeded`, nine entries in
all.

## Cleanup

```bash
rm -rf databases approvals rollout-state.json
```

## Learn more

- [Roll a release out in stages](../../docs/site/src/content/docs/operate/staged-rollout.md)
- [Rollout definition and state](../../docs/site/src/content/docs/reference/rollout-definition.md)
- [Apply migrations](../../docs/site/src/content/docs/versioned/apply.md)
