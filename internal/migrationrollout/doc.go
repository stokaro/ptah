// Package migrationrollout coordinates one versioned migration release across
// many databases: a staged rollout.
//
// A rollout definition names an inventory of targets and the stages they are
// grouped in. A stage may depend on others, carries its own concurrency and
// failure policy, and states how many of its targets must succeed before a
// stage that depends on it may start. One stage may be marked as the canary,
// which every other stage then has to depend on.
//
// The package owns the semantics and nothing that touches a database. [Decode]
// reads a definition strictly, [Definition.Resolve] validates it and fixes the
// order of stages and of the targets in each, and [Runner] schedules the
// targets and records what each run reported in a [State]. The work on one
// target is an [Executor]'s: the native command runs `ptah migrations up` for
// each target, so every guard that command applies to one database --
// integrity checks, the migration lock, dirty-revision refusals, pre-migration
// checks and approvals -- applies to each target in a rollout unchanged. A
// rollout adds no approval of its own that could stand in for one of them.
//
// A run is bound to one release, recorded as a [Release] digest, and to one
// resolved plan, recorded as [Plan.Digest]. A resumed run refuses either one
// changing, and reads every target it started back from the target's own
// revision table before it decides what to run next: see [Runner.Resume].
//
// Only versioned migrations are rolled out; direct schema changes are not. A
// schema plan computed and approved for one database authorizes nothing on
// another, so a rollout carries no plan between targets: each target selects
// its own pending migrations under its own migration lock.
package migrationrollout
