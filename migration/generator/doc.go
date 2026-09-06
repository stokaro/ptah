// Package generator plans migrations from schema differences and publishes
// them as migration files: reversible timestamped up/down pairs, cumulative
// checkpoints in either directory layout, and empty skeletons for manual SQL.
//
// The desired schema comes from annotated Go entities or a pre-merged
// schemamodel.Database, the current schema from a live database connection,
// and the difference from ptah.run/migration/schemadiff. Rendering is
// deterministic and dependency-ordered, and the down file restores the
// introspected current schema rather than merely inverting the up file's
// statement list.
//
// # Plan, then publish
//
// [PlanMigration] performs schema loading, live introspection, diff planning,
// safety checks, and optional shadow verification without writing migration
// artifacts. [MigrationPlan.WriteFilesContext] publishes the validated
// artifacts once. [GenerateMigration] is the convenience composition of the
// two phases and propagates its context through both; a caller with work to
// do between them -- database cleanup, review, its own locking -- calls the
// phases itself.
//
// A plan is a claim on the migration directory, not on a pathname: it binds a
// directory handle while it is built and records the directory snapshot used
// during planning. Publication refuses with [ErrMigrationDirectoryChanged]
// when the directory no longer matches that snapshot, and with
// [ErrMigrationPlanInUse] when a second goroutine publishes through the same
// plan; both are sentinels for errors.Is. A plan that will not be published
// is released with [MigrationPlan.Close] -- deferring Close next to
// PlanMigration is always correct, because closing a published plan and
// closing twice are both no-ops.
//
// When the comparison finds no changes, PlanMigration returns a nil plan with
// a nil error and GenerateMigration returns nil files: nothing to do is a
// successful outcome rather than an error.
//
// The returned [MigrationFiles].Files slice is the authoritative ordered list
// of generated pairs and published paths, named
// {version}_{name}.up.sql and {version}_{name}.down.sql with a Unix-seconds
// version. One generation can publish several pairs: statements the target
// dialect cannot run in the same transaction as the rest -- a concurrent index
// build, for one -- are split into files of their own, in the order they must
// run, so a caller iterates Files rather than assuming a single pair.
//
// # Other writers
//
// [GenerateEmptyMigration] creates a skeleton pair (or a single Atlas-layout
// file) for manual SQL, with no database involved. [WriteDataMigrationFiles]
// publishes an ordinary pair whose SQL bodies the caller rendered.
// [WriteCheckpointFilesWithOptions] and [WriteAtlasCheckpointFileWithOptions]
// publish a cumulative checkpoint in the reversible Ptah pair and the up-only
// Atlas single-file convention respectively, and
// [GenerateCheckpointFromShadow] renders the checkpoint bodies by replaying a
// migration directory into a disposable shadow database.
//
// # Lower-level planning
//
// [PlanBidirectionalSchemaDiff] is the planning boundary for a caller that
// already holds a schema diff: it binds the diff, the desired and current
// schemas, dialect capabilities, and concurrent-index policy into one
// validated forward and reverse plan, with AST nodes and an independent
// RequiresNoTransaction classification for each direction.
//
// # Shadow verification
//
// Setting GenerateMigrationOptions.ShadowDatabaseURL measures the candidate
// against a live disposable database before any file is written. That
// measurement is not this package's work. It belongs to
// ptah.run/migration/shadow, which this package calls and whose
// structured *shadow.VerificationError it returns unchanged through
// PlanMigration and GenerateMigration, inspectable with errors.As. What stays
// here is the offline half: a diff, a directory, and the files published into
// it.
//
// # Concurrency
//
// The package's functions are safe for concurrent use. A [MigrationPlan] is
// not a shared resource: it is single-use, and concurrent publication through
// one plan returns [ErrMigrationPlanInUse] rather than blocking.
package generator
