// Package shadow verifies migrations against a live disposable database before
// anything reaches the target.
//
// # What a shadow database answers
//
// A migration file is a claim about what a database will look like after it
// runs. Reading the file cannot settle that claim: the server decides what a
// statement means, and two engines that accept the same DDL do not always
// produce the same catalog. The shadow database is how the claim is measured.
// It is provisioned empty, the migrations are replayed into it, and the schema
// that results is compared with the schema that was asked for. What comes back
// is a fact about a real server rather than an inference about text.
//
// The database is disposable by construction. Every entry point refuses a URL
// that could resolve to the target's live realm, because the verification is
// destructive and the target is not. It also refuses a database that holds a
// table, or on the PostgreSQL family anything else the reset before the replay
// would drop, and it empties the database again on its way out, so the same
// URL serves the next run.
//
// # The four questions
//
//   - [VerifyMigration] replays the prior history plus a candidate migration,
//     compares the result with the desired schema, then rolls the candidate back
//     and forward again. It is what [ptah.run/migration/generator]
//     calls before it writes files.
//   - [VerifyBaseline] replays the history up to a version and compares the
//     result with the target, which is what makes recording that version as a
//     baseline honest rather than assumed.
//   - [VerifyRollback] replays a rollback plan on the shadow database before the
//     target is touched, so a down body that does not run fails somewhere
//     harmless.
//   - [PlanDynamicRollback] builds the target version's schema on a dev database
//     and derives the statements that take the live database back to it, without
//     reading a down file at all.
//
// # How a history replays
//
// The revision table does not record the transaction mode a migration ran in,
// so each replay of a history decides it for every file, by
// [ptah.run/migration/migrator.Migrator.WithAppliedHistoryReplay], and takes no
// mode from the caller. A file whose directive names a mode replays in it, a
// file that a run in tx-mode file refuses replays without a transaction, and
// every other file replays in a transaction of its own. The candidates
// [VerifyMigration] applies have run nowhere, so they run as `migrations up`
// runs a pending file.
//
// Every replay into the shadow database, of the history and of a candidate,
// runs through the dev replay guard `ptah migrations validate` runs. Before the
// first statement of a run, the guard refuses a statement whose effect the
// reset of the shadow database cannot take back, such as a write to another
// database on the same server, a role, or a routine body, and the run fails
// with that refusal. The guard's realm is the one database the URL names: a
// MySQL-family URL that names no database is refused when the entry point
// connects. Nothing in this package reads an environment variable to lift a
// refusal. A caller that trusts the SQL says so in the Trust field of the
// options ([ReplayTrust]); trust still leaves the cleanup to the shadow
// database alone.
//
// # Failures are structured
//
// A [VerifyMigration] or [VerifyBaseline] failure carries a
// [VerificationError]: a stage naming where the verification stopped and a
// deterministic list of [Mismatch] values naming what differed, object by
// object. Callers that render text use the error message; callers that report
// machine-readable diagnostics inspect the result with errors.As. The list is
// ordered, so the same drift reports the same way twice.
//
// [VerifyRollback] and [PlanDynamicRollback] report ordinary wrapped errors
// instead: their failures are operational, so there is no mismatch list to
// carry and errors.As for a [VerificationError] does not match. For the
// planner, a schema difference is the successful output rather than a failure.
// The planner's one refusal with a sentinel is [ErrUnmatchedExclude], given
// when its caller asks it to refuse an exclude selector that names nothing;
// match it with errors.Is.
//
// A refusal of the replay guard matches [ErrReplayRefused] with errors.Is from
// every entry point, whatever error carries it.
package shadow
