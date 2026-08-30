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
// The database is disposable by construction. Every entry point drops it clean
// before the replay and refuses a URL that could resolve to the target's live
// realm, because the verification is destructive and the target is not.
//
// # The four questions
//
//   - [VerifyMigration] replays the prior history plus a candidate migration,
//     compares the result with the desired schema, then rolls the candidate back
//     and forward again. It is what [go.5x5.cz/ptah/migration/generator]
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
package shadow
