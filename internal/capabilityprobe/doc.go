// Package capabilityprobe measures a live database server against the
// capability preset Ptah hands out for it.
//
// A preset in core/platform/capability is a written claim: "a PostgreSQL 17
// server accepts ALTER COLUMN ... SET EXPRESSION". Nothing in the build
// executes that claim, so a preset can be wrong for years and the first
// symptom is a user's failed migration. This package turns each claim into a
// measurement: for one capability it emits the DDL whose acceptance the
// capability governs, executes it against the live server, and compares what
// the server did to what the preset promised (stokaro/ptah#1339, Tier 2 of
// stokaro/ptah#916).
//
// # Three outcomes, never two
//
// Every row is [Agrees], [Disagrees] or [Undecidable]. A disagreement is a
// failure, not a warning. Undecidable is a first-class answer with a mandatory
// reason and it is NOT a synonym for agreement: a harness that folds the two
// together reports a green matrix for capabilities nobody measured, which is
// worse than no harness at all because it manufactures evidence. [Report.Err]
// therefore also fails a run that decided nothing — a probe that skipped every
// row must not read as a probe that passed every row.
//
// # Why the deciding statement is not always the obvious one
//
// Four shapes of statement decide nothing on their own, and each of them was
// measured rather than assumed:
//
//   - Acceptance without enforcement. MySQL before 8.0.16 accepted a CHECK
//     clause and ignored it, which is the entire reason
//     [capability.CheckConstraintsEnforced] exists. DDL acceptance is
//     identical on both sides of that boundary, so the deciding statement is a
//     row the constraint must reject, with a row it must accept as the control
//     that proves the rejection was the constraint talking.
//   - Acceptance of a keyword the server parses and drops. MySQL 9.7.1 accepts
//     CREATE MATERIALIZED VIEW and then recomputes the query on every read, so
//     the decider inserts into the source table and re-reads: a stored result
//     does not move.
//   - Acceptance as a compatibility no-op. CockroachDB parses CREATE INDEX
//     CONCURRENTLY without changing behavior. Real PostgreSQL refuses that
//     statement inside an explicit transaction block and a keyword-only parser
//     has no reason to, so the transaction block is the discriminator.
//   - Acceptance of a same-spelled feature that is a different surface. MySQL
//     9.7.1 accepts CREATE ROLE and GRANT while [capability.RoleManagement] is
//     false for it, because that key names the PostgreSQL role surface Ptah's
//     PostgreSQL planner gates on and no MySQL code path reads. Those keys are
//     declared undecidable per dialect in advance, as data, in plans.go — never
//     derived from an outcome the run did not like.
//
// # Isolation
//
// The probe does not wrap its statements in a transaction it rolls back.
// PostgreSQL refuses CREATE INDEX CONCURRENTLY inside an explicit transaction
// block, so transaction-based isolation would report two true capabilities as
// false. Each run creates a throwaway namespace (a schema on the PostgreSQL
// family, a database on the MySQL family) and drops it at the end.
//
// # Scope
//
// Adding a release line is a data change in cells.go and nothing else. Wiring
// this into CI is stokaro/ptah#1341 and is deliberately not done here.
package capabilityprobe
