// Package ddltx names the data definition language (DDL) transaction contract
// Ptah's migrator implements for each dialect.
//
// The migrator writes a migration's body and the revision row that says the
// migration succeeded, and what happens when the second write fails depends
// entirely on whether the first one can still be undone. That question has more
// than one answer across Ptah's targets, and before this package each answer
// was spelled separately: the MySQL family was recognized by an unexported
// dialect switch in migration/migrator, ClickHouse by a no-op
// BeginTransaction in its writer plus ad-hoc isClickHouse checks, and every
// other target was whatever fell through. Nothing named the set, so nothing
// could notice a dialect that had never been assigned an answer — which is the
// failure mode issue #999 asks tests to rule out.
//
// [ClassOf] is that name. It has one arm per dialect and no catch-all, so a
// target added to core/platform/capability without a decision here reads as
// [Unclassified] and fails the guard in this package's tests rather than
// silently inheriting the transactional contract.
package ddltx

import (
	"go.5x5.cz/ptah/core/platform"
)

// Class is a DDL transaction contract: what a database guarantees about a
// migration body once the migrator has executed it and is about to record the
// migration as applied.
type Class string

const (
	// Unclassified is the zero value: no contract has been established for
	// this dialect. It is what [ClassOf] answers for a dialect
	// platform.NormalizeDialect does not recognize, and it is deliberately
	// what a newly added dialect gets until someone decides.
	Unclassified Class = ""

	// Transactional marks targets whose DDL participates in the client's
	// transaction. The migration body and the success revision are written
	// inside one transaction, so they commit together or roll back together,
	// and a failed revision-completion write leaves no trace of the body.
	Transactional Class = "transactional"

	// ImplicitCommit marks targets whose server commits the open transaction
	// on its own before DDL runs. The body is durable the moment it executes,
	// so a later revision-completion failure cannot undo it: the schema change
	// stays visible and the revision row must become an accurate dirty record
	// of a migration that is half done from Ptah's point of view.
	ImplicitCommit Class = "implicit_commit"

	// NoTransaction marks targets that offer the migrator no transaction at
	// all. BeginTransaction, Commit and Rollback are accepted and do nothing,
	// so every statement — body and revision write alike — is independently
	// durable. Like ImplicitCommit the body survives a revision-completion
	// failure; unlike it, no commit step exists that could fail on its own.
	NoTransaction Class = "no_transaction"
)

// ClassOf returns the DDL transaction contract Ptah's migrator implements for
// a dialect. The dialect is normalized through platform.NormalizeDialect, so
// every accepted spelling of a target answers the same.
//
// Each arm states what the answer rests on. Three of them are proven against a
// live server by the revision-completion matrix in migration/migrator; the
// rest record the contract the migrator's code path already implements and are
// marked as not yet driven live.
func ClassOf(dialect string) Class {
	switch platform.NormalizeDialect(dialect) {
	// Proven live by TestRevisionCompletionFailure_PostgresTransactionalLive
	// and, in process, by the SQLite case of the same matrix.
	case platform.Postgres, platform.SQLite:
		return Transactional

	// Not yet driven live for revision completion. These three reach the
	// database through the same transactional migrator path PostgreSQL takes:
	// dbschema routes CockroachDB, YugabyteDB and Spanner onto the PostgreSQL
	// driver, and SQL Server has its own writer with a real transaction. Their
	// contract is therefore the one the code implements, not a measurement.
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner, platform.SQLServer:
		return Transactional

	// Proven live by TestRevisionCompletionFailure_MySQLImplicitCommitLive and
	// TestRevisionCompletionFailure_MariaDBImplicitCommitLive.
	case platform.MySQL, platform.MariaDB:
		return ImplicitCommit

	// Proven live by TestRevisionCompletionFailure_ClickHouseNoTransactionLive.
	// The ClickHouse writer's BeginTransaction returns a transaction whose
	// Commit and Rollback are no-ops.
	case platform.ClickHouse:
		return NoTransaction

	default:
		return Unclassified
	}
}

// BodySurvivesRevisionCompletionFailure reports whether a migration body that
// executed successfully is still present in the catalog after the write that
// records the migration as applied has failed.
//
// This is the half of the body-plus-revision invariant that differs by class,
// and stating it as a function keeps the tests from restating the rule per
// dialect — which is how a matrix comes to assert the transactional answer on
// a target that never had it.
func BodySurvivesRevisionCompletionFailure(class Class) bool {
	return class == ImplicitCommit || class == NoTransaction
}

// AllStatementsDurable reports whether every statement of a migration body is
// durable the moment it executes, so a body that ran to completion is fully
// applied no matter what happens afterwards.
//
// Only [NoTransaction] qualifies. [ImplicitCommit] is deliberately excluded and
// the distinction is not academic: on MySQL the server commits before each DDL
// statement, so a body of `CREATE TABLE ...; INSERT ...` keeps the CREATE and
// loses the INSERT when the transaction rolls back. The surviving prefix there
// has to come from the revision-row witness, statement by statement; claiming
// the whole body would record an INSERT that never happened as applied and a
// resume would skip it. See stokaro/ptah#887.
func AllStatementsDurable(class Class) bool {
	return class == NoTransaction
}

// HasCommitStep reports whether the migrator issues a commit that can fail on
// its own after the revision-completion write has succeeded.
//
// Only [Transactional] targets have one. On [ImplicitCommit] targets the DDL
// has already been committed by the server before the commit is reached, and
// on [NoTransaction] targets the commit is a no-op that cannot fail, so
// "commit failure" is not a state those classes can be driven into.
func HasCommitStep(class Class) bool {
	return class == Transactional
}
