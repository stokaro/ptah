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
// [ClassOf] is that name. It reads the class each dialect's descriptor states
// and has no default, so a target added to core/platform/capability whose
// descriptor states no decision reads as [Unclassified] and fails the guard in
// this package's tests rather than silently inheriting the transactional
// contract.
package ddltx

import (
	"slices"

	"ptah.run/core/platform"
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

// Classes returns every non-zero DDL transaction contract [ClassOf] can assign.
func Classes() []Class {
	return []Class{Transactional, ImplicitCommit, NoTransaction}
}

// ClassOf returns the DDL transaction contract Ptah's migrator implements for
// a dialect: the DDLTransactions its descriptor in the process registry
// states. The dialect is folded as platform.NormalizeDialect folds it, so
// every accepted spelling of a target answers the same.
//
// A dialect the registry does not know, or whose descriptor states no
// contract or one this package does not declare, is [Unclassified]. Each
// built-in descriptor says beside its value what the answer rests on: a live
// revision-completion test in migration/migrator, or the code path the
// migrator implements where no test drives one yet.
func ClassOf(dialect string) Class {
	described, found := platform.Lookup(dialect)
	if !found {
		return Unclassified
	}
	class := Class(described.DDLTransactions)
	if !slices.Contains(Classes(), class) {
		return Unclassified
	}
	return class
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
