package migrator

import "fmt"

// A body that opens and closes its own transactions, in a directory loaded
// with [WithOwnTransactions], is repaired with those transactions read as
// they ran: a repair that runs none of its SQL records the revision the way a
// run that reached its end would, and nothing resumes it from a statement.
// These are the refusals such a repair returns, and each names the way to end
// the revision that works for such a directory.

// downEndingsByHand is how a rollback that committed part of a down file
// that opens its own transactions ends: the operator finishes the rollback
// and moves the boundary, or restores what it reverted and records the
// migration applied.
const downEndingsByHand = "finish the rollback by hand and run \"ptah migrations set --version <previous>\" " +
	"(version 0 where this is the oldest migration in the directory), or restore what it reverted and repair " +
	"with --force to record the migration applied"

// upResumeByRun is how an up file that opens its own transactions and
// stopped partway ends: a retry under the same runner resumes after the
// transactions that committed, which a repair cannot do.
const upResumeByRun = "run \"ptah migrations up --allow-dirty\" with the golang-migrate runner, which resumes " +
	"after the transactions that committed, or repair with --force once you have run the rest yourself"

// ownTransactionsResumeError refuses --resume-from over a body that opens its
// own transactions. A resume runs the rest of the body one statement at a
// time, outside any transaction, so the statements after a BEGIN would commit
// one by one, and the COMMIT that ends them would commit nothing.
func ownTransactionsResumeError(migration *Migration, revision *MigrationRevision, direction MigrationDirection) error {
	file := "up"
	if direction == MigrationDirectionDown {
		file = "down"
	}
	return fmt.Errorf(
		"migration %d cannot be resumed: its %s file opens and closes its own transactions, and a resume would "+
			"run the rest of it one statement at a time, outside them; %s",
		migration.Version, file, ownTransactionsNextStep(revision, direction),
	)
}

// ownTransactionsNextStep names what ends a dirty revision of a body that
// opens its own transactions, in place of a resume.
func ownTransactionsNextStep(revision *MigrationRevision, direction MigrationDirection) string {
	complete := revision != nil && revision.Total > 0 && revision.Applied == revision.Total
	nothing := revision != nil && revision.Applied == 0 && !revision.StatementOutcomeUnknown()
	if direction == MigrationDirectionDown {
		switch {
		case complete:
			return "every down statement committed, so repair without --resume-from to remove the revision"
		case nothing:
			return "no down statement committed, so repair without --resume-from to record the migration " +
				"applied, and roll it back again"
		}
		return downEndingsByHand
	}
	switch {
	case complete:
		return "every statement committed, so repair without --resume-from to record it applied"
	case revision != nil && revision.StatementOutcomeUnknown():
		return "inspect the database and apply what is missing, then repair with --force to record it applied"
	}
	return upResumeByRun
}

// ownTransactionsDownRepairError is [rollbackRepairNeedsDirectionError] for a
// down file that opens its own transactions, which offers no resume.
func ownTransactionsDownRepairError(migration *Migration, revision *MigrationRevision) error {
	return fmt.Errorf(
		"migration %d stopped while rolling back: %d of %d down statements committed, in transactions its down "+
			"file opens itself, and a resume would run the rest outside them; %s",
		migration.Version, revision.Applied, revision.Total, downEndingsByHand,
	)
}

// ownTransactionsUpRepairError refuses to record applied an up file that
// opens its own transactions and stopped partway, and names the retry that
// finishes it.
func ownTransactionsUpRepairError(migration *Migration, revision *MigrationRevision) error {
	return fmt.Errorf(
		"migration %d recorded %d of %d applied statements, in transactions its file opens itself; repair "+
			"without --resume-from would mark the rest applied without running them: %s",
		migration.Version, revision.Applied, revision.Total, upResumeByRun,
	)
}

// unwrappedRepairError refuses a repair that would read the body of direction
// one statement at a time although it controls transaction state, with err,
// the refusal [Migrator.validateNoTransactionSQL] returns.
//
// For a migrator that was not loaded with [WithOwnTransactions], over a body
// that such a migrator would run with its own transactions, the refusal also
// names the runner: no run of a migrator without the option marks a revision
// for such a body, because every run refuses the body first, and a directory
// written for golang-migrate leaves such revisions behind. Where the runner
// would refuse the body too, as on an engine whose migrator reads no body as
// one with transactions of its own, naming it would send the operator to a
// second refusal. A file edited after its run failed can still hold
// transaction control, which is why the advice err gives stays.
func (m *Migrator) unwrappedRepairError(migration *Migration, direction MigrationDirection, err error) error {
	if migration.ownTransactions || !migration.runnerReadsOwnTransactions(m.conn, direction) {
		return fmt.Errorf("migration %d cannot be repaired safely: %w", migration.Version, err)
	}
	return fmt.Errorf(
		"migration %d cannot be repaired safely: %w; a directory written for golang-migrate, whose files open "+
			"their own transactions, is repaired with --runner golang-migrate",
		migration.Version, err,
	)
}
