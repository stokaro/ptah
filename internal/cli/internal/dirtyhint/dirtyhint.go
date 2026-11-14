// Package dirtyhint words the command that ends a dirty migration revision,
// for the native commands that meet one: `ptah migrations status` prints it
// under the revision, and `ptah migrations down` adds it to its refusal to roll
// back past one.
package dirtyhint

import (
	"fmt"

	"ptah.run/internal/ddltx"
	"ptah.run/migration/migrator"
)

// Revision is the dirty revision a hint is about, with what the revision row
// alone cannot say.
type Revision struct {
	// Revision is the dirty row.
	Revision *migrator.MigrationRevision
	// Dialect is the dialect of the database that holds it.
	Dialect string
	// OwnTransactions reports that the body the failed run executed opens and
	// closes transactions of its own that the golang-migrate runner runs as
	// written ([migrator.Migrator.RevisionBodyOpensOwnTransactions]). Every
	// command that runs such a body, or repairs it over committed progress,
	// needs that runner, and none resumes it from a statement.
	OwnTransactions bool
}

// Recovery names the command that ends this particular dirty state, one that
// `ptah migrations repair` or `ptah migrations up` accepts over it. A rollback
// that stopped partway is not repaired by recording the migration applied --
// that would sign it off over a schema whose objects the rollback already
// dropped -- so it points at the resume that finishes the rollback instead, at
// the statement the revision says comes next.
//
// The other shapes are read off `applied`, which is why the dialect is read
// too. `applied` counts what is known to have committed, and an interrupted
// statement may have committed as well without recording that; on a dialect
// whose statements commit on their own, nothing writes a checkpoint at all, so
// zero there says only that no progress was recorded. Neither shape can be
// answered from the count alone, so each names the reconciliation and then the
// forced record, because a plain repair refuses to record a body it cannot
// show ran.
//
// `--resume-from` is offered only where a statement is left to run and the
// body can be resumed. A body that committed every statement and stopped
// before recording that has none, and so does a body with no statements at
// all, and the command refuses the offset past the end either way. Both are
// read before the direction split rather than inside each side of it. A body
// that opens its own transactions cannot be resumed at all, so its hint names
// the retry or the manual end instead.
func Recovery(dirty Revision) string {
	revision := dirty.Revision
	if revision.RollbackLeftUnknown() {
		return rollbackLeftUnknownHint(dirty)
	}
	if revision.StatementOutcomeUnknown() {
		return unknownStatementOutcomeHint(dirty)
	}
	if revision.Applied == revision.Total {
		return completedBodyHint(dirty)
	}
	if revision.Applied == 0 && ddltx.AllStatementsDurable(ddltx.ClassOf(dirty.Dialect)) {
		return uncheckpointedHint(dirty)
	}
	if revision.Direction == migrator.MigrationDirectionDown {
		return partialRollbackHint(dirty)
	}
	if revision.Applied == 0 {
		return fmt.Sprintf("No statement of this migration reached the database. Run %s to apply it.", dirty.retry())
	}
	if dirty.OwnTransactions {
		return fmt.Sprintf(
			"This migration stopped after %d of %d statements, in transactions its file opens itself. Run %s "+
				"to resume after the transactions that committed, or run %s once you have run the rest yourself.",
			revision.Applied,
			revision.Total,
			dirty.retry(),
			dirty.repair(" --force"),
		)
	}
	return fmt.Sprintf(
		"This migration stopped after %d of %d statements. Run %s to run the rest, "+
			"or repair with --force once you have run them yourself.",
		revision.Applied,
		revision.Total,
		dirty.repair(fmt.Sprintf(" --resume-from %d", revision.Applied+1)),
	)
}

// runnerFlag is what every command a hint names for a body that opens its own
// transactions carries, because only that runner reads such a body with its
// transactions.
const runnerFlag = " --runner golang-migrate"

// repair is the quoted repair command for the revision, with flags after its
// version.
func (r Revision) repair(flags string) string {
	command := fmt.Sprintf("ptah migrations repair --version %d%s", r.Revision.Version, flags)
	if r.OwnTransactions {
		command += runnerFlag
	}
	return "'" + command + "'"
}

// retry is the quoted command that applies the migration again from where its
// revision says it stopped.
func (r Revision) retry() string {
	command := "ptah migrations up --allow-dirty"
	if r.OwnTransactions {
		command += runnerFlag
	}
	return "'" + command + "'"
}

// partialRollbackHint names what ends a rollback that stopped before its
// last down statement, on a dialect that records how far it got. A rollback
// that committed nothing left the migration applied, and recording that lets
// the next rollback run the whole down file again, inside the transactions it
// asks for. A rollback that committed part of the file is finished by the
// resume that runs the rest, except where the file opens its own transactions:
// a resume would run the rest outside them, so such a rollback is finished by
// hand, or undone by hand and recorded applied.
func partialRollbackHint(dirty Revision) string {
	revision := dirty.Revision
	if revision.Applied == 0 {
		return fmt.Sprintf(
			"No down statement of this rollback committed, so the migration is still applied. Run %s "+
				"to record it applied, then roll it back again.",
			dirty.repair(""),
		)
	}
	if dirty.OwnTransactions {
		return fmt.Sprintf(
			"This rollback committed %d of %d down statements, in transactions its file opens itself, and "+
				"a resume would run the rest of the file outside them. Finish the rollback by hand and run "+
				"'ptah migrations set --version <previous>' (version 0 where this is the oldest migration in "+
				"the directory), or restore what it reverted and run %s to record the migration applied.",
			revision.Applied,
			revision.Total,
			dirty.repair(" --force"),
		)
	}
	return fmt.Sprintf(
		"This rollback stopped after %d of %d down statements. Run %s to run the remaining down statements "+
			"and remove the revision.",
		revision.Applied,
		revision.Total,
		dirty.repair(fmt.Sprintf(" --resume-from %d", revision.Applied+1)),
	)
}

// completedBodyHint names what ends a revision whose body ran to its last
// statement and stopped before the run could record that. A plain repair is the
// answer in both directions and the outcome it reaches is the opposite one:
// recording the migration applied for an up body, removing the revision for a
// rollback that un-applied it.
func completedBodyHint(dirty Revision) string {
	if dirty.Revision.Direction == migrator.MigrationDirectionDown {
		return fmt.Sprintf(
			"Every down statement of this rollback committed and the run stopped before removing "+
				"the revision. Run %s to finish it.",
			dirty.repair(""),
		)
	}
	return fmt.Sprintf(
		"Every statement of this migration committed and the run stopped before recording that. "+
			"Run %s to record it applied.",
		dirty.repair(""),
	)
}

// uncheckpointedHint names what ends a dirty row on a dialect where every
// statement is durable the moment it runs and the migrator therefore writes no
// per-statement witness. Zero progress says only that nothing was recorded, so
// the body may have run in full, in part, or not at all, and no verb decides
// between those on its own: a repair records a body that may be half there, and
// a rerun repeats what already committed. The operator finishes the body and
// the hint names the record.
//
// Which record that is depends on the direction, and naming the wrong one is
// worse than naming none: 'migrations up --allow-dirty' is refused outright
// over a row a rollback left.
func uncheckpointedHint(dirty Revision) string {
	if dirty.Revision.Direction == migrator.MigrationDirectionDown {
		return fmt.Sprintf(
			"On %s a statement commits on its own, so nothing records how far this rollback got: "+
				"it may have run in full, in part, or not at all. Inspect the database and finish "+
				"the rollback by hand, then run 'ptah migrations set --version <previous>' (version 0 "+
				"where this is the oldest migration in the directory) to move the boundary back -- "+
				"or, if you restore what it reverted instead, %s to record the migration applied.",
			dirty.Dialect,
			dirty.repair(" --force"),
		)
	}
	return fmt.Sprintf(
		"On %s a statement commits on its own, so nothing records how far this run got: it may "+
			"have run in full, in part, or not at all. Inspect the database and apply whatever is "+
			"missing, then run %s to record it applied. "+
			"%s is safe only once you have confirmed no statement of it ran.",
		dirty.Dialect,
		dirty.repair(" --force"),
		dirty.retry(),
	)
}

// rollbackLeftUnknownHint is the hint for a row whose transaction rolled back and
// left statements in place that no count describes: a schema statement stayed and
// a statement ahead of it did not.
func rollbackLeftUnknownHint(dirty Revision) string {
	if dirty.Revision.Direction == migrator.MigrationDirectionDown {
		return fmt.Sprintf(
			"The rollback stopped, and the rollback of its transaction left some statements in place and "+
				"undid others. Inspect the database, then run %s to "+
				"record the migration applied if you restored what the rollback reverted, or 'ptah migrations set "+
				"--version <previous>' (version 0 where this is the oldest migration in the directory) if you "+
				"finished the rollback by hand.",
			dirty.repair(" --force"),
		)
	}
	return fmt.Sprintf(
		"The migration stopped, and the rollback of its transaction left some statements in place and undid "+
			"others. Inspect the database and apply what is missing, then run %s "+
			"to record the migration applied -- rerunning it could repeat a statement that stayed.",
		dirty.repair(" --force"),
	)
}

// unknownStatementOutcomeHint names what ends a revision whose statement was
// interrupted before its outcome could be recorded. One statement may have
// committed and the rest of that body did not run, so no verb finishes it on
// its own: the operator reconciles the database and then records the result.
// Which record that is depends on the direction, because a repair that signs a
// migration off is the wrong answer for a rollback that was reverting it.
func unknownStatementOutcomeHint(dirty Revision) string {
	revision := dirty.Revision
	if revision.Direction == migrator.MigrationDirectionDown {
		return fmt.Sprintf(
			"The rollback was interrupted while down statement %d of %d was executing, so whether "+
				"it committed was never recorded, and nothing after it ran. Inspect the database, "+
				"then run %s to record the migration "+
				"applied if you restored what the rollback reverted, or 'ptah migrations set "+
				"--version <previous>' (version 0 where this is the oldest migration in the "+
				"directory) if you finished the rollback by hand.",
			revision.Applied+1,
			revision.Total,
			dirty.repair(" --force"),
		)
	}
	return fmt.Sprintf(
		"The run was interrupted while statement %d of %d was executing, so whether it committed "+
			"was never recorded, and nothing after it ran. Inspect the database and apply what is "+
			"missing, then run %s to record the migration "+
			"applied -- rerunning it could repeat the statement that committed.",
		revision.Applied+1,
		revision.Total,
		dirty.repair(" --force"),
	)
}
