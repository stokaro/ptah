// Package dirtyhint words the command that ends a dirty migration revision,
// for the native commands that meet one: `ptah migrations status` prints it
// under the revision, and `ptah migrations down` adds it to its refusal to roll
// back past one.
package dirtyhint

import (
	"fmt"
	"io/fs"

	"ptah.run/internal/ddltx"
	"ptah.run/internal/repaircommand"
	"ptah.run/migration/migrationfile"
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
	// FileStatements is how many statements the body a resume would index into
	// holds in the directory now ([migrator.Migrator.RevisionBodyStatements]),
	// or nil when the directory holds no file for the revision. A count other
	// than the revision's Total means the file changed after the run.
	FileStatements *int
	// FailedStatementRunsSeveral reports that the statement the run failed
	// in runs several statements outside a transaction, so some of them may
	// have committed before the one that failed
	// ([migrator.Migrator.RevisionFailedStatementRunsSeveral]). Neither
	// running it again nor resuming after it is right on its own.
	FailedStatementRunsSeveral bool
	// Selection is what [repaircommand.SelectionFor] returns for the inputs
	// the migrator that read the revision was built from. Every command the
	// hint names carries it, so it reads the same directory and revision
	// table.
	Selection repaircommand.Selection
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
	if revision.Applied > 0 && dirty.fileChanged() {
		return changedFileHint(dirty)
	}
	if revision.Applied == 0 && ddltx.AllStatementsDurable(ddltx.ClassOf(dirty.Dialect)) {
		return uncheckpointedHint(dirty)
	}
	if dirty.FailedStatementRunsSeveral && !dirty.OwnTransactions {
		return partlyCommittedHint(dirty)
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
			dirty.reconciled(),
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

// repair is the quoted repair command for the revision, with flags after its
// version.
func (r Revision) repair(flags string) string {
	return repaircommand.Repair(r.Revision.VersionLabel(), flags, r.runner(), r.Selection)
}

// reconciled is the quoted repair command that records the revision applied
// once the operator has reconciled the database, the one the migrator's
// refusal to resume names for the same row.
func (r Revision) reconciled() string {
	return repaircommand.Reconciled(r.Revision.VersionLabel(), r.runner(), r.Selection)
}

// setPrevious is the quoted command that moves the revision boundary below the
// migration after the operator finished a rollback by hand.
func (r Revision) setPrevious() string {
	return repaircommand.SetPrevious(r.Selection)
}

// Selection is the field of that name for a migrator built over dir, read in
// dirFormat, that keeps its revisions in revisionFormat: the inputs the
// command handed [migrator.NewFSMigrator] and
// [migrator.Migrator.WithRevisionTableFormat].
func Selection(dir fs.FS, dirFormat migrationfile.DirFormat, revisionFormat migrator.RevisionTableFormat) repaircommand.Selection {
	return repaircommand.SelectionFor(repaircommand.Reading{
		Dir:        dir,
		DirFormat:  dirFormat,
		AtlasTable: revisionFormat == migrator.RevisionTableFormatAtlas,
	})
}

// FileStatements reads the field of that name for revision out of mig.
func FileStatements(mig *migrator.Migrator, revision migrator.MigrationRevision) *int {
	count, ok := mig.RevisionBodyStatements(revision)
	if !ok {
		return nil
	}
	return &count
}

// fileChanged reports that the directory holds a file for the revision whose
// statement count differs from the count the revision recorded.
func (r Revision) fileChanged() bool {
	return r.FileStatements != nil && *r.FileStatements != r.Revision.Total
}

// changedFileHint names what ends a run that stopped partway through a body
// whose statement count changed after it. The statement number the revision
// records does not index into the file, so a resume from it would run the
// wrong statements, and both resumes refuse it: `ptah migrations up
// --allow-dirty` for a run, and `ptah migrations repair --resume-from` for a
// rollback. The operator reconciles the database and records the result, the
// commands those refusals name.
func changedFileHint(dirty Revision) string {
	if dirty.Revision.Direction == migrator.MigrationDirectionDown {
		return fmt.Sprintf(
			"This rollback stopped after %d of %d down statements, and its down file now has %d, so the "+
				"recorded count does not index into it and a resume would run the wrong statements. Inspect the "+
				"database, then run %s if you restored what the rollback reverted, or %s (version 0 where this "+
				"is the oldest migration in the directory) if you finished the rollback by hand.",
			dirty.Revision.Applied,
			dirty.Revision.Total,
			*dirty.FileStatements,
			dirty.reconciled(),
			dirty.setPrevious(),
		)
	}
	return fmt.Sprintf(
		"This migration stopped after %d of %d statements, and its file now has %d, so the recorded count "+
			"does not index into it and a resume would run the wrong statements. Inspect the database and "+
			"apply what is missing, then run %s to record the migration applied.",
		dirty.Revision.Applied,
		dirty.Revision.Total,
		*dirty.FileStatements,
		dirty.reconciled(),
	)
}

// runner is the runner every command a hint names for the revision carries.
func (r Revision) runner() repaircommand.Runner {
	if r.OwnTransactions {
		return repaircommand.GolangMigrate
	}
	return repaircommand.DefaultRunner
}

// retry is the quoted command that applies the migration again from where its
// revision says it stopped.
func (r Revision) retry() string {
	return "'ptah migrations up --allow-dirty" + string(r.runner()) + string(r.Selection) + "'"
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
				"%s (version 0 where this is the oldest migration in "+
				"the directory), or restore what it reverted and run %s to record the migration applied.",
			revision.Applied,
			revision.Total,
			dirty.setPrevious(),
			dirty.reconciled(),
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

// partlyCommittedHint names what ends a run that failed inside a statement
// that runs several, such as a T-SQL block, outside a transaction. The
// statements before the one that failed committed, so a run from that
// statement repeats them and a run after it skips the rest of it: the operator
// finishes the statement by hand, then resumes after it, or records the result
// where it was the last one.
func partlyCommittedHint(dirty Revision) string {
	revision := dirty.Revision
	failed := revision.Applied + 1
	if revision.Direction == migrator.MigrationDirectionDown {
		next := fmt.Sprintf("%s (version 0 where this is the oldest migration in the directory) to move the "+
			"boundary back", dirty.setPrevious())
		if failed < revision.Total {
			next = fmt.Sprintf("%s to run the remaining down statements and remove the revision",
				dirty.repair(fmt.Sprintf(" --resume-from %d", failed+1)))
		}
		return fmt.Sprintf(
			"This rollback failed in down statement %d of %d, which runs several statements outside a "+
				"transaction, so some of them may have committed. Inspect the database and finish what "+
				"statement %d left undone, then run %s.",
			failed, revision.Total, failed, next,
		)
	}
	next := fmt.Sprintf("%s to record the migration applied", dirty.reconciled())
	if failed < revision.Total {
		next = fmt.Sprintf("%s to run the rest, or %s once you have run them yourself",
			dirty.repair(fmt.Sprintf(" --resume-from %d", failed+1)), dirty.reconciled())
	}
	return fmt.Sprintf(
		"This migration failed in statement %d of %d, which runs several statements outside a transaction, "+
			"so some of them may have committed. Inspect the database and finish what statement %d left "+
			"undone, then run %s.",
		failed, revision.Total, failed, next,
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
				"the rollback by hand, then run %s (version 0 "+
				"where this is the oldest migration in the directory) to move the boundary back -- "+
				"or, if you restore what it reverted instead, %s to record the migration applied.",
			dirty.Dialect,
			dirty.setPrevious(),
			dirty.reconciled(),
		)
	}
	return fmt.Sprintf(
		"On %s a statement commits on its own, so nothing records how far this run got: it may "+
			"have run in full, in part, or not at all. Inspect the database and apply whatever is "+
			"missing, then run %s to record it applied. "+
			"%s is safe only once you have confirmed no statement of it ran.",
		dirty.Dialect,
		dirty.reconciled(),
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
				"record the migration applied if you restored what the rollback reverted, or %s "+
				"(version 0 where this is the oldest migration in the directory) if you "+
				"finished the rollback by hand.",
			dirty.reconciled(),
			dirty.setPrevious(),
		)
	}
	return fmt.Sprintf(
		"The migration stopped, and the rollback of its transaction left some statements in place and undid "+
			"others. Inspect the database and apply what is missing, then run %s "+
			"to record the migration applied -- rerunning it could repeat a statement that stayed.",
		dirty.reconciled(),
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
				"applied if you restored what the rollback reverted, or %s "+
				"(version 0 where this is the oldest migration in the "+
				"directory) if you finished the rollback by hand.",
			revision.Applied+1,
			revision.Total,
			dirty.reconciled(),
			dirty.setPrevious(),
		)
	}
	return fmt.Sprintf(
		"The run was interrupted while statement %d of %d was executing, so whether it committed "+
			"was never recorded, and nothing after it ran. Inspect the database and apply what is "+
			"missing, then run %s to record the migration "+
			"applied -- rerunning it could repeat the statement that committed.",
		revision.Applied+1,
		revision.Total,
		dirty.reconciled(),
	)
}
