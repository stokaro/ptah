package migrator

import (
	"context"
	"errors"

	"ptah.run/dbschema"
	"ptah.run/internal/execoutcome"
	"ptah.run/internal/migratorext"
)

// rollbackLeftUnknownError is what a revision row records in place of the failure
// when the rollback of the body's transaction left some statements in place and
// no count of leading statements describes them.
const rollbackLeftUnknownError = "the rollback of the migration's transaction left some statements in place and undid others"

// rollbackLeftUnknownFailure is a failure whose rollback left statements in place
// that the revision row cannot count. See [rollbackLeftUnknownError].
type rollbackLeftUnknownFailure struct {
	err error
}

func (e *rollbackLeftUnknownFailure) Error() string { return e.err.Error() }

func (e *rollbackLeftUnknownFailure) Unwrap() error { return e.err }

// bodyStage is how far a migration body got when its transaction stopped.
type bodyStage uint8

const (
	// bodyNotStarted means no statement of the body was sent.
	bodyNotStarted bodyStage = iota
	// bodyStopped means the failure itself says where the body stopped.
	bodyStopped
	// bodyFinished means every statement of the body ran.
	bodyFinished
)

// rollbackEvidence gathers what an engine's [migratorext.RollbackSurvivors] reads
// about a body that is about to be rolled back. It has to run before the rollback
// is sent, because the rollback ends the transaction whatever had happened to it,
// and it sends nothing. An engine with no such service gets the zero value.
func (m *Migrator) rollbackEvidence(
	ctx context.Context,
	session *dbschema.DatabaseConnection,
	cause error,
	sqlText string,
	stage bodyStage,
) migratorext.RollbackEvidence {
	if m.services().RollbackSurvivors == nil {
		return migratorext.RollbackEvidence{}
	}
	statements := splitSQLStatementsForConnection(m.conn, sqlText)
	ran, unanswered := statementsRan(cause, statements, migrationResumeFrom(ctx), stage)
	outside, pinned := session.SessionOutsideTransaction(ctx)
	return migratorext.RollbackEvidence{
		Ran:                ran,
		Unanswered:         unanswered,
		SessionKnown:       pinned,
		OutsideTransaction: outside,
	}
}

// statementsRan splits the statements of a body at the point it stopped: the
// statements that ran and the server answered for, and the ones that may have run
// with no answer. resumeFrom is the 1-based index this attempt started at.
//
// A statement the server refused is in neither list: a refusal is an answer, and
// the statement did nothing. A refusal raised before any statement was sent names
// a statement too, and leaves both lists empty. A failure that names no statement
// leaves the position open, so every statement of the attempt may have run.
func statementsRan(cause error, statements []string, resumeFrom int, stage bodyStage) (ran, unanswered []string) {
	start := min(max(resumeFrom-1, 0), len(statements))
	attempt := statements[start:]
	switch stage {
	case bodyNotStarted:
		return nil, nil
	case bodyFinished:
		return attempt, nil
	case bodyStopped:
	}
	if observed, ok := errors.AsType[*StatementObservationError](cause); ok {
		return statements[start:max(start, min(observed.Event.Index, len(statements)))], nil
	}
	failed, ok := errors.AsType[*MigrationExecutionError](cause)
	if !ok {
		return nil, attempt
	}
	if !failed.sent {
		return nil, nil
	}
	before := max(start, min(failed.StatementIndex-1, len(statements)))
	ran = statements[start:before]
	if answerNeverArrived(failed.Err) {
		return ran, statements[before:min(before+1, len(statements))]
	}
	return ran, nil
}

// answerNeverArrived reports that a statement failed with no answer from the
// server, so it may have run.
func answerNeverArrived(err error) bool {
	return execoutcome.Unknown(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// withRollbackSurvivors returns failure with what the rollback left of the body in
// place, for the revision row: how many leading statements stay, or that no count
// describes them. A rollback that took everything leaves failure as it is.
func (m *Migrator) withRollbackSurvivors(ctx context.Context, failure error, evidence migratorext.RollbackEvidence) error {
	survivors := m.services().RollbackSurvivors
	if survivors == nil {
		return failure
	}
	readCtx, cancelRead := durableRevisionWriteContext(ctx)
	defer cancelRead()
	kept, known := survivors.Kept(readCtx, m.conn, evidence)
	switch {
	case !known:
		return &rollbackLeftUnknownFailure{err: failure}
	case kept > 0:
		return &committedProgressFailure{err: failure, applied: migrationAppliedFloor(ctx) + kept}
	default:
		return failure
	}
}

// batchRollbackEvidence gathers the evidence for one transaction that holds a
// whole run: the statements of the migrations that finished in it and of the one
// it stopped at. Each migration starts where its own plan says.
func (m *Migrator) batchRollbackEvidence(
	ctx context.Context,
	session *dbschema.DatabaseConnection,
	finished []*Migration,
	plans map[string]upRetryPlan,
	current *Migration,
	cause error,
	stage bodyStage,
) migratorext.RollbackEvidence {
	if m.services().RollbackSurvivors == nil {
		return migratorext.RollbackEvidence{}
	}
	evidence := m.rollbackEvidence(ctx, session, cause, current.UpSQL, stage)
	var ran []string
	for _, migration := range finished {
		statements := splitSQLStatementsForConnection(m.conn, migration.UpSQL)
		start := max(plans[migration.RevisionVersion()].resumeFrom-1, 0)
		ran = append(ran, statements[min(start, len(statements)):]...)
	}
	evidence.Ran = append(ran, evidence.Ran...)
	return evidence
}

// batchLeftStatements reports whether anything stayed after the transaction of a
// whole run rolled back, or whether that cannot be told. The revision row belongs
// to one migration and what stayed can belong to several, so no count on that row
// describes it.
func (m *Migrator) batchLeftStatements(ctx context.Context, evidence migratorext.RollbackEvidence) bool {
	survivors := m.services().RollbackSurvivors
	if survivors == nil {
		return false
	}
	readCtx, cancelRead := durableRevisionWriteContext(ctx)
	defer cancelRead()
	kept, known := survivors.Kept(readCtx, m.conn, evidence)
	return !known || kept > 0
}
