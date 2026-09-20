package migrator

import (
	"context"
	"errors"
	"fmt"

	"ptah.run/dbschema"
)

// VerifyStatus is what a verification run established about one assertion.
type VerifyStatus string

const (
	// VerifyStatusVerified reports an assertion that ran and held.
	VerifyStatusVerified VerifyStatus = "verified"
	// VerifyStatusFailed reports an assertion that ran and did not hold. The
	// database answered; the answer was not the one the author required.
	VerifyStatusFailed VerifyStatus = "failed"
	// VerifyStatusErrored reports an assertion that could not run: its text is
	// not a well-formed read-only predicate, or the query failed. Nothing is
	// established about the requirement either way.
	VerifyStatusErrored VerifyStatus = "errored"
)

// VerifyVerdict is the single word a verification run earns as a whole.
type VerifyVerdict string

const (
	// VerifyVerdictVerified reports a run whose every assertion held, and which
	// had at least one.
	VerifyVerdictVerified VerifyVerdict = "verified"
	// VerifyVerdictFailed reports a run in which an assertion did not hold.
	VerifyVerdictFailed VerifyVerdict = "failed"
	// VerifyVerdictErrored reports a run in which an assertion could not run.
	// It outranks a failure deliberately: a run that could not evaluate one of
	// its requirements cannot say the others are the only ones violated.
	VerifyVerdictErrored VerifyVerdict = "errored"
	// VerifyVerdictNotVerified reports a run with no assertions to evaluate.
	// It is not a pass: nothing was asked, so nothing was established.
	VerifyVerdictNotVerified VerifyVerdict = "not verified"
)

// VerifyResult is one assertion's outcome in a verification run.
//
// Err is set only for [VerifyStatusErrored] and carries why the assertion could
// not run. A failed assertion has no error: it ran, and the database said no.
type VerifyResult struct {
	Name   string
	Assert string
	Status VerifyStatus
	Err    error
}

// VerifyReport is the outcome of a verification run, one entry per assertion in
// the order the assertions were given.
//
// The zero value is the report of a run that evaluated nothing, and its
// [VerifyReport.Verdict] is [VerifyVerdictNotVerified] rather than a pass.
type VerifyReport struct {
	Results []VerifyResult
}

// Count returns how many results carry status.
func (r VerifyReport) Count(status VerifyStatus) int {
	total := 0
	for _, result := range r.Results {
		if result.Status == status {
			total++
		}
	}
	return total
}

// Verdict reduces the run to one word.
//
// An assertion that could not run outranks one that failed, and both outrank an
// empty run, so a caller that prints only this word never prints a stronger
// claim than the results support.
func (r VerifyReport) Verdict() VerifyVerdict {
	switch {
	case r.Count(VerifyStatusErrored) > 0:
		return VerifyVerdictErrored
	case r.Count(VerifyStatusFailed) > 0:
		return VerifyVerdictFailed
	case len(r.Results) == 0:
		return VerifyVerdictNotVerified
	default:
		return VerifyVerdictVerified
	}
}

// Verified reports whether the run established that every requirement holds.
// An empty run is not verified: it asked nothing.
func (r VerifyReport) Verified() bool {
	return r.Verdict() == VerifyVerdictVerified
}

// VerifyChecks evaluates checks against an existing database and writes
// nothing to it.
//
// This is the assertion half of a migration's `-- +ptah check` directive with
// the migration taken away: the same parser produces the checks, the same
// static rule decides whether an assertion is a well-formed read-only
// predicate, and the same isolated query session evaluates it. What differs is
// that a migration stops at the first unsatisfied precondition, because there
// is a body it must not run, while a verification run has nothing to protect
// and every requirement is worth an answer. So every check is evaluated and
// every outcome is reported, including the ones after the first failure.
//
// Two rules hold this to reading only. The assertion must be a single SELECT,
// proved from its text before any query is sent, and the session is opened
// read-only wherever the dialect has such a mode. Where it does not, the
// static proof is the whole of the protection, which is the same guarantee a
// pre-migration check already carries on that dialect.
//
// What neither rule reaches is a routine that runs in a transaction of its own:
// an Oracle function declared PRAGMA AUTONOMOUS_TRANSACTION can insert and
// commit while a SELECT calls it, and nothing in a session undoes a transaction
// that was never part of it. An ordinary writing routine is refused before that
// matters -- PostgreSQL by the read-only transaction, Oracle by ORA-14551 for a
// DML operation inside a query. So the guarantee is about what Ptah sends and
// the session it sends it in; an assertion that calls a routine answers for
// what the routine does.
//
// Each assertion is evaluated in a session of its own, so one that the server
// refuses cannot decide the outcome of the next. Sharing a session would be
// cheaper by a round trip per check and would report every assertion after the
// first error as errored on PostgreSQL, whose transaction refuses further
// commands once a statement in it has failed. Nothing is lost by separating
// them: a check is a read, the sessions are read-only, and read committed --
// the level these open at -- gives a statement its own snapshot inside one
// transaction anyway.
//
// A returned error means the run itself could not happen -- no session, no
// answers. Assertion-level failures are reported in the [VerifyReport] and are
// not errors here: a report that says which requirement was violated is the
// point of the verb.
func VerifyChecks(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	checks []Check,
) (VerifyReport, error) {
	if conn == nil {
		return VerifyReport{}, fmt.Errorf("verification requires an open database connection")
	}
	if len(checks) == 0 {
		return VerifyReport{}, nil
	}

	info := conn.Info()
	report := VerifyReport{Results: make([]VerifyResult, 0, len(checks))}
	for _, check := range checks {
		result, err := verifyOneCheck(ctx, conn, info.Dialect, info.Version, check)
		if err != nil {
			return VerifyReport{}, err
		}
		report.Results = append(report.Results, result)
	}
	return report, nil
}

// verifyOneCheck opens one read-only session and evaluates one assertion in it.
//
// The returned error is the run's, not the assertion's: failing to open a
// session, failing to undo one, or having the context end under a statement
// says nothing about the requirement and is not an outcome to report.
// Everything the assertion itself can do -- run and hold, run and not hold, or
// fail to run at all -- comes back in the result.
func verifyOneCheck(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect,
	serverVersion string,
	check Check,
) (VerifyResult, error) {
	result := VerifyResult{Name: check.Name, Assert: check.Assert}
	var assertionErr error
	sessionErr := conn.WithIsolatedQuerySession(
		ctx,
		checkTransactionOptions(dialect),
		func(queryer dbschema.IsolatedQueryer) error {
			value, err := runCheckAssertion(ctx, queryer, dialect, serverVersion, check.Assert)
			if err != nil {
				if cause := context.Cause(ctx); cause != nil {
					// The run ended, rather than the assertion answering. A
					// cancelled or expired context says nothing about the
					// requirement, and recording it as an outcome tells a
					// caller the database was asked and answered. The driver's
					// own error is kept beside the cause so a reader sees
					// which statement was in flight.
					return errors.Join(err, cause)
				}
				result.Status = VerifyStatusErrored
				result.Err = err
				assertionErr = err
				// Returned so the session ends rather than commits over a
				// statement the server refused; the outcome is already
				// recorded, and the caller reports it rather than the wrapper.
				return err
			}
			if assertionPassed(value) {
				result.Status = VerifyStatusVerified
				return nil
			}
			result.Status = VerifyStatusFailed
			return nil
		},
	)
	if failure := sessionFailure(sessionErr, assertionErr); failure != nil {
		return VerifyResult{}, failure
	}
	return result, nil
}

// sessionFailure returns what the session reported apart from the assertion
// error the callback raised on purpose.
//
// The wrapper joins that error with a rollback or discard failure, so asking
// whether the assertion error is in there answers yes while a second error
// sits beside it. What is left after removing the assertion error is the
// isolation failing -- the rollback that keeps the read from touching the
// database, or the discard that keeps its session state out of the pool -- and
// a report that says verified over one of those is a report about a database
// nobody proved was left alone.
func sessionFailure(sessionErr, assertionErr error) error {
	if sessionErr == nil {
		return nil
	}
	joined, isJoined := sessionErr.(interface{ Unwrap() []error })
	if !isJoined {
		if assertionErr != nil && errors.Is(sessionErr, assertionErr) {
			return nil
		}
		return sessionErr
	}
	rest := make([]error, 0, 1)
	for _, err := range joined.Unwrap() {
		if assertionErr != nil && errors.Is(err, assertionErr) {
			continue
		}
		rest = append(rest, err)
	}
	return errors.Join(rest...)
}
