package capabilityprobe

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/core/platform/capability"
)

// observation is what one experiment concluded about one capability key.
type observation struct {
	// does is what the server does. It is meaningful only when undecidable is
	// empty.
	does bool
	// undecidable, when non-empty, is why this run did not establish what the
	// server does. An observation with a reason NEVER becomes an agreement.
	undecidable string
	// note is context that belongs on the row even when it was decided —
	// typically that the key names one dialect's spelling of an ability other
	// dialects also have under a different one.
	note string
}

func decided(does bool) observation                { return observation{does: does} }
func annotated(does bool, note string) observation { return observation{does: does, note: note} }
func cannotDecide(format string, args ...any) observation {
	return observation{undecidable: fmt.Sprintf(format, args...)}
}

// verdicts maps each key an experiment decides to its observation.
type verdicts map[capability.Capability]observation

// decider observes a live session and reports one observation per key the
// experiment decides, together with every statement it ran.
type decider func(ctx context.Context, s *session) (verdicts, []Attempt)

// experiment is one measurement against a live server.
//
// It is not "one capability, one statement". Three of the twenty-four keys are
// a mutual-exclusion group decided by two statements, and running them as
// three independent experiments would let the group report a combination
// Validate rejects. Two more need a DML follow-up because DDL acceptance is
// identical on both sides of the boundary the key names.
type experiment struct {
	// decides lists every key this experiment answers.
	decides []capability.Capability

	// requires lists keys that must already have been decided TRUE. When one
	// was not, every key here is undecidable rather than false: the
	// registry's own edge says DropConstraintIfExists presupposes
	// DropConstraintGeneric, so on a server without the generic clause the
	// guarded statement is refused for the clause, not for the guard, and
	// scoring it false would answer a question the run never asked.
	requires []capability.Capability

	// setup must succeed in full. A refused setup statement makes every key
	// in decides undecidable: a deciding statement whose precondition was
	// never created has observed the missing precondition, not the
	// capability.
	setup []string

	decide decider
}

// plan is one dialect's complete answer for the capability registry: every
// registered key is either decided by an experiment here or declared
// undecidable with its reason.
//
// The exactly-once rule is enforced by a test, so a key added to the registry
// turns the build red until this file answers for it. Without that, a new
// capability would silently be absent from every matrix row and the report
// would still say the run was complete.
type plan struct {
	experiments []experiment
	// undecided declares, in advance and as data, keys this dialect cannot
	// decide by asking the server. A reason recorded here is a property of the
	// dialect that was known before the run; it is never derived from an
	// outcome somebody did not like.
	undecided map[capability.Capability]string
}

// acceptance decides one key by whether the server accepts a single statement.
func acceptance(key capability.Capability, setup []string, statement string) experiment {
	return acceptanceNote(key, setup, statement, "")
}

// acceptanceNote is acceptance with a note carried onto the row.
func acceptanceNote(key capability.Capability, setup []string, statement, note string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempt := s.exec(ctx, statement)
			return verdicts{key: annotated(attempt.Accepted, note)}, []Attempt{attempt}
		},
	}
}

// all decides one key by whether the server accepts every statement in a
// sequence. It stops at the first refusal, so the evidence names the statement
// that answered.
func all(key capability.Capability, setup []string, statements ...string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempts, ok := s.runAll(ctx, statements)
			return verdicts{key: decided(ok)}, attempts
		},
	}
}

// guarded decides an IF EXISTS-style key.
//
// Acceptance of the guarded statement alone does not isolate the guard: it
// would also pass on a server that accepts a drop of an absent object outright.
// So the unguarded spelling of the same statement must be REFUSED. When both
// are accepted the key is undecidable, with that stated, rather than recorded
// as support the run did not separate from permissiveness.
func guarded(key capability.Capability, setup []string, guardedStmts []string, unguarded string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			attempts, accepted := s.runAll(ctx, guardedStmts)
			control := s.exec(ctx, unguarded)
			attempts = append(attempts, control)
			if accepted && control.Accepted {
				return verdicts{key: cannotDecide(
					"the server also accepted the unguarded spelling %q against an absent object, "+
						"so accepting the IF EXISTS form does not separate the guard from general permissiveness",
					collapse(unguarded),
				)}, attempts
			}
			return verdicts{key: decided(accepted)}, attempts
		},
	}
}

// enforced decides a key whose statement the server may accept and then
// ignore.
//
// MySQL before 8.0.16 accepted a CHECK clause and did not enforce it, which is
// the entire reason CheckConstraintsEnforced exists: DDL acceptance is byte
// for byte the same on both sides of that boundary. So the deciding statement
// is one the constraint must REFUSE, and the control is one it must ACCEPT —
// without the control, a table that rejects every insert for an unrelated
// reason would read as an enforced constraint.
func enforced(key capability.Capability, setup []string, accept, reject string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			control := s.exec(ctx, accept)
			violation := s.exec(ctx, reject)
			attempts := []Attempt{control, violation}
			if !control.Accepted {
				return verdicts{key: cannotDecide(
					"the control row %q was refused too (%s), so a refusal does not show the constraint is enforced",
					collapse(accept), collapse(control.ServerErr),
				)}, attempts
			}
			return verdicts{key: decided(!violation.Accepted)}, attempts
		},
	}
}

// concurrentIndex decides one of the CONCURRENTLY keys.
//
// Acceptance is the wrong test on its own. CockroachDB parses the keyword as a
// compatibility no-op, so an acceptance probe records support the engine does
// not provide. Real PostgreSQL refuses the statement inside an explicit
// transaction block — a build that cannot be rolled back cannot live in one —
// and a parser that merely swallows the word has no reason to refuse. The
// transaction block is therefore the discriminator, and it runs only after the
// standalone form was accepted.
func concurrentIndex(key capability.Capability, setup []string, standalone, insideTx string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			first := s.exec(ctx, standalone)
			attempts := []Attempt{first}
			if !first.Accepted {
				return verdicts{key: decided(false)}, attempts
			}

			inTx, started := s.tryInTransaction(ctx, insideTx)
			attempts = append(attempts, inTx...)
			if !started {
				return verdicts{key: cannotDecide(
					"the server accepted %q but refused BEGIN (%s), so the transaction-block discriminator "+
						"that separates a real concurrent build from a parsed no-op could not run",
					collapse(standalone), collapse(inTx[0].ServerErr),
				)}, attempts
			}
			if second := inTx[1]; second.Accepted {
				return verdicts{key: annotated(false,
					"the server accepted the statement inside an explicit transaction block as well, "+
						"so the keyword is parsed and dropped rather than changing how the index is built",
				)}, attempts
			}
			return verdicts{key: decided(true)}, attempts
		},
	}
}

// storedResult decides MaterializedViews.
//
// MySQL 9.7.1 accepts CREATE MATERIALIZED VIEW, reports the object as a plain
// VIEW, and recomputes the query on every read. This key names a view whose
// result is STORED, so the decider inserts a row into the source table between
// two reads: a stored result does not move, a recomputed one does.
func storedResult(key capability.Capability, setup []string, create, read, mutate string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			created := s.exec(ctx, create)
			attempts := []Attempt{created}
			if !created.Accepted {
				return verdicts{key: decided(false)}, attempts
			}
			before, readBefore := s.query(ctx, read)
			mutated := s.exec(ctx, mutate)
			after, readAfter := s.query(ctx, read)
			attempts = append(attempts, readBefore, mutated, readAfter)
			if !readBefore.Accepted || !mutated.Accepted || !readAfter.Accepted {
				return verdicts{key: cannotDecide(
					"the statement was accepted but the storedness check could not run, " +
						"so acceptance of the keyword is all this run saw",
				)}, attempts
			}
			if before == after {
				return verdicts{key: decided(true)}, attempts
			}
			return verdicts{key: annotated(false, fmt.Sprintf(
				"the keyword was accepted and then dropped: the view reported %d before the source row was "+
					"inserted and %d after, so the result is recomputed rather than stored", before, after,
			))}, attempts
		},
	}
}
