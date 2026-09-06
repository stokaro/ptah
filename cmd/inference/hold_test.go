package inference_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/inference"
)

// TestPause_RefusesAReasonlessPauseBeforeConnecting is what the CLI's own check
// buys over the one in the run.
//
// `Run.Pause` refuses an empty reason too, so removing this one still refuses
// the pause -- after opening the specification and connecting to the database.
// Two answers to one question, and only one of them is measurable: this asserts
// the half that is, by naming a database nothing is listening on and requiring
// the refusal to be about the flag rather than about the connection.
func TestPause_RefusesAReasonlessPauseBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "pause",
		"--spec", "no-such-spec.yaml",
		"--db-url", "postgres://127.0.0.1:1/nothing",
		"--run-id", "run-1")

	c.Assert(err, qt.ErrorMatches, `--reason is required`)
}

// TestPause_ReachesTheDatabaseOnceItHasAReason is the control.
//
// Without it a check written to refuse every pause would pass the test above.
// The address is one nothing answers on, so what this asserts is that the run
// got past the flags -- the error is about the specification the flags named.
func TestPause_ReachesTheDatabaseOnceItHasAReason(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "pause",
		"--spec", "no-such-spec.yaml",
		"--db-url", "postgres://127.0.0.1:1/nothing",
		"--run-id", "run-1", "--reason", "waiting on a budget approval")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "no-such-spec.yaml")
}

// TestAbandon_RefusesAReasonlessAbandonmentBeforeConnecting keeps the reason
// requirement at the command boundary. The verb does not need a specification,
// so the unreachable database is the first external resource it could touch.
func TestAbandon_RefusesAReasonlessAbandonmentBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "abandon",
		"--db-url", "postgres://127.0.0.1:1/nothing",
		"--run-id", "run-1")

	c.Assert(err, qt.ErrorMatches, `--reason is required`)
}

// TestAbandon_NeedsNoSpecification is the operator-facing distinction from
// pause and resume. The run row already identifies its generation and source,
// so requiring the old specification would make a superseded run need the
// artifact the operator is explicitly leaving behind.
func TestAbandon_NeedsNoSpecification(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "abandon",
		"--db-url", "postgres://127.0.0.1:1/nothing",
		"--run-id", "run-1", "--reason", "the migration was superseded")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "connect to postgres://127.0.0.1:1/nothing")
	c.Assert(err.Error(), qt.Not(qt.Contains), "--spec or --release")
}

// TestAbandon_DoesNotAcceptASpecification prevents the generated command
// reference from teaching an unnecessary dependency by accident.
func TestAbandon_DoesNotAcceptASpecification(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "abandon", "--spec", "obsolete.yaml")

	c.Assert(err, qt.ErrorMatches, `unknown flag: --spec`)
}

// TestAbandon_DoesNotExposeAWorkerFlag keeps the operator contract about the
// run being ended. The atomic store transition fences the old worker; there is
// no new lease owner to name or persist.
func TestAbandon_DoesNotExposeAWorkerFlag(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "abandon", "--worker", "operator")

	c.Assert(err, qt.ErrorMatches, `unknown flag: --worker`)
}

// TestAbandon_RefusesPositionalArgumentsBeforeConnecting prevents a mistyped
// run identifier from being ignored by an irreversible run-state transition.
func TestAbandon_RefusesPositionalArgumentsBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	_, err := runHoldVerb(t, "abandon", "accidental-positional",
		"--db-url", "postgres://127.0.0.1:1/nothing",
		"--run-id", "run-1", "--reason", "the migration was superseded")

	c.Assert(err, qt.ErrorMatches, `unknown command "accidental-positional" for "inference abandon"`)
}

// TestHoldVerbs_RefuseARunNobodyNamed keeps both verbs from acting on a run
// identifier they were never given.
func TestHoldVerbs_RefuseARunNobodyNamed(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args []string
	}{
		{name: "pause", verb: "pause", args: []string{
			"--spec", "no-such-spec.yaml", "--reason", "waiting on a budget approval",
		}},
		{name: "resume", verb: "resume", args: []string{"--spec", "no-such-spec.yaml"}},
		{name: "abandon", verb: "abandon", args: []string{
			"--reason", "the migration was superseded",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := runHoldVerb(t, test.verb, append([]string{
				"--db-url", "postgres://127.0.0.1:1/nothing",
			}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, `--run-id is required`)
		})
	}
}

// runHoldVerb drives the real command tree.
func runHoldVerb(t *testing.T, verb string, args ...string) (string, error) {
	t.Helper()
	cmd := inference.NewCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(append([]string{verb}, args...))
	err := cmd.ExecuteContext(context.Background())
	return output.String(), err
}
