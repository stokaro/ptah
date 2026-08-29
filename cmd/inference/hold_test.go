package inference_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/inference"
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

// TestHoldVerbs_RefuseARunNobodyNamed keeps both verbs from acting on a run
// identifier they were never given.
func TestHoldVerbs_RefuseARunNobodyNamed(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args []string
	}{
		{name: "pause", verb: "pause", args: []string{"--reason", "waiting on a budget approval"}},
		{name: "resume", verb: "resume"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := runHoldVerb(t, test.verb, append([]string{
				"--spec", "no-such-spec.yaml",
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
