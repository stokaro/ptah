package inference_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/inference"
)

// TestVerify_RefusesTwoDestinationsForOneRecord covers the flag pair that would
// otherwise be answered by whichever branch ran.
//
// A referrer lands in its subject's repository, so --attach-to already decides
// where the record goes. A run naming --publish-evidence as well said it twice,
// and the registry would have obeyed one of them without telling anybody which.
//
// It is refused before any work: the run has not opened a database, so nothing
// here needs a server.
func TestVerify_RefusesTwoDestinationsForOneRecord(t *testing.T) {
	tests := []struct {
		name string
		verb string
	}{
		{name: "verify", verb: "verify"},
		{name: "cutover", verb: "cutover"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := runInferenceVerb(t, test.verb,
				"--publish-evidence", "oci://registry.invalid/evidence:report",
				"--attach-to", "oci://registry.invalid/evidence:release")

			c.Assert(err, qt.ErrorMatches,
				`if any flags in the group \[publish-evidence attach-to\] are set none of the others can be; .*`)
		})
	}
}

// TestPlan_TakesNoSubject is the other half. A release is what the other records
// attach TO, so the verb that writes one has no subject of its own, and
// offering the flag there would invite a release attached to a release.
func TestPlan_TakesNoSubject(t *testing.T) {
	c := qt.New(t)

	_, err := runInferenceVerb(t, "plan", "--attach-to", "oci://registry.invalid/evidence:release")

	c.Assert(err, qt.ErrorMatches, `unknown flag: --attach-to`)
}

// runInferenceVerb drives the real command tree far enough to parse flags.
func runInferenceVerb(t *testing.T, verb string, args ...string) (string, error) {
	t.Helper()
	cmd := inference.NewCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(append([]string{verb}, args...))
	err := cmd.ExecuteContext(context.Background())
	return output.String(), err
}
