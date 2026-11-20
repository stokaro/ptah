package schema_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/schema"
)

// TestSchemaSecurity_FailurePath_ReachThresholds refuses a reach threshold
// before connecting: the address names a port nothing listens on, so a refusal
// that came after the connection would read as a connection error instead.
func TestSchemaSecurity_FailurePath_ReachThresholds(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "a warning threshold above the error one", args: []string{"--reach-warn", "80", "--reach-error", "60"},
			wantErr: `--reach-warn \(80\) is above --reach-error \(60\)`},
		{name: "an error threshold above 100", args: []string{"--reach-error", "101"},
			wantErr: `--reach-error must be a percentage from 0 to 100, got 101`},
		{name: "a negative warning threshold", args: []string{"--reach-warn", "-1"},
			wantErr: `--reach-warn must be a percentage from 0 to 100, got -1`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := schema.NewSchemaSecurityCommand()
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetContext(c.Context())
			cmd.SetArgs(append([]string{"--db-url", "postgres://ptah@127.0.0.1:1/none?sslmode=disable"}, test.args...))

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(stdout.String(), qt.Equals, "")
		})
	}
}
