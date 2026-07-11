package drift

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/safety"
)

func TestNewDriftCommand_Creation(t *testing.T) {
	c := qt.New(t)

	cmd := NewDriftCommand()

	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "drift")
	c.Assert(cmd.Short, qt.Contains, "drift")
}

func TestRunDrift_MissingDatabaseURLReturnsCode2(t *testing.T) {
	c := qt.New(t)

	cmd := NewDriftCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetArgs([]string{"--root-dir", "."})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Contains, "database URL is required")
}

func TestParseIgnoredTables(t *testing.T) {
	c := qt.New(t)

	tables, err := parseIgnoredTables([]string{"tables=audit_log,sessions", "tables= audit_log , events "})

	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.DeepEquals, []string{"audit_log", "events", "sessions"})
}

func TestParseIgnoredTablesRejectsUnknownScope(t *testing.T) {
	c := qt.New(t)

	_, err := parseIgnoredTables([]string{"views=audit_view"})

	c.Assert(err, qt.ErrorMatches, `invalid --ignore value "views=audit_view": expected tables=name\[,name\.\.\.\]`)
}

func TestShouldFailDrift(t *testing.T) {
	c := qt.New(t)

	c.Assert(shouldFailDrift(safety.Warning, severityAll), qt.IsTrue)
	c.Assert(shouldFailDrift(safety.Warning, severityDestructive), qt.IsFalse)
	c.Assert(shouldFailDrift(safety.Destructive, severityDestructive), qt.IsTrue)
}

func TestWriteGitHubActionsReport(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	err := writeGitHubActionsReport(&buf, driftReport{
		Drift:            true,
		Failed:           true,
		FailureThreshold: severityDestructive,
		HighestSeverity:  safety.Destructive,
		Findings: []safety.Finding{
			{Category: "tables_removed", Count: 1, Severity: safety.Destructive},
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Contains, "::error title=Ptah schema drift::")
	c.Assert(buf.String(), qt.Contains, "tables_removed: 1")
}

func TestWriteJSONReport(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	err := writeReport(&buf, formatJSON, driftReport{
		Drift:            true,
		Failed:           false,
		FailureThreshold: severityDestructive,
		HighestSeverity:  safety.Warning,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Contains, `"drift": true`)
	c.Assert(buf.String(), qt.Contains, `"highest_severity": "warning"`)
}
