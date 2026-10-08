package drift

// White-box testing required: drift ignore parsing, the grading of a
// comparison and report formatting are internal command helpers whose edge
// cases are clearer to verify directly than through full live-database command
// execution. An undecided object in particular needs a server that refuses a
// catalog read, which the e2e suite measures once.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/coverage"
	"ptah.run/internal/cli/internal/schemaops"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

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

// TestAssessDriftGradesAnUndecidedObject holds the check to what the
// comparison could not see. A declared role the read was refused the catalog
// of is no difference -- drift stays false -- but the check did not look, so
// it fails at the default threshold. It is a warning, not a destructive
// change, so the destructive threshold lets it through, and turning the exit
// code off passes everything (stokaro/ptah#3844).
func TestAssessDriftGradesAnUndecidedObject(t *testing.T) {
	tests := []struct {
		name        string
		undecided   schemadiff.Diagnostics
		severity    string
		useExitCode bool
		wantFailed  bool
		wantHighest safety.Severity
	}{
		{
			name:        "an undecided role at the default threshold",
			undecided:   undecidedRoles("reporter"),
			severity:    severityAll,
			useExitCode: true,
			wantFailed:  true,
			wantHighest: safety.Warning,
		},
		{
			name:        "an undecided role at the destructive threshold",
			undecided:   undecidedRoles("reporter"),
			severity:    severityDestructive,
			useExitCode: true,
			wantFailed:  false,
			wantHighest: safety.Warning,
		},
		{
			name:        "an undecided role with the exit code off",
			undecided:   undecidedRoles("reporter"),
			severity:    severityAll,
			useExitCode: false,
			wantFailed:  false,
			wantHighest: safety.Warning,
		},
		{
			name:        "nothing withheld",
			undecided:   schemadiff.Diagnostics{},
			severity:    severityAll,
			useExitCode: true,
			wantFailed:  false,
			wantHighest: safety.Safe,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Undecided: test.undecided}

			report := assessDrift(result, test.severity, test.useExitCode, nil)

			c.Assert(report.Drift, qt.IsFalse)
			c.Assert(report.Failed, qt.Equals, test.wantFailed)
			c.Assert(report.HighestSeverity, qt.Equals, test.wantHighest)
			c.Assert(report.Undecided, qt.DeepEquals, test.undecided)
			c.Assert(report.Findings, qt.CmpEquals(cmpopts.EquateEmpty()), undecidednote.Findings(test.undecided))
		})
	}
}

// TestWriteTextReportNamesAnUndecidedObject is the text half: a run that found
// no difference and withheld a role must not say no drift was detected.
func TestWriteTextReportNamesAnUndecidedObject(t *testing.T) {
	c := qt.New(t)
	result := &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Undecided: undecidedRoles("reporter", "auditor")}
	var buf bytes.Buffer

	err := writeReport(&buf, formatText, assessDrift(result, severityAll, true, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Equals, `No schema drift found, but 2 declared objects could not be decided (highest severity: warning).
Failure threshold: all. Failing: true.

Undecided:
- role "auditor"
- role "reporter"

Findings:
- undecided: 2 (warning)
`)
}

// TestWriteGitHubActionsReportAnnotatesAnUndecidedObject is the workflow
// half: the run is not the "no drift" notice, and each withheld object gets
// an annotation of its own.
func TestWriteGitHubActionsReportAnnotatesAnUndecidedObject(t *testing.T) {
	c := qt.New(t)
	result := &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Undecided: undecidedRoles("reporter")}
	var buf bytes.Buffer

	err := writeReport(&buf, formatGitHubActions, assessDrift(result, severityAll, true, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Equals, "::error title=Ptah schema drift::No schema drift found, but 1 declared object"+
		" could not be decided; highest severity: warning; failure threshold: all\n"+
		"::error title=Ptah undecided object::role \"reporter\" could not be checked: the database does not describe role objects because the read was refused the catalog that would have listed them\n"+
		"::error title=Ptah drift finding::undecided: 1 (warning)\n")
}

// TestWriteJSONReportCarriesTheUndecidedObjects is the document half: each
// withheld object with its kind, name, reason and provenance.
func TestWriteJSONReportCarriesTheUndecidedObjects(t *testing.T) {
	c := qt.New(t)
	result := &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Undecided: undecidedRoles("reporter")}
	var buf bytes.Buffer

	err := writeReport(&buf, formatJSON, assessDrift(result, severityAll, true, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Contains, `"drift": false,
  "failed": true,`)
	c.Assert(buf.String(), qt.Contains, `"undecided": {
    "common": [
      {
        "kind": "role",
        "name": "reporter",
        "reason": "not-inspected",
        "provenance": "observed"
      }
    ]
  },`)
}

// undecidedRoles is what the comparison withholds for declared roles a read
// was refused the catalog of.
func undecidedRoles(names ...string) schemadiff.Diagnostics {
	objects := make([]coverage.Object, 0, len(names))
	for _, name := range names {
		object := coverage.Refused(coverage.Role)
		object.Name = name
		objects = append(objects, object)
	}
	return schemadiff.Diagnostics{Common: objects}
}
