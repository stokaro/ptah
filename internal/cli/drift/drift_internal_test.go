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

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/internal/cli/internal/schemaops"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
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
	err := writeReport(t.Context(), &buf, formatJSON, driftReport{
		Drift:            true,
		Failed:           false,
		FailureThreshold: severityDestructive,
		HighestSeverity:  safety.Warning,
	}, schemaext.Registry{})

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

	err := writeReport(t.Context(), &buf, formatText, assessDrift(result, severityAll, true, nil), schemaext.Registry{})

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

	err := writeReport(t.Context(), &buf, formatGitHubActions, assessDrift(result, severityAll, true, nil), schemaext.Registry{})

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

	err := writeReport(t.Context(), &buf, formatJSON, assessDrift(result, severityAll, true, nil), schemaext.Registry{})

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

// resultHolding is a comparison that found nothing, over a read that holds
// objects of a kind nothing compares.
func resultHolding(objects ...catalog.UnmodeledObject) *schemaops.CompareResult {
	return &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Database: &catalog.Database{UnmodeledObjects: objects}}
}

var (
	unmodeledEvent = catalog.UnmodeledObject{Kind: "event", Name: "e_tick", Reason: "nothing compares it"}
	unlistedEvents = catalog.UnmodeledObject{Kind: "event", Reason: "the account cannot list them"}
)

// TestAssessDriftLeavesTheAnswerAloneOverAnObjectItDidNotCompare holds the exit
// code in place: an object of a kind no schema source can declare is not a
// difference, and a check that failed on one would have no remedy short of
// dropping it. The report names it, and that is all it does.
func TestAssessDriftLeavesTheAnswerAloneOverAnObjectItDidNotCompare(t *testing.T) {
	tests := []struct {
		name     string
		object   catalog.UnmodeledObject
		severity string
	}{
		{name: "an event the read found", object: unmodeledEvent, severity: severityAll},
		{name: "an event the read found, at the destructive threshold", object: unmodeledEvent, severity: severityDestructive},
		{name: "events the account cannot list", object: unlistedEvents, severity: severityAll},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := assessDrift(resultHolding(test.object), test.severity, true, nil)

			c.Assert(report.Drift, qt.IsFalse)
			c.Assert(report.Failed, qt.IsFalse)
			c.Assert(report.Findings, qt.HasLen, 0)
			c.Assert(report.HighestSeverity, qt.Equals, safety.Safe)
			c.Assert(report.NotCompared, qt.DeepEquals, []catalog.UnmodeledObject{test.object})
		})
	}
}

// TestWriteReportNamesWhatItDidNotCompare is each format of each report that has
// something to say, and the control that has nothing.
func TestWriteReportNamesWhatItDidNotCompare(t *testing.T) {
	const database = "Failure threshold: all. Failing: false.\n"
	tests := []struct {
		name   string
		format string
		result *schemaops.CompareResult
		want   string
	}{
		{
			name:   "text, an event the read found",
			format: formatText,
			result: resultHolding(unmodeledEvent),
			want: "No schema drift found, but 1 event was not compared.\n" + database +
				"\nNot compared:\n- event \"e_tick\": nothing compares it\n",
		},
		{
			name:   "text, events the account cannot list",
			format: formatText,
			result: resultHolding(unlistedEvents),
			want: "No schema drift found, but events were not compared.\n" + database +
				"\nNot compared:\n- events: the account cannot list them\n",
		},
		{
			name:   "text, a read that found none",
			format: formatText,
			result: resultHolding(),
			want:   "No schema drift detected.\n",
		},
		{
			name:   "workflow annotations, an event the read found",
			format: formatGitHubActions,
			result: resultHolding(unmodeledEvent),
			want: "::warning title=Ptah schema drift::No schema drift found, but 1 event was not compared;" +
				" highest severity: safe; failure threshold: all\n" +
				"::warning title=Ptah object not compared::event \"e_tick\": nothing compares it\n",
		},
		{
			name:   "workflow annotations, a read that found none",
			format: formatGitHubActions,
			result: resultHolding(),
			want:   "::notice title=Ptah drift check::No schema drift detected\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var buf bytes.Buffer

			err := writeReport(t.Context(), &buf, test.format, assessDrift(test.result, severityAll, true, nil), schemaext.Registry{})

			c.Assert(err, qt.IsNil)
			c.Assert(buf.String(), qt.Equals, test.want)
		})
	}
}

// TestWriteJSONReportNamesWhatItDidNotCompare is the document: a named entry, with
// no kind of its own among the findings, and absent where there is nothing to
// name.
func TestWriteJSONReportNamesWhatItDidNotCompare(t *testing.T) {
	c := qt.New(t)
	var buf bytes.Buffer

	err := writeReport(t.Context(), &buf, formatJSON, assessDrift(resultHolding(unmodeledEvent), severityAll, true, nil), schemaext.Registry{})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Contains, `"drift": false,
  "failed": false,`)
	c.Assert(buf.String(), qt.Contains, `"not_compared": [
    {
      "kind": "event",
      "name": "e_tick",
      "reason": "nothing compares it"
    }
  ],`)

	buf.Reset()
	err = writeReport(t.Context(), &buf, formatJSON, assessDrift(resultHolding(), severityAll, true, nil), schemaext.Registry{})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Not(qt.Contains), "not_compared")
}

// TestWriteTextReportNamesWhatItDidNotCompareBesideADifference is a report with
// both answers, neither of which hides the other.
func TestWriteTextReportNamesWhatItDidNotCompareBesideADifference(t *testing.T) {
	c := qt.New(t)
	result := resultHolding(unmodeledEvent)
	result.Diff = &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "orphan"}}}
	var buf bytes.Buffer

	err := writeReport(t.Context(), &buf, formatText, assessDrift(result, severityAll, true, nil), schemaext.Registry{})

	c.Assert(err, qt.IsNil)
	c.Assert(buf.String(), qt.Equals, `Schema drift detected (highest severity: destructive).
Failure threshold: all. Failing: true.

Not compared:
- event "e_tick": nothing compares it

Findings:
- tables_removed: 1 (destructive)
`)
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
