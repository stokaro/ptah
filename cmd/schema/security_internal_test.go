package schema

// White-box testing required: the threshold rule and the report writers are
// unexported, and exporting them would add public API that exists only to be
// read here.

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/schemasecurity"
	"go.5x5.cz/ptah/migration/risk"
)

// TestSecurityReportFails pins which threshold acts on which report.
//
// The default is `error`, which no rule in this release can produce, so the
// verb reports and does not fail. That is the interesting row: a default that
// silently gated on info would turn every advisory finding into a red build the
// day this shipped.
func TestSecurityReportFails(t *testing.T) {
	tests := []struct {
		name    string
		report  schemasecurity.Report
		failOn  string
		wantErr bool
	}{
		{
			name:    "the default does not fail on an advisory finding",
			report:  reportWith(schemasecurity.Summary{Info: 3}, 3),
			failOn:  securityFailOnError,
			wantErr: false,
		},
		{
			name:    "the default does not fail on a warning either",
			report:  reportWith(schemasecurity.Summary{Warning: 2}, 2),
			failOn:  securityFailOnError,
			wantErr: false,
		},
		{
			name:    "the default fails on an error-severity finding",
			report:  reportWith(schemasecurity.Summary{Error: 1}, 1),
			failOn:  securityFailOnError,
			wantErr: true,
		},
		{
			name:    "any fails on the advisory finding the default ignores",
			report:  reportWith(schemasecurity.Summary{Info: 1}, 1),
			failOn:  securityFailOnAny,
			wantErr: true,
		},
		{
			name:    "any passes a clean report",
			report:  reportWith(schemasecurity.Summary{}, 0),
			failOn:  securityFailOnAny,
			wantErr: false,
		},
		{
			name:    "none passes a report that would block",
			report:  reportWith(schemasecurity.Summary{Error: 4}, 4),
			failOn:  securityFailOnNone,
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(securityReportFails(test.report, test.failOn), qt.Equals, test.wantErr)
		})
	}
}

// TestValidateSecurityFailOn pins that an unknown threshold is refused rather
// than treated as the default.
//
// Silently falling back would gate on something the operator did not ask for,
// in whichever direction the default happens to point.
func TestValidateSecurityFailOn(t *testing.T) {
	tests := []struct {
		name    string
		failOn  string
		wantErr string
	}{
		{name: "error", failOn: securityFailOnError, wantErr: ""},
		{name: "any", failOn: securityFailOnAny, wantErr: ""},
		{name: "none", failOn: securityFailOnNone, wantErr: ""},
		{
			name:   "a threshold nothing understands",
			failOn: "critical",
			// The message names every accepted value, because an operator who
			// guessed once will guess again without the list.
			wantErr: `--fail-on must be error, any or none, got "critical"`,
		},
		{name: "the empty string", failOn: "", wantErr: `--fail-on must be error, any or none, got ""`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(errorMessage(validateSecurityFailOn(test.failOn)), qt.Equals, test.wantErr)
		})
	}
}

// TestWriteSecurityReport_SaysWhatDidNotRun is the control on a clean report.
//
// "No security findings" and "the rule that would have found them did not run"
// are different answers, and a report that prints only the first is the failure
// mode this whole verb exists to avoid.
func TestWriteSecurityReport_SaysWhatDidNotRun(t *testing.T) {
	c := qt.New(t)
	report := schemasecurity.Report{
		Findings: make([]schemasecurity.Finding, 0),
		SkippedRules: []schemasecurity.SkippedRule{
			{Code: "PRV01", Reason: "the target does not model row-level security"},
		},
	}

	var out bytes.Buffer
	err := writeSecurityReport(&out, "table", report)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "No security findings.")
	c.Assert(out.String(), qt.Contains, "Not checked here:")
	c.Assert(out.String(), qt.Contains, "PRV01: the target does not model row-level security")
}

// TestWriteSecurityReport_JSONCarriesEveryFieldTheTableShows pins the machine
// form, which is the one a pipeline reads.
func TestWriteSecurityReport_JSONCarriesEveryFieldTheTableShows(t *testing.T) {
	c := qt.New(t)
	report := schemasecurity.Report{
		Findings: []schemasecurity.Finding{{
			Code:       "PRV03",
			Severity:   risk.Warning,
			Subject:    schemasecurity.Subject{Kind: "table", Name: "audit_log"},
			Message:    "privileges on table audit_log are granted to PUBLIC",
			Detail:     schemasecurity.Detail{Privileges: []string{"SELECT"}, Roles: []string{"PUBLIC"}},
			Suggestion: "grant to a named role",
		}},
		Summary: schemasecurity.Summary{Warning: 1},
	}

	var out bytes.Buffer
	err := writeSecurityReport(&out, "json", report)
	c.Assert(err, qt.IsNil)

	var decoded schemasecurity.Report
	c.Assert(json.Unmarshal(out.Bytes(), &decoded), qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, report)
}

// reportWith builds a report with count findings and the given summary, for the
// threshold rows above, which read only those two things.
func reportWith(summary schemasecurity.Summary, count int) schemasecurity.Report {
	findings := make([]schemasecurity.Finding, 0, count)
	for range count {
		findings = append(findings, schemasecurity.Finding{Code: "PRV03"})
	}
	return schemasecurity.Report{Findings: findings, Summary: summary}
}

// errorMessage is the message or the empty string, so a row states its outcome
// as a value rather than the test branching on which outcome it expects.
func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
