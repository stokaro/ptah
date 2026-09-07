package migrationlintreport_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationlintreport"
	migrationlint "ptah.run/migration/lint"
)

type gitlabEntry struct {
	Description string `json:"description"`
	CheckName   string `json:"check_name"`
	Fingerprint string `json:"fingerprint"`
	Severity    string `json:"severity"`
	Location    struct {
		Path  string `json:"path"`
		Lines struct {
			Begin int `json:"begin"`
		} `json:"lines"`
	} `json:"location"`
}

func renderGitLab(c *qt.C, report migrationlintreport.Report) []gitlabEntry {
	c.Helper()
	var buf bytes.Buffer
	c.Assert(migrationlintreport.Write(&buf, migrationlintreport.FormatGitLab, report), qt.IsNil)
	var entries []gitlabEntry
	c.Assert(json.Unmarshal(buf.Bytes(), &entries), qt.IsNil)
	return entries
}

func TestWrite_GitLabMapsEverySeverity_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		severity migrationlint.Severity
		want     string
	}{
		{name: "error becomes major", severity: migrationlint.SeverityError, want: "major"},
		{name: "warning becomes minor", severity: migrationlint.SeverityWarning, want: "minor"},
		{name: "info stays info", severity: migrationlint.SeverityInfo, want: "info"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			entries := renderGitLab(c, migrationlintreport.Report{
				Findings: []migrationlint.Finding{{
					Rule:     "DS101",
					Severity: test.severity,
					File:     "migrations/0001_init.up.sql",
					Line:     7,
					Message:  "drops a column",
				}},
			})

			c.Assert(entries, qt.HasLen, 1)
			c.Assert(entries[0].Severity, qt.Equals, test.want)
			c.Assert(entries[0].CheckName, qt.Equals, "DS101")
			c.Assert(entries[0].Description, qt.Equals, "DS101: drops a column")
			c.Assert(entries[0].Location.Path, qt.Equals, "migrations/0001_init.up.sql")
			c.Assert(entries[0].Location.Lines.Begin, qt.Equals, 7)
			c.Assert(entries[0].Fingerprint, qt.Not(qt.Equals), "")
		})
	}
}

// A file-level finding carries line 0, and GitLab requires a line, so the
// entry anchors to the first line of the file rather than being dropped.
func TestWrite_GitLabAnchorsFileLevelFindings_HappyPath(t *testing.T) {
	c := qt.New(t)

	entries := renderGitLab(c, migrationlintreport.Report{
		Findings: []migrationlint.Finding{{
			Rule:     "NM100",
			Severity: migrationlint.SeverityWarning,
			File:     "migrations/0002_add.up.sql",
			Message:  "file name does not follow the configured convention",
		}},
	})

	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Location.Lines.Begin, qt.Equals, 1)
}

// GitLab collapses two entries that share a fingerprint, so two findings that
// agree on rule, file, line and message must still be told apart.
func TestWrite_GitLabSeparatesIdenticalFindings_HappyPath(t *testing.T) {
	c := qt.New(t)
	finding := migrationlint.Finding{
		Rule:     "DS101",
		Severity: migrationlint.SeverityError,
		File:     "migrations/0001_init.up.sql",
		Line:     4,
		Message:  "drops a column",
	}

	entries := renderGitLab(c, migrationlintreport.Report{
		Findings: []migrationlint.Finding{finding, finding},
	})

	c.Assert(entries, qt.HasLen, 2)
	c.Assert(entries[0].Fingerprint, qt.Not(qt.Equals), entries[1].Fingerprint)
}

// The fingerprint tells GitLab a surviving finding from a new one, so it has
// to depend on what the finding says and not on where it sits in the report.
func TestWrite_GitLabFingerprintIgnoresReportOrder_HappyPath(t *testing.T) {
	c := qt.New(t)
	first := migrationlint.Finding{
		Rule: "DS101", Severity: migrationlint.SeverityError,
		File: "migrations/0001_init.up.sql", Line: 4, Message: "drops a column",
	}
	second := migrationlint.Finding{
		Rule: "MF103", Severity: migrationlint.SeverityWarning,
		File: "migrations/0002_add.up.sql", Line: 9, Message: "adds a NOT NULL column",
	}

	forward := renderGitLab(c, migrationlintreport.Report{
		Findings: []migrationlint.Finding{first, second},
	})
	reversed := renderGitLab(c, migrationlintreport.Report{
		Findings: []migrationlint.Finding{second, first},
	})

	c.Assert(forward[0].Fingerprint, qt.Equals, reversed[1].Fingerprint)
	c.Assert(forward[1].Fingerprint, qt.Equals, reversed[0].Fingerprint)
}

// The array is the whole artifact, so an empty one reads as a clean corpus. A
// run that failed has to say so here too, not only on stderr.
func TestWrite_GitLabReportsAFailedRun_FailurePath(t *testing.T) {
	c := qt.New(t)

	report := migrationlintreport.ErrorReport("error", "dev database is unreachable")
	report.Dir = "migrations"
	entries := renderGitLab(c, report)

	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Severity, qt.Equals, "blocker")
	c.Assert(entries[0].Description, qt.Equals, "dev database is unreachable")
	c.Assert(entries[0].Location.Path, qt.Equals, "migrations")
}

func TestWrite_GitLabRendersAnEmptyArrayForACleanRun_HappyPath(t *testing.T) {
	c := qt.New(t)

	var buf bytes.Buffer
	c.Assert(migrationlintreport.Write(&buf, migrationlintreport.FormatGitLab,
		migrationlintreport.Report{}), qt.IsNil)

	c.Assert(buf.String(), qt.Equals, "[]\n")
}

func TestValidateFormat_AcceptsGitLab_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(migrationlintreport.ValidateFormat(migrationlintreport.FormatGitLab), qt.IsNil)
}
