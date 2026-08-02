package generator

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/safety"
)

func TestCheckDestructiveAllowed(t *testing.T) {
	c := qt.New(t)

	destructive := []safety.StatementAssessment{
		{Severity: safety.Destructive, Reason: "DROP TABLE removes the table and all rows"},
	}
	warning := []safety.StatementAssessment{
		{Severity: safety.Warning, Reason: "CREATE UNIQUE INDEX can fail on existing duplicate values"},
	}

	err := checkDestructiveAllowed(GenerateMigrationOptions{CheckDestructive: true}, destructive)
	c.Assert(err, qt.ErrorMatches, "destructive migration statements require AllowDestructive")

	err = checkDestructiveAllowed(GenerateMigrationOptions{CheckDestructive: true, AllowDestructive: true}, destructive)
	c.Assert(err, qt.IsNil)

	err = checkDestructiveAllowed(GenerateMigrationOptions{CheckDestructive: false}, destructive)
	c.Assert(err, qt.IsNil)

	err = checkDestructiveAllowed(GenerateMigrationOptions{CheckDestructive: true}, warning)
	c.Assert(err, qt.IsNil)
}

func TestRenderSafetyReport(t *testing.T) {
	c := qt.New(t)

	reportFile, content, err := renderSafetyReport(
		"1234567890_drop_legacy.up.sql",
		"html",
		[]safety.StatementAssessment{
			{
				Index:     1,
				NodeType:  "sql",
				Subject:   "legacy",
				Statement: "DROP TABLE legacy;",
				Severity:  safety.Destructive,
				Reason:    "DROP TABLE removes the table and all rows",
			},
		},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(reportFile, qt.Equals, "1234567890_drop_legacy.safety.html")
	c.Assert(string(content), qt.Contains, "Ptah migration safety report")
	c.Assert(string(content), qt.Contains, "DROP TABLE legacy;")
	c.Assert(string(content), qt.Contains, "destructive")

	jsonReportFile, rawJSON, err := renderSafetyReport(
		"1234567890_drop_legacy.up.sql",
		"json",
		[]safety.StatementAssessment{{
			Index:    1,
			Severity: safety.Destructive,
			Reason:   "DROP TABLE removes the table and all rows",
		}},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(jsonReportFile, qt.Equals, "1234567890_drop_legacy.safety.json")
	var report safety.Report
	c.Assert(json.Unmarshal(rawJSON, &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Destructive)
	c.Assert(report.Destructive, qt.IsTrue)
}
