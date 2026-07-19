package generator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/safety"
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

func TestCreateSafetyReportFile(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	upFile := filepath.Join(dir, "1234567890_drop_legacy.up.sql")
	err := os.WriteFile(upFile, []byte("DROP TABLE legacy;\n"), 0o600)
	c.Assert(err, qt.IsNil)

	reportFile, err := createSafetyReportFile(upFile, "html", []safety.StatementAssessment{
		{
			Index:     1,
			NodeType:  "sql",
			Subject:   "legacy",
			Statement: "DROP TABLE legacy;",
			Severity:  safety.Destructive,
			Reason:    "DROP TABLE removes the table and all rows",
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(reportFile, qt.Equals, filepath.Join(dir, "1234567890_drop_legacy.safety.html"))

	content, err := os.ReadFile(reportFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, "Ptah migration safety report")
	c.Assert(string(content), qt.Contains, "DROP TABLE legacy;")
	c.Assert(string(content), qt.Contains, "destructive")

	jsonReportFile, err := createSafetyReportFile(upFile, "json", []safety.StatementAssessment{{
		Index:    1,
		Severity: safety.Destructive,
		Reason:   "DROP TABLE removes the table and all rows",
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(jsonReportFile, qt.Equals, filepath.Join(dir, "1234567890_drop_legacy.safety.json"))
	rawJSON, err := os.ReadFile(jsonReportFile)
	c.Assert(err, qt.IsNil)
	var report safety.Report
	c.Assert(json.Unmarshal(rawJSON, &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Destructive)
	c.Assert(report.Destructive, qt.IsTrue)
}
