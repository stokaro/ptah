//go:build integration

package safety_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// subjectCellRe reads the subject cell of each row of the HTML safety report.
var subjectCellRe = regexp.MustCompile(`<td class="name">([^<]*)</td>`)

// TestMigrationsPlanE2E_TheSafetyReportNamesWhatEachStatementActsOn plans a
// SQLite table rebuild, a new table and a new view through the shipped binary
// and reads the subject column of the text and the HTML safety report. The
// rebuild is raw SQL, which names its object only in its words; the table and
// the view are nodes that name theirs. Neither report prints a Go type name.
func TestMigrationsPlanE2E_TheSafetyReportNamesWhatEachStatementActsOn(t *testing.T) {
	c := qt.New(t)
	conn, address := sqliteScratch(c)
	_, err := conn.ExecContext(c.Context(), "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
	c.Assert(err, qt.IsNil)
	schema := schemaFile(c, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL);\n"+
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
		"CREATE VIEW active_users AS SELECT id FROM users")
	plan := func(report string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
			"migrations", "plan", "--db-url", address, "--schema-file", schema, "--allow-table-rebuild", "--report", report)
	}
	want := []string{"foreign_keys", "users", "new_products", "new_products", "products", "new_products", "active_users", "foreign_keys"}

	text := plan("text")
	html := plan("html")

	c.Assert(text.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", text.Stdout, text.Stderr))
	c.Assert(textSubjects(c, text.Stdout), qt.DeepEquals, want)
	c.Assert(text.Stdout, qt.Not(qt.Contains), "*ast.")
	c.Assert(html.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", html.Stdout, html.Stderr))
	c.Assert(htmlSubjects(html.Stdout), qt.DeepEquals, want)
	c.Assert(html.Stdout, qt.Not(qt.Contains), "*ast.")
}

// sqlServerExtensionSchema declares a table, an extended property at each scope
// SQL Server has, and a synonym. Each property and the synonym plan as an
// extension statement, which names its object only through its payload.
const sqlServerExtensionSchema = `schema "dbo" {
}

table "Gauge" {
  schema = schema.dbo
  column "id" {
    type = int
    null = false
  }
  column "Title" {
    type = nvarchar(100)
    null = true
  }
}

extended_property "ptah_db" {
  value = "database scope"
}

extended_property "ptah_schema" {
  schema = schema.dbo
  value  = "schema scope"
}

extended_property "ptah_table" {
  schema = schema.dbo
  table  = "Gauge"
  value  = "table scope"
}

extended_property "ptah_column" {
  schema = schema.dbo
  table  = "Gauge"
  column = "Title"
  value  = "column scope"
}

synonym "GaugeAlias" {
  schema = schema.dbo
  target = "dbo.Gauge"
}
`

// reportedAssessment is one row of the JSON safety report.
type reportedAssessment struct {
	Subject   string `json:"subject"`
	Statement string `json:"statement"`
}

// TestMigrationsPlanE2E_TheSafetyReportNamesASQLServerExtensionObject plans
// extended properties and a synonym on an empty SQL Server database through the
// shipped binary. Each property row names the object the property is on, and the
// synonym row names the synonym. A database property is on no object and has no
// subject.
func TestMigrationsPlanE2E_TheSafetyReportNamesASQLServerExtensionObject(t *testing.T) {
	c := qt.New(t)
	_, address := temporalScratch(c, sqlServerTemporal)
	schema := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(schema, []byte(sqlServerExtensionSchema), 0o600), qt.IsNil)

	plan := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "plan", "--db-url", address, "--schema-file", schema, "--report", "json")

	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", plan.Stdout, plan.Stderr))
	var report struct {
		Assessments []reportedAssessment `json:"assessments"`
	}
	c.Assert(json.Unmarshal([]byte(plan.Stdout), &report), qt.IsNil, qt.Commentf("stdout:\n%s", plan.Stdout))
	c.Assert(report.Assessments, qt.DeepEquals, []reportedAssessment{
		{Subject: "dbo.Gauge", Statement: "CREATE TABLE [dbo].[Gauge] (\n  [id] int NOT NULL,\n  [Title] nvarchar(100)\n)"},
		{Subject: "", Statement: "EXEC sp_addextendedproperty @name = N'ptah_db', @value = N'database scope'"},
		{
			Subject: "dbo.Gauge.Title",
			Statement: "EXEC sp_addextendedproperty @name = N'ptah_column', @value = N'column scope', " +
				"@level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'Gauge', " +
				"@level2type = N'COLUMN', @level2name = N'Title'",
		},
		{
			Subject:   "dbo",
			Statement: "EXEC sp_addextendedproperty @name = N'ptah_schema', @value = N'schema scope', @level0type = N'SCHEMA', @level0name = N'dbo'",
		},
		{
			Subject: "dbo.Gauge",
			Statement: "EXEC sp_addextendedproperty @name = N'ptah_table', @value = N'table scope', " +
				"@level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'Gauge'",
		},
		{Subject: "dbo.GaugeAlias", Statement: "CREATE SYNONYM [dbo].[GaugeAlias] FOR [dbo].[Gauge]"},
	})
}

// textSubjects reads the subject column of the text safety table: the rows
// between its header and the migration SQL.
func textSubjects(c *qt.C, stdout string) []string {
	c.Helper()
	_, table, found := strings.Cut(stdout, "  #  severity      subject                  reason\n")
	c.Assert(found, qt.IsTrue, qt.Commentf("no safety table in:\n%s", stdout))
	table, _, found = strings.Cut(table, "=== MIGRATION SQL ===")
	c.Assert(found, qt.IsTrue, qt.Commentf("no migration SQL after the safety table in:\n%s", stdout))
	var subjects []string
	for line := range strings.Lines(table) {
		fields := strings.Fields(line)
		c.Assert(len(fields) >= 3, qt.IsTrue, qt.Commentf("row %q", line))
		subjects = append(subjects, fields[2])
	}
	return subjects
}

// htmlSubjects reads the subject cell of every row of the HTML safety report.
func htmlSubjects(page string) []string {
	var subjects []string
	for _, match := range subjectCellRe.FindAllStringSubmatch(page, -1) {
		subjects = append(subjects, match[1])
	}
	return subjects
}
