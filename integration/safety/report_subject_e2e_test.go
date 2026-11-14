//go:build integration

package safety_test

import (
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
