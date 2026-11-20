package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// analyzePlaced runs the real analyzer over the gap fixture against the server
// a run names with --server-version, and returns the codes and the MY142
// message it reported for version 2.
func analyzePlaced(c *qt.C, dialect, version, statement string) (codes []string, message string) {
	c.Helper()
	target, err := lint.ResolveTarget(dialect, version)
	c.Assert(err, qt.IsNil)
	options := lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Target:    target,
	}
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(gapFS(statement)), options)
	c.Assert(err, qt.IsNil)
	for _, finding := range analysis.Findings() {
		codes = append(codes, finding.Rule)
	}
	return codes, messageOf(analysis.Findings(), "MY142")
}

// TestInstantColumnPositionRule_HappyPath drives MY142 on the servers that
// rebuild the table for a column placed before existing ones. Measured with
// ALGORITHM=INSTANT, MySQL 8.0.28 and MariaDB 10.3.39 refuse FIRST and AFTER
// with 1845, so each row names the column and the server it planned against.
func TestInstantColumnPositionRule_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		version   string
		statement string
		wantText  string
	}{
		{
			name: "FIRST on MySQL 8.0.28", dialect: "mysql", version: "8.0.28",
			statement: "ALTER TABLE orders ADD COLUMN tag int FIRST;",
			wantText:  "column tag is added with FIRST or AFTER, which mysql 8.0.28 cannot do as an instant change",
		},
		{
			name: "AFTER on MySQL 8.0.20", dialect: "mysql", version: "8.0.20",
			statement: "ALTER TABLE orders ADD tag int NOT NULL DEFAULT 0 AFTER id;",
			wantText:  "column tag is added with FIRST or AFTER, which mysql 8.0.20 cannot do as an instant change",
		},
		{
			name: "the second clause of two", dialect: "mysql", version: "8.0.28",
			statement: "ALTER TABLE orders ADD COLUMN last_one int, ADD COLUMN `Tag` int AFTER id;",
			wantText:  "column `Tag` is added with FIRST or AFTER",
		},
		{
			name: "FIRST on MariaDB 10.3", dialect: "mariadb", version: "10.3.39",
			statement: "ALTER TABLE orders ADD COLUMN tag int FIRST;",
			wantText:  "which mariadb 10.3.39 cannot do as an instant change",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			codes, message := analyzePlaced(c, tt.dialect, tt.version, tt.statement)
			c.Assert(codes, qt.Contains, "MY142")
			c.Assert(message, qt.Contains, tt.wantText)
		})
	}
}

// TestInstantColumnPositionRule_StaysQuiet is the control: the same placement
// on a server that does it instantly, a run that names no server and so plans
// against the newest line, a run that names no dialect and so knows nothing
// about the server, a column added last, and statements that place no new
// column.
func TestInstantColumnPositionRule_StaysQuiet(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		version   string
		statement string
	}{
		{name: "MySQL 8.0.29", dialect: "mysql", version: "8.0.29", statement: "ALTER TABLE orders ADD COLUMN tag int FIRST;"},
		{name: "MySQL 8.4", dialect: "mysql", version: "8.4", statement: "ALTER TABLE orders ADD COLUMN tag int AFTER id;"},
		{name: "MariaDB 10.4", dialect: "mariadb", version: "10.4.34", statement: "ALTER TABLE orders ADD COLUMN tag int FIRST;"},
		{name: "no server named", dialect: "mysql", version: "", statement: "ALTER TABLE orders ADD COLUMN tag int FIRST;"},
		// A run with no dialect runs every rule and answers no capability key,
		// so the rule cannot tell a server that rebuilds from one that does not.
		{name: "no dialect named", dialect: "", version: "", statement: "ALTER TABLE orders ADD COLUMN tag int FIRST;"},
		{name: "a column added last", dialect: "mysql", version: "8.0.28", statement: "ALTER TABLE orders ADD COLUMN tag int;"},
		{name: "a column named first", dialect: "mysql", version: "8.0.28", statement: "ALTER TABLE orders ADD COLUMN `first` int;"},
		{name: "a comment that says FIRST", dialect: "mysql", version: "8.0.28", statement: "ALTER TABLE orders ADD COLUMN tag int COMMENT 'FIRST';"},
		{name: "an existing column moved", dialect: "mysql", version: "8.0.28", statement: "ALTER TABLE orders MODIFY COLUMN note varchar(10) FIRST;"},
		{name: "an index", dialect: "mysql", version: "8.0.28", statement: "ALTER TABLE orders ADD INDEX first_idx (total);"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			codes, message := analyzePlaced(c, tt.dialect, tt.version, tt.statement)
			c.Assert(codes, qt.Not(qt.Contains), "MY142")
			c.Assert(message, qt.Equals, "")
		})
	}
}
