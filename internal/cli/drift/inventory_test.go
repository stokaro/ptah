package drift_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/safety"
)

// inventoryDesired is the schema both inventory tests declare: two tables, one
// of them with an index.
const inventoryDesired = `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);
CREATE INDEX idx_users_email ON users (email);
CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);
`

// inventoryReport is the part of the JSON report these tests read.
type inventoryReport struct {
	Drift     bool                    `json:"drift"`
	Inventory []safety.InventoryEntry `json:"inventory"`
}

// TestRunDrift_TheInventoryCountsBothSides drives the command against a SQLite
// database that differs from the declaration by an added table, a removed
// index and a column the database holds and the declaration does not. Each
// kind is counted on both sides, the columns in the table both sides hold.
func TestRunDrift_TheInventoryCountsBothSides(t *testing.T) {
	tests := []struct {
		name string
		// database is the DDL the live database is built from.
		database string
		// wantExit is the process exit code.
		wantExit int
		// want is the inventory the report carries.
		want []safety.InventoryEntry
	}{
		{
			name: "a database that differs",
			database: `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL, legacy TEXT);
CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_users_legacy ON users (legacy);
`,
			wantExit: 1,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 2, Observed: 3},
				{Kind: "indexes", Desired: 1, Observed: 2},
				{Kind: "tables", Desired: 2, Observed: 1},
			},
		},
		{
			name:     "a database that matches",
			database: inventoryDesired,
			wantExit: 0,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 4, Observed: 4},
				{Kind: "indexes", Desired: 1, Observed: 1},
				{Kind: "tables", Desired: 2, Observed: 2},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TB.TempDir()
			schemaFile := writeSchemaFile(c, dir, inventoryDesired)
			url := sqliteDatabase(c, dir, test.database)

			code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", "json")

			c.Assert(code, qt.Equals, test.wantExit)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(decodeInventoryReport(c, stdout).Inventory, qt.DeepEquals, test.want)
		})
	}
}

// TestRunDrift_AnIgnoredTableIsCountedOnNeitherSide excludes a table both
// sides hold, with columns that differ. The inventory leaves it out of every
// kind, as the findings do, and still counts the table the run compares.
func TestRunDrift_AnIgnoredTableIsCountedOnNeitherSide(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	schemaFile := writeSchemaFile(c, dir, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);
CREATE TABLE audit_log (id INTEGER PRIMARY KEY, at TEXT, actor TEXT);
CREATE INDEX idx_audit_log_at ON audit_log (at);
`)
	url := sqliteDatabase(c, dir, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);
CREATE TABLE audit_log (id INTEGER PRIMARY KEY, at TEXT);
`)

	code, stdout, stderr := runDriftCommand(c,
		"--db-url", url, "--schema-file", schemaFile, "--format", "json", "--ignore", "tables=audit_log")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stderr, qt.Equals, "")
	report := decodeInventoryReport(c, stdout)
	c.Assert(report.Drift, qt.IsFalse)
	c.Assert(report.Inventory, qt.DeepEquals, []safety.InventoryEntry{
		{Kind: "columns", Desired: 2, Observed: 2},
		{Kind: "tables", Desired: 1, Observed: 1},
	})
}

// TestRunDrift_AComparisonOfNothingCarriesAnEmptyInventory pins the field on a
// report that compared and found no object on either side: it is there, and
// empty, so a reader tells a report that counted nothing from one that did not
// count.
func TestRunDrift_AComparisonOfNothingCarriesAnEmptyInventory(t *testing.T) {
	c := qt.New(t)
	dir := c.TB.TempDir()
	schemaFile := writeSchemaFile(c, dir, "")
	url := "sqlite://" + filepath.Join(dir, "empty.db")

	code, stdout, stderr := runDriftCommand(c, "--db-url", url, "--schema-file", schemaFile, "--format", "json")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, `"inventory": []`)
}

// TestRunDrift_AnErrorReportCarriesNoInventory pins the difference between a
// report that compared and one that did not: only the first counts anything.
func TestRunDrift_AnErrorReportCarriesNoInventory(t *testing.T) {
	c := qt.New(t)

	code, _, stderr := runDriftCommand(c, "--root-dir", ".", "--format", "json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(stderr, qt.Contains, `"error": "database URL is required"`)
	c.Assert(stderr, qt.Not(qt.Contains), `"inventory"`)
}

// writeSchemaFile writes the desired schema as a SQL file and returns its path.
func writeSchemaFile(c *qt.C, dir, ddl string) string {
	c.Helper()
	path := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(path, []byte(ddl), 0o600), qt.IsNil)
	return path
}

// sqliteDatabase builds a SQLite database from ddl, one statement at a time,
// and returns the URL to check it with.
func sqliteDatabase(c *qt.C, dir, ddl string) string {
	c.Helper()
	ctx := context.Background()
	url := "sqlite://" + filepath.Join(dir, "target.db")
	conn, err := dbschema.ConnectToDatabase(ctx, url)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, ddl)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)
	return url
}

// decodeInventoryReport reads the JSON report the command wrote.
func decodeInventoryReport(c *qt.C, stdout string) inventoryReport {
	c.Helper()
	var report inventoryReport
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	return report
}
