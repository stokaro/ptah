//go:build integration

package driftcmd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/drift"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/safety"
)

// postgresDesired declares objects of every kind a PostgreSQL inventory counts
// that a database of its own can hold: an enum type and its values, tables and
// their columns, an index, a foreign key, a CHECK and a function. A role is
// left out because it belongs to the server, not to the database.
var postgresDesired = []string{
	`CREATE TYPE mood AS ENUM ('happy', 'sad', 'calm')`,
	`CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL, feeling mood)`,
	`CREATE INDEX idx_users_email ON users (email)`,
	`CREATE TABLE posts (id integer PRIMARY KEY, author_id integer NOT NULL, title text,
		CONSTRAINT fk_posts_author FOREIGN KEY (author_id) REFERENCES users (id),
		CONSTRAINT chk_posts_title CHECK (title <> ''))`,
	`CREATE FUNCTION add_one(i integer) RETURNS integer LANGUAGE sql AS 'SELECT i + 1'`,
}

// mysqlDesired is postgresDesired in the MySQL family's terms. An enum is a
// column type there rather than an object, so there is no enum kind.
var mysqlDesired = []string{
	"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL, feeling enum('happy','sad','calm'))",
	"CREATE INDEX idx_users_email ON users (email)",
	"CREATE TABLE posts (id int PRIMARY KEY, author_id int NOT NULL, title varchar(100), " +
		"CONSTRAINT fk_posts_author FOREIGN KEY (author_id) REFERENCES users (id), " +
		"CONSTRAINT chk_posts_title CHECK (title <> ''))",
}

// TestSchemaDriftInventoryE2E drives `ptah schema drift --format json` against
// a live server, which decides what the catalog holds beside what the DDL
// names: the index behind a PRIMARY KEY or a foreign key, the type an enum
// column carries, the name a server gives a constraint.
//
// The database that matches holds the invariant the inventory rests on: with
// no difference, each kind counts the same on both sides, and the count is
// what the declaration holds. The database that differs by an added table,
// a removed index and a column the declaration does not have is counted on
// both sides, each kind by what the comparison pairs: the columns are the
// columns of the table both sides hold.
func TestSchemaDriftInventoryE2E(t *testing.T) {
	tests := []struct {
		name     string
		engine   dbtarget.Engine
		desired  []string
		database []string
		// wantDrift is the report's drift field: the comparison found a
		// difference.
		wantDrift bool
		want      []safety.InventoryEntry
	}{
		{
			name:      "PostgreSQL matches",
			engine:    dbtarget.PostgreSQL,
			desired:   postgresDesired,
			database:  postgresDesired,
			wantDrift: false,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 6, Observed: 6},
				{Kind: "constraints", Desired: 2, Observed: 2},
				{Kind: "enum_values", Desired: 3, Observed: 3},
				{Kind: "enums", Desired: 1, Observed: 1},
				{Kind: "functions", Desired: 1, Observed: 1},
				{Kind: "indexes", Desired: 1, Observed: 1},
				{Kind: "tables", Desired: 2, Observed: 2},
			},
		},
		{
			name:    "PostgreSQL differs",
			engine:  dbtarget.PostgreSQL,
			desired: postgresDesired,
			database: []string{
				`CREATE TYPE mood AS ENUM ('happy', 'sad', 'calm')`,
				`CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL, feeling mood, legacy text)`,
				`CREATE INDEX idx_users_email ON users (email)`,
				`CREATE INDEX idx_users_legacy ON users (legacy)`,
				`CREATE FUNCTION add_one(i integer) RETURNS integer LANGUAGE sql AS 'SELECT i + 1'`,
			},
			wantDrift: true,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 3, Observed: 4},
				{Kind: "constraints", Desired: 2, Observed: 0},
				{Kind: "enum_values", Desired: 3, Observed: 3},
				{Kind: "enums", Desired: 1, Observed: 1},
				{Kind: "functions", Desired: 1, Observed: 1},
				{Kind: "indexes", Desired: 1, Observed: 2},
				{Kind: "tables", Desired: 2, Observed: 1},
			},
		},
		{
			name:      "MySQL matches",
			engine:    dbtarget.MySQLAdmin,
			desired:   mysqlDesired,
			database:  mysqlDesired,
			wantDrift: false,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 6, Observed: 6},
				{Kind: "constraints", Desired: 2, Observed: 2},
				{Kind: "indexes", Desired: 1, Observed: 1},
				{Kind: "tables", Desired: 2, Observed: 2},
			},
		},
		{
			name:    "MySQL differs",
			engine:  dbtarget.MySQLAdmin,
			desired: mysqlDesired,
			database: []string{
				"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL, " +
					"feeling enum('happy','sad','calm'), legacy text)",
				"CREATE INDEX idx_users_email ON users (email)",
				"CREATE INDEX idx_users_legacy ON users (legacy(10))",
			},
			wantDrift: true,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 3, Observed: 4},
				{Kind: "constraints", Desired: 2, Observed: 0},
				{Kind: "indexes", Desired: 1, Observed: 2},
				{Kind: "tables", Desired: 2, Observed: 1},
			},
		},
		{
			name:      "MariaDB matches",
			engine:    dbtarget.MariaDBAdmin,
			desired:   mysqlDesired,
			database:  mysqlDesired,
			wantDrift: false,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 6, Observed: 6},
				{Kind: "constraints", Desired: 2, Observed: 2},
				{Kind: "indexes", Desired: 1, Observed: 1},
				{Kind: "tables", Desired: 2, Observed: 2},
			},
		},
		{
			name:    "MariaDB differs",
			engine:  dbtarget.MariaDBAdmin,
			desired: mysqlDesired,
			database: []string{
				"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL, " +
					"feeling enum('happy','sad','calm'), legacy text)",
				"CREATE INDEX idx_users_email ON users (email)",
				"CREATE INDEX idx_users_legacy ON users (legacy(10))",
			},
			wantDrift: true,
			want: []safety.InventoryEntry{
				{Kind: "columns", Desired: 3, Observed: 4},
				{Kind: "constraints", Desired: 2, Observed: 0},
				{Kind: "indexes", Desired: 1, Observed: 2},
				{Kind: "tables", Desired: 2, Observed: 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := scratchDatabase(c, test.engine, test.database)
			schema := writeSchema(c, test.desired)

			report := runDrift(c, "--db-url", url, "--schema-file", schema, "--format", "json")

			c.Assert(report.Error, qt.Equals, "")
			c.Assert(report.Drift, qt.Equals, test.wantDrift)
			c.Assert(report.Inventory, qt.DeepEquals, test.want)
		})
	}
}

// TestSchemaDriftInventoryLeavesAnIgnoredTableOutE2E excludes a table both
// sides hold, with a column, an index and a CHECK only the declaration has.
// Nothing of it is counted on either side, and the table the run compares
// still is.
func TestSchemaDriftInventoryLeavesAnIgnoredTableOutE2E(t *testing.T) {
	tests := []struct {
		name     string
		engine   dbtarget.Engine
		desired  []string
		database []string
	}{
		{
			name:   "PostgreSQL",
			engine: dbtarget.PostgreSQL,
			desired: []string{
				`CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL)`,
				`CREATE TABLE audit_log (id integer PRIMARY KEY, at text, actor text, CONSTRAINT chk_audit_at CHECK (at <> ''))`,
				`CREATE INDEX idx_audit_log_at ON audit_log (at)`,
			},
			database: []string{
				`CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL)`,
				`CREATE TABLE audit_log (id integer PRIMARY KEY, at text)`,
			},
		},
		{
			name:   "MySQL",
			engine: dbtarget.MySQLAdmin,
			desired: []string{
				"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL)",
				"CREATE TABLE audit_log (id int PRIMARY KEY, at varchar(40), actor varchar(40), CONSTRAINT chk_audit_at CHECK (at <> ''))",
				"CREATE INDEX idx_audit_log_at ON audit_log (at)",
			},
			database: []string{
				"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL)",
				"CREATE TABLE audit_log (id int PRIMARY KEY, at varchar(40))",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := scratchDatabase(c, test.engine, test.database)
			schema := writeSchema(c, test.desired)

			report := runDrift(c,
				"--db-url", url, "--schema-file", schema, "--format", "json", "--ignore", "tables=audit_log")

			c.Assert(report.Error, qt.Equals, "")
			c.Assert(report.Drift, qt.IsFalse)
			c.Assert(report.Inventory, qt.DeepEquals, []safety.InventoryEntry{
				{Kind: "columns", Desired: 2, Observed: 2},
				{Kind: "tables", Desired: 1, Observed: 1},
			})
		})
	}
}

// driftReport is the part of the JSON report these tests read.
type driftReport struct {
	Drift     bool                    `json:"drift"`
	Error     string                  `json:"error"`
	Findings  []safety.Finding        `json:"findings"`
	Inventory []safety.InventoryEntry `json:"inventory"`
}

// runDrift runs `ptah schema drift` in-process and decodes the JSON report it
// writes. A comparison writes it to standard output, and an error to standard
// error, so a run that failed to compare reads back with Error set.
func runDrift(c *qt.C, args ...string) driftReport {
	c.Helper()
	cmd := drift.NewDriftCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	runErr := cmd.ExecuteContext(c.Context())
	document := out.Bytes()
	if out.Len() == 0 {
		document = errOut.Bytes()
	}
	var report driftReport
	c.Assert(json.Unmarshal(document, &report), qt.IsNil,
		qt.Commentf("error: %v\nstdout:\n%s\nstderr:\n%s", runErr, out.String(), errOut.String()))
	return report
}

// writeSchema writes the statements as the desired schema file.
func writeSchema(c *qt.C, statements []string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(strings.Join(statements, ";\n")+";\n"), 0o600), qt.IsNil)
	return path
}

// scratchDatabase creates a database of its own on the engine's server, runs
// the statements on it with nothing of Ptah's in between, and returns the URL
// Ptah connects to it with. The database is dropped when the test ends.
func scratchDatabase(c *qt.C, engine dbtarget.Engine, statements []string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	quote := identifierQuote(engine)
	name := fmt.Sprintf("ptah_drift_inventory_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+quote+name+quote)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), dropDatabase(engine, name))
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})

	target := withDatabase(c, adminURL, name)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return target
}

// identifierQuote is the character the engine quotes a database name with.
func identifierQuote(engine dbtarget.Engine) string {
	if engine == dbtarget.PostgreSQL {
		return `"`
	}
	return "`"
}

// dropDatabase is the statement that drops the scratch database, closing the
// sessions a PostgreSQL server keeps open on it.
func dropDatabase(engine dbtarget.Engine, name string) string {
	if engine == dbtarget.PostgreSQL {
		return `DROP DATABASE IF EXISTS "` + name + `" WITH (FORCE)`
	}
	return "DROP DATABASE IF EXISTS `" + name + "`"
}

// withDatabase replaces the database an address names, keeping its query.
func withDatabase(c *qt.C, address, database string) string {
	c.Helper()
	base, query, hasQuery := strings.Cut(address, "?")
	slash := strings.LastIndex(base, "/")
	c.Assert(slash > strings.Index(base, "://")+2, qt.IsTrue, qt.Commentf("%s names no database", address))
	replaced := base[:slash+1] + database
	if hasQuery {
		return replaced + "?" + query
	}
	return replaced
}
