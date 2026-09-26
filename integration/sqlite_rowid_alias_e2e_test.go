//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// A SQLite table built from SQL and then managed from HCL: the HCL says the
// rowid alias is NOT NULL and the SQL did not. SQLite stores no NULL in the
// alias either way, so applying the HCL leaves the table as it is, where a
// nullability change would rebuild it (stokaro/ptah#3685). The control is a TEXT
// key, which holds NULL on a rowid table until the rebuild makes it NOT NULL.
// Each row reads the result back from `pragma table_info` and from the table
// definition SQLite stored.

// sqliteKeyTable reports whether the key column of widgets is NOT NULL, and the
// CREATE TABLE text SQLite stored for the table.
func sqliteKeyTable(c *qt.C, dbPath string) (notNull bool, definition string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var flag int
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT "notnull" FROM pragma_table_info('widgets') WHERE name = 'id'`).Scan(&flag), qt.IsNil)
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'widgets'`).Scan(&definition), qt.IsNil)
	return flag != 0, definition
}

func TestSQLiteRowidAliasManagedFromHCLE2E(t *testing.T) {
	tests := []struct {
		name        string
		built       string
		keyType     string
		wantNotNull bool
		wantRebuilt bool
	}{
		{
			name:        "the rowid alias is left as it is",
			built:       "CREATE TABLE widgets (id INTEGER PRIMARY KEY);",
			keyType:     "integer",
			wantNotNull: false,
			wantRebuilt: false,
		},
		{
			name:        "a TEXT key is rebuilt NOT NULL",
			built:       "CREATE TABLE widgets (id TEXT PRIMARY KEY);",
			keyType:     "text",
			wantNotNull: true,
			wantRebuilt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			dbPath := filepath.Join(dir, "main.db")
			schemaSQL := writeFileIn(c, dir, "schema.sql", tt.built+"\n")
			hcl := "schema \"main\" {\n}\ntable \"widgets\" {\n  schema = schema.main\n" +
				"  column \"id\" {\n    null = false\n    type = " + tt.keyType + "\n  }\n" +
				"  primary_key {\n    columns = [column.id]\n  }\n}\n"
			schemaHCL := writeFileIn(c, dir, "schema.hcl", hcl)
			dev := "sqlite://" + filepath.Join(dir, "dev.db")
			out, err := runCompatVerb("schema", "apply", "--url", "sqlite://"+dbPath,
				"--to", "file://"+schemaSQL, "--dev-url", dev, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			_, before := sqliteKeyTable(c, dbPath)

			out, err = runCompatVerb("schema", "apply", "--url", "sqlite://"+dbPath,
				"--to", "file://"+schemaHCL, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			notNull, after := sqliteKeyTable(c, dbPath)
			c.Assert(notNull, qt.Equals, tt.wantNotNull, qt.Commentf("stored: %s", after))
			c.Assert(after != before, qt.Equals, tt.wantRebuilt, qt.Commentf("before: %s\nafter: %s", before, after))
		})
	}
}

// TestSQLiteRowidAliasHoldsNoNullEitherWayE2E is the measurement the rule
// rests on, asked of the engine Ptah links: both spellings of the alias accept
// an explicit NULL and store a rowid for it.
func TestSQLiteRowidAliasHoldsNoNullEitherWayE2E(t *testing.T) {
	tests := []struct {
		name       string
		definition string
	}{
		{name: "without NOT NULL", definition: "CREATE TABLE widgets (id INTEGER PRIMARY KEY)"},
		{name: "with NOT NULL", definition: "CREATE TABLE widgets (id integer NOT NULL PRIMARY KEY)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "main.db")
			conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), tt.definition)
			c.Assert(err, qt.IsNil)

			_, err = conn.ExecContext(c.Context(), "INSERT INTO widgets (id) VALUES (NULL)")

			c.Assert(err, qt.IsNil)
			var id int
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT id FROM widgets").Scan(&id), qt.IsNil)
			c.Assert(id, qt.Equals, 1)
		})
	}
}
