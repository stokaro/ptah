package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
)

// This file pins the other half of the SQLite key-nullability rule
// (stokaro/ptah#1235): SQLite decides it from the table's shape, not from the
// dialect, and a STRICT or WITHOUT ROWID table does enforce NOT NULL on its key
// columns.
//
// Reading the rule off `pragma table_info` on SQLite 3.51.0, and confirmed
// against the pinned Atlas community v1.3.0 binary, which reports the same
// nullability for each shape when it reads the same DDL through a dev database:
//
//	shape                                            notnull
//	id TEXT PRIMARY KEY                              0
//	id INTEGER PRIMARY KEY                           0
//	PRIMARY KEY (team, member)                       0, 0
//	id TEXT PRIMARY KEY            WITHOUT ROWID     1
//	id INTEGER PRIMARY KEY         WITHOUT ROWID     1
//	PRIMARY KEY (team, member)     WITHOUT ROWID     1, 1
//	id TEXT PRIMARY KEY            STRICT            1
//	id INT PRIMARY KEY             STRICT            1
//	PRIMARY KEY (team, member)     STRICT            1, 1
//	id INTEGER PRIMARY KEY         STRICT            0
//
// The last row is not an exception: a STRICT table still has a rowid, and
// `id INTEGER PRIMARY KEY` is still its alias, so `INSERT INTO t (id) VALUES
// (NULL)` is accepted there and assigns a rowid.
//
// What treating every SQLite table as nullable-key cost, measured on 2026-08-08
// with `ptah-compat` built from this branch's previous head 90a39945: a second
// `schema apply` of the identical fixture planned a full table rebuild
// (CREATE __ptah_rebuild_users / INSERT / DROP / RENAME) for the WITHOUT ROWID
// and STRICT rows and never reached a fixed point, and a second `migrate diff`
// against the identical desired file wrote a second migration containing that
// rebuild. Both are permanent: the catalog answers NOT NULL, the desired model
// answers nullable, and nothing either one does can make them agree.

// applySQLiteSourceFile runs `schema apply --auto-approve` from a desired-state
// file and returns the combined output.
//
// devName names a dev database per call so a second apply is answered from a
// scratch database of its own rather than out of the first one's leftovers.
func applySQLiteSourceFile(c *qt.C, dbPath, sourcePath, devName string) string {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", sqliteURLFromPath(dbPath),
		"--to", "file://" + sourcePath,
		"--dev-url", sqliteURLFromPath(filepath.Join(filepath.Dir(dbPath), devName)),
		"--auto-approve",
	})
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	return out.String()
}

// keyColumnNullability returns what SQLite says about every primary key column
// of table, read from `pragma table_info` rather than from the DDL text, so the
// assertion is about the database that was built and not about how it was
// spelled. The map covers the key exactly, so a composite key that lost a
// column is a failure too.
func keyColumnNullability(c *qt.C, dbPath, table string) map[string]bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), sqliteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.QueryContext(
		context.Background(),
		`SELECT name, "notnull" FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk`,
		table,
	)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()

	nullability := map[string]bool{}
	for rows.Next() {
		var name string
		var notNull int
		c.Assert(rows.Scan(&name, &notNull), qt.IsNil)
		nullability[name] = notNull != 0
	}
	c.Assert(rows.Err(), qt.IsNil)
	return nullability
}

// TestSchemaApplySQLiteTableShapeConvergesOnKeyNullability applies each table
// shape and then applies the identical desired state again, which must be a
// no-op.
//
// The DDL is executed rather than only rendered, and the key's nullability is
// read back out of the catalog, because the whole defect is a disagreement
// between what Ptah's model says about a key column and what SQLite did with it.
//
// The rows are in both directions on purpose. Take the shape out of the rule and
// the WITHOUT ROWID and STRICT rows report a rebuild; apply the rule to every
// STRICT table and `strict rowid alias` reports one instead, which is
// stokaro/ptah#1235's own defect wearing a STRICT table.
func TestSchemaApplySQLiteTableShapeConvergesOnKeyNullability(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		source string
		want   map[string]bool
	}{
		{
			name:   "rowid table key column stays nullable",
			table:  "users",
			source: "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL);",
			want:   map[string]bool{"id": false},
		},
		{
			name:   "rowid alias key column stays nullable",
			table:  "users",
			source: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
			want:   map[string]bool{"id": false},
		},
		{
			name:   "rowid table level composite key stays nullable",
			table:  "memberships",
			source: "CREATE TABLE memberships (team TEXT, member TEXT, PRIMARY KEY (team, member));",
			want:   map[string]bool{"team": false, "member": false},
		},
		{
			name:   "without rowid key column is not null",
			table:  "users",
			source: "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL) WITHOUT ROWID;",
			want:   map[string]bool{"id": true},
		},
		{
			name:   "without rowid integer key column is not null",
			table:  "users",
			source: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL) WITHOUT ROWID;",
			want:   map[string]bool{"id": true},
		},
		{
			name:  "without rowid table level composite key is not null",
			table: "memberships",
			source: "CREATE TABLE memberships (team TEXT, member TEXT, PRIMARY KEY (team, member))" +
				" WITHOUT ROWID;",
			want: map[string]bool{"team": true, "member": true},
		},
		{
			name:   "strict key column is not null",
			table:  "users",
			source: "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL) STRICT;",
			want:   map[string]bool{"id": true},
		},
		{
			name:   "strict int key column is not null because INT is no rowid alias",
			table:  "users",
			source: "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL) STRICT;",
			want:   map[string]bool{"id": true},
		},
		{
			name:  "strict table level composite key is not null",
			table: "memberships",
			source: "CREATE TABLE memberships (team TEXT, member TEXT, PRIMARY KEY (team, member))" +
				" STRICT;",
			want: map[string]bool{"team": true, "member": true},
		},
		{
			name:   "strict rowid alias key column stays nullable",
			table:  "users",
			source: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL) STRICT;",
			want:   map[string]bool{"id": false},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			sourcePath := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(sourcePath, []byte(test.source), 0o600), qt.IsNil)
			dbPath := filepath.Join(dir, "main.db")

			applySQLiteSourceFile(c, dbPath, sourcePath, "dev1.db")
			c.Assert(keyColumnNullability(c, dbPath, test.table), qt.DeepEquals, test.want,
				qt.Commentf("persisted DDL: %s", sqliteTableDDL(c, dbPath, test.table)))

			second := applySQLiteSourceFile(c, dbPath, sourcePath, "dev2.db")
			c.Assert(second, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// TestSchemaApplySQLiteTableShapeStillPlansGenuineChanges is the companion that
// keeps the rule above from being satisfied by a comparator that reports
// nothing. A real difference on a WITHOUT ROWID table is still planned, applied,
// and converges on the next run.
func TestSchemaApplySQLiteTableShapeStillPlansGenuineChanges(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "main.db")
	before := filepath.Join(dir, "before.sql")
	after := filepath.Join(dir, "after.sql")
	c.Assert(os.WriteFile(before, []byte(
		"CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL) WITHOUT ROWID;",
	), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(after, []byte(
		"CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, note TEXT) WITHOUT ROWID;",
	), 0o600), qt.IsNil)

	applySQLiteSourceFile(c, dbPath, before, "dev1.db")

	changed := applySQLiteSourceFile(c, dbPath, after, "dev2.db")
	c.Assert(changed, qt.Contains, `ADD COLUMN "note"`)
	c.Assert(keyColumnNullability(c, dbPath, "users"), qt.DeepEquals, map[string]bool{"id": true})

	again := applySQLiteSourceFile(c, dbPath, after, "dev3.db")
	c.Assert(again, qt.Contains, "Schema is synced, no changes to be made.")
}
