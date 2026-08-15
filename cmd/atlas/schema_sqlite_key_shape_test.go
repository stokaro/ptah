package atlas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
)

// This file pins the SQLite key-and-uniqueness output shape of the
// Atlas-compatible surface, measured against the pinned Atlas community v1.3.0
// binary for stokaro/ptah#1235 (findings 5.1, 5.2 and 6.3).
//
// The three findings share one cause: Ptah folded constraints into the column
// definition using rules SQLite does not have. A primary key was rendered as
// `id integer PRIMARY KEY`, swallowing the author's NOT NULL, and every
// single-column unique index — including one an author created by name — was
// folded into the column as an inline UNIQUE while the index itself was still
// emitted.
//
// What that cost, measured on 2026-08-08 with each binary in its own directory:
//
//   - `schema apply --to file://users.hcl --auto-approve` over an HCL table
//     whose key column says `null = false` produced a database the pinned
//     binary answered was NOT in sync with that same file: asked
//     `schema diff --from sqlite://main.db --to file://users.hcl`, it planned a
//     full table rebuild (PRAGMA foreign_keys off / CREATE new_users / INSERT /
//     DROP / RENAME / PRAGMA on). Against its own applied database it answered
//     `Schemas are synced, no changes to be made.` A `schema apply` hand-off
//     between the two binaries never reached a fixed point.
//   - `schema inspect --format '{{ sql . }}'` over
//     `CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE,
//     c TEXT); CREATE UNIQUE INDEX ux_t_c ON t(c);` replayed into a fresh
//     database with four indexes where the source had three. The extra one,
//     `sqlite_autoindex_t_3`, was a phantom unique index on `c` that the source
//     never had, backing a constraint the author never wrote.
//   - `schema inspect --format '{{ json . }}'` reported the key column of
//     `id INTEGER PRIMARY KEY` as NOT NULL. The pinned binary reports
//     `"null": true`, and it was the only column in the fixture whose flag the
//     two disagreed about. SQLite is on the pinned binary's side: on a rowid
//     table `pragma table_info.notnull` is 0 for that column and an explicit
//     NULL insert is accepted, with a rowid assigned for it.
//
// After the fix, the same `schema apply` fixture makes the pinned binary answer
// `Schemas are synced, no changes to be made.` at exit 0, and the same inspect
// fixture replays to exactly the source's three indexes.

// inspectSQLiteFormat runs `atlas schema inspect` over dbPath with an explicit
// --format template and returns what reached stdout.
func inspectSQLiteFormat(tb testing.TB, dbPath, format string) string {
	c := qt.New(tb)
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", sqliteURLFromPath(dbPath),
		"--format", format,
	})
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	return out.String()
}

// sqliteIndexNames returns every index name the database reports, in name
// order, so a replayed dump can be compared with the source it came from.
func sqliteIndexNames(tb testing.TB, dbPath string) []string {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), sqliteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.QueryContext(
		context.Background(),
		"SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
	)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()

	names := []string{}
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// sqliteTableDDL returns the CREATE TABLE text SQLite persisted for table.
func sqliteTableDDL(tb testing.TB, dbPath, table string) string {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), sqliteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var ddl string
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&ddl)
	c.Assert(err, qt.IsNil)
	return ddl
}

// applySQLiteHCL runs `atlas schema apply --auto-approve` from an HCL file and
// returns the combined output. The dev URL names a path inside the test's own
// directory: `sqlite://dev?mode=memory` materializes a file called `dev` in the
// working directory, which for a package test is the package source tree.
func applySQLiteHCL(tb testing.TB, dbPath, hclPath string) string {
	c := qt.New(tb)
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", sqliteURLFromPath(dbPath),
		"--to", "file://" + hclPath,
		"--dev-url", sqliteURLFromPath(filepath.Join(filepath.Dir(dbPath), "dev.db")),
		"--auto-approve",
	})
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	return out.String()
}

const sqliteKeyNotNullHCL = `schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    null = false
    type = integer
  }
  column "name" {
    null = false
    type = text
  }
  primary_key {
    columns = [column.id]
  }
}
`

const sqliteKeyNullableHCL = `schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    null = true
    type = integer
  }
  column "name" {
    null = false
    type = text
  }
  primary_key {
    columns = [column.id]
  }
}
`

// TestSchemaApplySQLiteKeyColumnKeepsDeclaredNullability pins finding 5.1 in
// both directions: a declared NOT NULL survives onto the key column, and a
// declared nullable key column does not acquire one.
//
// The second row is the over-correction guard. Emitting NOT NULL beside every
// PRIMARY KEY would make the first row pass while writing a stricter table than
// the source asked for, which is the same class of defect in the other
// direction — a restored database that rejects rows the original accepted.
func TestSchemaApplySQLiteKeyColumnKeepsDeclaredNullability(t *testing.T) {
	tests := []struct {
		name        string
		hcl         string
		wantNotNull bool
	}{
		{
			name:        "declared not null survives onto the key column",
			hcl:         sqliteKeyNotNullHCL,
			wantNotNull: true,
		},
		{
			name:        "declared nullable key column stays nullable",
			hcl:         sqliteKeyNullableHCL,
			wantNotNull: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			hclPath := filepath.Join(dir, "users.hcl")
			c.Assert(os.WriteFile(hclPath, []byte(test.hcl), 0o600), qt.IsNil)
			dbPath := filepath.Join(dir, "main.db")

			applySQLiteHCL(c.TB, dbPath, hclPath)

			ddl := sqliteTableDDL(c.TB, dbPath, "users")
			c.Assert(keyColumnIsNotNull(c.TB, dbPath, "users", "id"), qt.Equals, test.wantNotNull,
				qt.Commentf("persisted DDL: %s", ddl))
		})
	}
}

// keyColumnIsNotNull reports what SQLite itself says about the column, read
// from pragma table_info rather than from the DDL text, so the assertion is
// about the database that was built and not about how it was spelled.
func keyColumnIsNotNull(tb testing.TB, dbPath, table, column string) bool {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), sqliteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var notNull int
	err = conn.QueryRowContext(
		context.Background(),
		`SELECT "notnull" FROM pragma_table_info(?) WHERE name = ?`,
		table, column,
	).Scan(&notNull)
	c.Assert(err, qt.IsNil)
	return notNull != 0
}

// TestSchemaInspectSQLiteSQLReplaysTheSourceIndexSet pins finding 5.2: the SQL
// rendering of an inspected database must replay to the index set it was read
// from, neither adding a constraint nor dropping one.
//
// Row one is the reported case, where uniqueness on `c` comes only from a named
// standalone index. Row two is the companion that keeps the fold from simply
// being deleted: a column that declares UNIQUE inline still round-trips through
// its own implicit index.
func TestSchemaInspectSQLiteSQLReplaysTheSourceIndexSet(t *testing.T) {
	tests := []struct {
		name      string
		schemaSQL string
		wantNames []string
	}{
		{
			name: "named unique index is not folded into the column",
			schemaSQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE, c TEXT);" +
				"CREATE UNIQUE INDEX ux_t_c ON t(c);",
			wantNames: []string{"sqlite_autoindex_t_1", "sqlite_autoindex_t_2", "ux_t_c"},
		},
		{
			name:      "declared column UNIQUE still round-trips",
			schemaSQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT UNIQUE);",
			wantNames: []string{"sqlite_autoindex_t_1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			sourcePath := filepath.Join(dir, "source.db")
			seedSQLiteSchema(c.TB, sourcePath, test.schemaSQL)
			c.Assert(sqliteIndexNames(c.TB, sourcePath), qt.DeepEquals, test.wantNames,
				qt.Commentf("fixture does not have the index set it claims"))

			rendered := inspectSQLiteFormat(c.TB, sourcePath, "{{ sql . }}")

			replayPath := filepath.Join(dir, "replay.db")
			seedSQLiteSchema(c.TB, replayPath, rendered)
			c.Assert(sqliteIndexNames(c.TB, replayPath), qt.DeepEquals, test.wantNames,
				qt.Commentf("rendered SQL: %s", rendered))
		})
	}
}

// TestSchemaInspectSQLiteJSONNullabilityMatchesTheCatalog pins finding 6.3:
// `{{ json . }}` reports nullability as SQLite reports it, with the key column
// of a rowid table nullable unless the DDL said otherwise.
//
// The pinned Atlas community v1.3.0 binary answers `"null": true` for the first
// row's `id` and omits the key for the second row's, which is what those rows
// assert. `null` is omitted rather than written false when the column is NOT
// NULL, so the assertion reads the key's presence.
//
// The STRICT and WITHOUT ROWID rows are the other half of the same question. The
// reader answers from `pragma table_info.notnull` alone, so it already reports
// them correctly; the rows are here so that a future normalization of key
// nullability on this path has to disagree with the catalog out loud. Their
// values are the catalog's: measured, a STRICT or WITHOUT ROWID key column
// reports notnull=1, and the rowid alias of a STRICT table still reports 0.
func TestSchemaInspectSQLiteJSONNullabilityMatchesTheCatalog(t *testing.T) {
	tests := []struct {
		name         string
		schemaSQL    string
		wantNullable map[string]bool
	}{
		{
			name:      "rowid alias key column is nullable",
			schemaSQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, note TEXT);",
			wantNullable: map[string]bool{
				"id": true, "name": false, "note": true,
			},
		},
		{
			name:      "declared NOT NULL key column is not",
			schemaSQL: "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, note TEXT);",
			wantNullable: map[string]bool{
				"id": false, "name": false, "note": true,
			},
		},
		{
			name:      "without rowid key column is not nullable",
			schemaSQL: "CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT NOT NULL, note TEXT) WITHOUT ROWID;",
			wantNullable: map[string]bool{
				"id": false, "name": false, "note": true,
			},
		},
		{
			name: "without rowid table level composite key is not nullable",
			schemaSQL: "CREATE TABLE t (team TEXT, member TEXT, note TEXT," +
				" PRIMARY KEY (team, member)) WITHOUT ROWID;",
			wantNullable: map[string]bool{
				"team": false, "member": false, "note": true,
			},
		},
		{
			name:      "strict key column is not nullable",
			schemaSQL: "CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT NOT NULL, note TEXT) STRICT;",
			wantNullable: map[string]bool{
				"id": false, "name": false, "note": true,
			},
		},
		{
			name: "strict table level composite key is not nullable",
			schemaSQL: "CREATE TABLE t (team TEXT, member TEXT, note TEXT," +
				" PRIMARY KEY (team, member)) STRICT;",
			wantNullable: map[string]bool{
				"team": false, "member": false, "note": true,
			},
		},
		{
			name:      "strict rowid alias key column is still nullable",
			schemaSQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, note TEXT) STRICT;",
			wantNullable: map[string]bool{
				"id": true, "name": false, "note": true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "nullability.db")
			seedSQLiteSchema(c.TB, dbPath, test.schemaSQL)

			rendered := inspectSQLiteFormat(c.TB, dbPath, "{{ json . }}")

			c.Assert(inspectJSONNullability(c.TB, rendered, "t"), qt.DeepEquals, test.wantNullable,
				qt.Commentf("rendered JSON: %s", rendered))
		})
	}
}

// inspectJSONNullability decodes the `{{ json . }}` document and returns each
// column's nullability for one table, reading an absent `null` key as NOT NULL
// the way the pinned binary writes it.
func inspectJSONNullability(tb testing.TB, document, table string) map[string]bool {
	c := qt.New(tb)
	c.Helper()
	var report struct {
		Schemas []struct {
			Tables []struct {
				Name    string `json:"name"`
				Columns []struct {
					Name string `json:"name"`
					Null bool   `json:"null"`
				} `json:"columns"`
			} `json:"tables"`
		} `json:"schemas"`
	}
	c.Assert(json.Unmarshal([]byte(document), &report), qt.IsNil)
	c.Assert(report.Schemas, qt.HasLen, 1)

	nullability := map[string]bool{}
	for _, reported := range report.Schemas[0].Tables {
		for _, column := range reported.Columns {
			nullability[column.Name] = column.Null
		}
	}
	return nullability
}
