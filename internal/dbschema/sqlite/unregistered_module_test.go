package sqlite_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/sqlite"
)

// TestReadSchemaRecordsVirtualTablesItCouldNotClassify is the read half of the
// data-loss fix for stokaro/ptah#1028.
//
// The measurement it exists for, taken end to end against master ebd2ba2e on an
// fts4 database built by a system SQLite that has the module. `PRAGMA
// table_list` read through the driver Ptah links, modernc.org/sqlite:
//
//	docs           virtual     <- recognized, because the DDL says so
//	docs_content   table       <- an FTS4 shadow table, reported as a user table
//	docs_docsize   table
//	docs_segdir    table
//	docs_segments  table
//	docs_stat      table
//
// The same read against an fts5 database reports all five as `shadow`. The
// difference is registration and nothing else: SQLite can only mark a shadow
// table while the module that owns it is loaded.
//
// Those five rows are what made the guard's own advice destructive. Ptah
// refused the comparison, told the operator to `--exclude docs`, and that exact
// command then planned and executed `DROP TABLE` for each of them at exit 0 --
// after which `MATCH` reported `SQL logic error` instead of a row.
//
// So the read records what it could not classify. The list is on the SCHEMA,
// not on a table, because the tables it warns about are the ones nothing marked.
func TestReadSchemaRecordsVirtualTablesItCouldNotClassify(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *sql.DB)
		want  []types.DBVirtualTable
	}{
		{
			// The control, and the reason this is not a rule about fts4: fts5
			// IS registered, so SQLite marked its shadow tables, the reader
			// classified everything, and there is nothing to record.
			name: "a registered module leaves the list empty",
			setup: func(t *testing.T, db *sql.DB) {
				execSQL(t, db, `CREATE VIRTUAL TABLE docs USING fts5(title, body)`)
			},
			want: nil,
		},
		{
			name: "an unregistered module is recorded with the table that uses it",
			setup: func(t *testing.T, db *sql.DB) {
				forgeCatalogRow(t, db, "docs", `CREATE VIRTUAL TABLE docs USING fts4(title, body)`)
			},
			want: []types.DBVirtualTable{{Name: "docs", Module: "fts4"}},
		},
		{
			// The class, stated as a test. Nothing here knows what fts3 or fts4
			// are; the question asked is whether THIS BUILD registers the
			// module, so a module invented for this row is treated the same way
			// as a real one it happens not to have.
			name: "a module no build has ever registered is recorded the same way",
			setup: func(t *testing.T, db *sql.DB) {
				forgeCatalogRow(t, db, "spatial", `CREATE VIRTUAL TABLE spatial USING acme_index(a, b)`)
			},
			want: []types.DBVirtualTable{{Name: "spatial", Module: "acme_index"}},
		},
		{
			// SQLite resolves a module name case-insensitively over ASCII:
			// `CREATE VIRTUAL TABLE t USING FTS5(a)` builds a real FTS5 index.
			// A byte comparison against the lowercase name PRAGMA module_list
			// reports would record this registered module as missing and refuse
			// a comparison with nothing wrong with it.
			name: "a registered module spelled in upper case is still registered",
			setup: func(t *testing.T, db *sql.DB) {
				execSQL(t, db, `CREATE VIRTUAL TABLE docs USING FTS5(title, body)`)
			},
			want: nil,
		},
		{
			// Ordering is by name, so a diagnostic built from the list does not
			// reshuffle between runs over one database.
			name: "several unclassifiable tables are recorded in name order",
			setup: func(t *testing.T, db *sql.DB) {
				forgeCatalogRow(t, db, "zeta", `CREATE VIRTUAL TABLE zeta USING fts3(a)`)
				forgeCatalogRow(t, db, "alpha", `CREATE VIRTUAL TABLE alpha USING fts4(b)`)
			},
			want: []types.DBVirtualTable{
				{Name: "alpha", Module: "fts4"},
				{Name: "zeta", Module: "fts3"},
			},
		},
		{
			// A database with no virtual table asks nothing and claims nothing.
			name: "an ordinary database records nothing",
			setup: func(t *testing.T, db *sql.DB) {
				execSQL(t, db, `CREATE TABLE orders (id INTEGER PRIMARY KEY)`)
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openFileDB(t)
			execSQL(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY)`)
			tt.setup(t, db)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()

			c.Assert(err, qt.IsNil)
			c.Assert(schema.UnregisteredVirtualTables, qt.DeepEquals, tt.want)
			// The rest of the read still works either way. This is the half of
			// the original defect that was an outright failure rather than a
			// wrong answer, and it must not come back as the price of the fix.
			c.Assert(findTable(schema.Tables, "users"), qt.IsNotNil)
		})
	}
}

// TestReadSchemaKeepsShadowTablesOutOfTheUnclassifiedList states the boundary
// the list draws.
//
// It names the tables Ptah could not classify, not the ones it could. An FTS5
// database has five shadow tables and they are all suppressed from the
// description entirely; recording them here as well would tell a caller that a
// fully understood database was not understood, and the comparison refusal
// reading this list would fire on every FTS5 database in existence.
func TestReadSchemaKeepsShadowTablesOutOfTheUnclassifiedList(t *testing.T) {
	c := qt.New(t)
	db := openFileDB(t)

	execSQL(t, db, `CREATE VIRTUAL TABLE docs USING fts5(title, body)`)
	c.Assert(catalogNamesOfKind(t, db, "shadow"), qt.DeepEquals,
		[]string{"docs_config", "docs_content", "docs_data", "docs_docsize", "docs_idx"})

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(schema.UnregisteredVirtualTables, qt.HasLen, 0)
	c.Assert(tableNames(schema.Tables), qt.DeepEquals, []string{"docs"})
}

// openFileDB opens a throwaway database in its own file inside the test's own
// directory, which the testing package removes afterwards.
//
// A file rather than `:memory:` because these fixtures stand in for an
// operator's database, and because a forged catalog row is written to storage.
func openFileDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "fixture.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	})
	return db
}
