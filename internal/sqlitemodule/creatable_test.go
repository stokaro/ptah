package sqlitemodule_test

import (
	"database/sql"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/internal/sqlitemodule"
)

// creatableRow is one registered module and a CREATE VIRTUAL TABLE that
// exercises it, because the modules do not take the same arguments.
type creatableRow struct {
	// module is the name PRAGMA module_list reports, and the key the guard
	// below matches against.
	module string
	// setup prepares whatever the create needs. fts5vocab is the only module
	// here that needs another object to point at.
	setup func(*testing.T, *sql.DB)
	// create is the statement under test.
	create string
}

func creatableRows() []creatableRow {
	nothing := func(*testing.T, *sql.DB) {}
	return []creatableRow{
		{module: "dbstat", setup: nothing, create: `CREATE VIRTUAL TABLE probe USING dbstat`},
		{module: "fts5", setup: nothing, create: `CREATE VIRTUAL TABLE probe USING fts5(a, b)`},
		{
			module: "fts5vocab",
			setup: func(t *testing.T, db *sql.DB) {
				execProbe(t, db, `CREATE VIRTUAL TABLE words USING fts5(x)`)
			},
			create: `CREATE VIRTUAL TABLE probe USING fts5vocab(words, 'row')`,
		},
		{module: "geopoly", setup: nothing, create: `CREATE VIRTUAL TABLE probe USING geopoly(a)`},
		{module: "rtree", setup: nothing, create: `CREATE VIRTUAL TABLE probe USING rtree(id, x0, x1)`},
		{module: "rtree_i32", setup: nothing, create: `CREATE VIRTUAL TABLE probe USING rtree_i32(id, x0, x1)`},
		{module: "sqlite_dbpage", setup: nothing, create: `CREATE VIRTUAL TABLE probe USING sqlite_dbpage`},
	}
}

// TestEveryRegisteredModuleCanCreateAVirtualTable checks the premise the
// desired-side refusal in [go.5x5.cz/ptah/internal/sqlitevirtual] rests on:
// that "this build registers the module" answers "this build can execute the
// CREATE VIRTUAL TABLE a plan would carry".
//
// The two are not the same property in general. SQLite has eponymous-only
// modules, which appear in PRAGMA module_list and implement no xCreate, so a
// CREATE VIRTUAL TABLE naming one fails even though the module is registered --
// and a guard reading registration alone would wave through exactly the
// mid-apply failure it exists to prevent. Raised in review on stokaro/ptah#1513
// against sqlite_dbpage specifically.
//
// Measured here rather than argued: on modernc.org/sqlite v1.56.0 every module
// in the set, sqlite_dbpage and dbstat included, creates a persistent virtual
// table that PRAGMA table_list then reports as `virtual`. So the premise holds
// on this build, and this test is what will say so if a dependency bump adds a
// module for which it does not.
func TestEveryRegisteredModuleCanCreateAVirtualTable(t *testing.T) {
	for _, row := range creatableRows() {
		t.Run(row.module, func(t *testing.T) {
			c := qt.New(t)
			db := openFileDB(t)
			row.setup(t, db)

			_, err := db.Exec(row.create)

			c.Assert(err, qt.IsNil, qt.Commentf(
				"module %q is registered but cannot own a created virtual table;"+
					" the desired-side refusal in internal/sqlitevirtual reads registration"+
					" as creatability and would let this reach the database", row.module))
			// Created is not connected: read the catalog back, because a module
			// that accepted the statement without producing a virtual table
			// would satisfy the assertion above and still break a replay.
			var kind string
			err = db.QueryRow(
				`SELECT type FROM pragma_table_list WHERE schema = 'main' AND name = 'probe'`,
			).Scan(&kind)
			c.Assert(err, qt.IsNil)
			c.Assert(kind, qt.Equals, "virtual")
		})
	}
}

// TestCreatableRowsCoverEveryRegisteredModule keeps the table above a census
// rather than a list somebody remembered to extend.
//
// A driver bump that registers a new module has no row here until it is
// measured, and this is what names it. That matters more than usual because the
// new module could be the eponymous-only one the premise does not survive.
func TestCreatableRowsCoverEveryRegisteredModule(t *testing.T) {
	c := qt.New(t)

	registered, err := sqlitemodule.Registered()
	c.Assert(err, qt.IsNil)

	covered := make([]string, 0, len(creatableRows()))
	for _, row := range creatableRows() {
		covered = append(covered, row.module)
	}
	slices.Sort(covered)

	c.Assert(covered, qt.DeepEquals, registered.Names())
}

func execProbe(t *testing.T, db *sql.DB, statement string) {
	t.Helper()

	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("exec %q: %v", statement, err)
	}
}
