package sqlitemodule_test

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/internal/sqlitemodule"
)

// TestRegisteredIsTheBuildsRealModuleSet pins the answer the refusal in
// [go.5x5.cz/ptah/internal/sqlitevirtual] is built on.
//
// The list is written out rather than derived, because deriving it from the
// same query it checks would assert nothing. It is the set measured with
// `PRAGMA module_list` against modernc.org/sqlite v1.56.0, and it is the reason
// an fts4 database cannot be classified by this build: fts3 and fts4 are not in
// it. A dependency bump that adds or removes a module reddens this test, which
// is the point -- docs/sqlite.md quotes this set to operators.
func TestRegisteredIsTheBuildsRealModuleSet(t *testing.T) {
	c := qt.New(t)

	registered, err := sqlitemodule.Registered()

	c.Assert(err, qt.IsNil)
	c.Assert(registered.Names(), qt.DeepEquals, []string{
		"dbstat", "fts5", "fts5vocab", "geopoly", "rtree", "rtree_i32", "sqlite_dbpage",
	})
}

// TestRegisteredExcludesPragmaFunctions is the guard on the measurement that
// decided how this query is spelled.
//
// `SELECT name FROM pragma_module_list` registers that table-valued function as
// an eponymous module, which then appears in its own answer -- and it stays
// registered, so even a later `PRAGMA module_list` on the same connection
// reports it. Measured on three fresh connections: 7 rows for the pragma form,
// 8 for the function form, 8 for the pragma form after the function form ran.
//
// Nothing a `CREATE VIRTUAL TABLE` can name is called `pragma_anything`, so the
// only thing those rows can do is make a diagnostic tell an operator this build
// supports a module it does not have. The fixture below reproduces the
// contamination deliberately, then requires the answer to be clean anyway.
func TestRegisteredExcludesPragmaFunctions(t *testing.T) {
	c := qt.New(t)
	db := openFileDB(t)

	// Contaminate the connection exactly as the SQLite reader does: it queries
	// pragma_table_list through the function form before it ever asks this
	// question. Without the filter the assertions below see that function.
	primeWithPragmaFunctions(t, db)

	registered, err := sqlitemodule.RegisteredOn(db)

	c.Assert(err, qt.IsNil)
	for _, name := range registered.Names() {
		c.Assert(strings.HasPrefix(name, "pragma_"), qt.IsFalse,
			qt.Commentf("module %q is a pragma table-valued function, not a module a schema can use", name))
	}
	c.Assert(registered.Names(), qt.Contains, "fts5")
	c.Assert(registered.Registers("pragma_module_list"), qt.IsFalse)
	c.Assert(registered.Registers("pragma_table_list"), qt.IsFalse)
}

// TestRegistersFoldsASCIICase is the measurement that a byte comparison would
// get wrong.
//
// `CREATE VIRTUAL TABLE t USING FTS5(a)` succeeds on a real database and builds
// a genuine FTS5 index with all five shadow tables, `sqlite_schema` records the
// module verbatim as `FTS5`, and `PRAGMA module_list` reports `fts5`. Comparing
// those two spellings byte for byte reports a registered module as missing and
// refuses a comparison that has nothing wrong with it.
//
// The non-ASCII row is the other half. SQLite folds ASCII only, so a Unicode
// fold here would answer for a module the engine would not resolve.
func TestRegistersFoldsASCIICase(t *testing.T) {
	tests := []struct {
		name   string
		module string
		want   bool
	}{
		{name: "the spelling module_list reports", module: "fts5", want: true},
		{name: "the spelling a CREATE statement may carry", module: "FTS5", want: true},
		{name: "a mixed spelling SQLite also accepts", module: "FtS5", want: true},
		{name: "an underscored module keeps its underscore", module: "RTREE_I32", want: true},
		{name: "a module this build does not register", module: "fts4", want: false},
		{name: "the other unregistered full-text module", module: "fts3", want: false},
		{name: "the empty module is nothing", module: "", want: false},
		{
			// Not a fold SQLite performs. Turkish dotless i lowercases to `i`
			// under Unicode rules, which would answer for `rtree_i32` here.
			name:   "a Unicode fold is not applied",
			module: "RTREE_İ32",
			want:   false,
		},
	}

	c := qt.New(t)

	registered, err := sqlitemodule.Registered()
	c.Assert(err, qt.IsNil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(registered.Registers(tt.module), qt.Equals, tt.want)
		})
	}
}

// TestZeroSetRegistersNothing pins the direction the zero value fails in.
//
// A caller that forgot to fill a Set must refuse to classify anything rather
// than quietly report every module present, because the second answer is the
// one that plans DROP TABLE for a module's storage.
func TestZeroSetRegistersNothing(t *testing.T) {
	c := qt.New(t)

	var empty sqlitemodule.Set

	c.Assert(empty.Registers("fts5"), qt.IsFalse)
	c.Assert(empty.Names(), qt.HasLen, 0)
	c.Assert(empty.String(), qt.Equals, "")
}

// TestStringRendersTheSetForADiagnostic keeps the rendering stable, because the
// refusal prints it to tell an operator what this build can do.
func TestStringRendersTheSetForADiagnostic(t *testing.T) {
	c := qt.New(t)

	registered, err := sqlitemodule.Registered()

	c.Assert(err, qt.IsNil)
	c.Assert(registered.String(), qt.Equals,
		"dbstat, fts5, fts5vocab, geopoly, rtree, rtree_i32, sqlite_dbpage")
}

// TestNamesDoesNotAliasTheSet stops a caller from editing the answer every
// other caller reads, which matters because [sqlitemodule.Registered] is
// computed once and shared.
func TestNamesDoesNotAliasTheSet(t *testing.T) {
	c := qt.New(t)

	registered, err := sqlitemodule.Registered()
	c.Assert(err, qt.IsNil)

	names := registered.Names()
	c.Assert(names, qt.Not(qt.HasLen), 0)
	names[0] = "clobbered"

	c.Assert(registered.Names()[0], qt.Equals, "dbstat")
}

// openFileDB opens a throwaway database in its own file, removed with the test
// directory. A file rather than `:memory:` so the fixture is the shape an
// operator's database has.
func openFileDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "modules.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close sqlite: %v", err)
		}
	})
	return db
}

// primeWithPragmaFunctions runs the pragma table-valued functions the SQLite
// reader runs, so the connection under test carries the eponymous modules they
// register.
func primeWithPragmaFunctions(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, query := range []string{
		"SELECT name FROM pragma_table_list",
		"SELECT name FROM pragma_module_list",
	} {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("prime %q: %v", query, err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close %q: %v", query, err)
		}
	}
}
