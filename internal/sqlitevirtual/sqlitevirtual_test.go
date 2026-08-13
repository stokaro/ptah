package sqlitevirtual_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

// TestValidateComparison is the guard on the data-loss path measured for
// stokaro/ptah#1028.
//
// On master, and on this branch before this guard existed,
// `ptah schema apply --auto-approve` against a desired state that did not name
// a live FTS5 index planned `DROP TABLE IF EXISTS "docs"` and ran it: the
// catalog went from seven rows to one and the three indexed rows were gone,
// with `no such table: docs` afterwards. No desired-state format can declare a
// virtual table -- the native SQL parser refuses `CREATE VIRTUAL TABLE` with
// `unsupported CREATE target: VIRTUAL` -- so the absence the comparator read as
// deletion intent was the only thing the operator could have written.
//
// The rows below carry both directions of the escape, because a refusal the
// operator cannot get past is its own defect: [sqlitevirtual.AllowDropEnvVar]
// restores the drop, and excluding the table from the comparison keeps it
// (measured on the command, not here -- the exclusion happens before this
// validator is reached).
func TestValidateComparison(t *testing.T) {
	fts5 := types.DBTable{Name: "docs", Type: "TABLE", VirtualModule: "fts5", VirtualArguments: "title, body"}
	rtree := types.DBTable{Name: "geo", Type: "TABLE", VirtualModule: "rtree", VirtualArguments: "id, x0, x1"}
	users := types.DBTable{Name: "users", Type: "TABLE"}

	tests := []struct {
		name            string
		dialect         string
		env             func(testing.TB)
		desired         *goschema.Database
		database        []types.DBTable
		wantErr         bool
		wantUnsupported bool
		wantContains    []string
	}{
		{
			name:            "a live virtual table the desired state does not declare is refused",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("users"),
			database:        []types.DBTable{fts5, users},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`virtual table "docs" (module fts5)`,
				"the desired schema does not declare",
				"would delete the index and everything in it",
				sqlitevirtual.AllowDropEnvVar,
			},
		},
		{
			// The rule is about virtual tables, not about FTS5. A module that
			// indexes geometry is refused on the same grounds.
			name:            "a module that is not fts5 is refused the same way",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("users"),
			database:        []types.DBTable{rtree, users},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`virtual table "geo" (module rtree)`},
		},
		{
			name:            "every offending table is named, not just the first",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("users"),
			database:        []types.DBTable{fts5, rtree, users},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`"docs" (module fts5)`, `"geo" (module rtree)`, "virtual tables"},
		},
		{
			name:     "the opt-in restores the removal",
			dialect:  "sqlite",
			env:      envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "1"),
			desired:  declaring("users"),
			database: []types.DBTable{fts5, users},
			wantErr:  false,
		},
		{
			// A valid false spelling keeps the refusal, which is the difference
			// between "set to off" and "unset".
			name:            "an explicit false keeps the refusal",
			dialect:         "sqlite",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "false"),
			desired:         declaring("users"),
			database:        []types.DBTable{fts5, users},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`virtual table "docs" (module fts5)`},
		},
		{
			name:         "a value that is not a boolean is a configuration error",
			dialect:      "sqlite",
			env:          envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe"),
			desired:      declaring("users"),
			database:     []types.DBTable{fts5, users},
			wantErr:      true,
			wantContains: []string{sqlitevirtual.AllowDropEnvVar, "maybe"},
		},
		{
			name:            "a desired ordinary table colliding with a live virtual one is refused",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("users", "docs"),
			database:        []types.DBTable{fts5, users},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`declares "docs" as an ordinary table`,
				"cannot convert one kind into the other",
				"ALTER TABLE statements SQLite refuses on a virtual table",
			},
		},
		{
			// The opt-in says "you may drop it". It cannot say "you may turn a
			// full-text index into a table", so the collision outlives it.
			name:            "the collision is refused even with the opt-in set",
			dialect:         "sqlite",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "1"),
			desired:         declaring("users", "docs"),
			database:        []types.DBTable{fts5, users},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"cannot convert one kind into the other"},
		},
		{
			// SQLite matches table names case-insensitively, so a declaration
			// spelled DOCS collides with a virtual docs and must not fall
			// through to the removal branch, which the opt-in could then waive.
			name:            "the collision is recognized across letter case",
			dialect:         "sqlite",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "1"),
			desired:         declaring("DOCS"),
			database:        []types.DBTable{fts5},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"cannot convert one kind into the other"},
		},
		{
			// The variable is validated on every SQLite comparison, not only on
			// the ones that hold a virtual table. A pipeline carrying a typo'd
			// opt-in must hear about it on the run that carries it, not on the
			// distant day someone adds an FTS5 index.
			name:         "a malformed value is refused even with no virtual table present",
			dialect:      "sqlite",
			env:          envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe"),
			desired:      declaring("users"),
			database:     []types.DBTable{users},
			wantErr:      true,
			wantContains: []string{sqlitevirtual.AllowDropEnvVar, "maybe"},
		},
		{
			// Measured, not reasoned: `CREATE VIRTUAL TABLE "\u00c4"` and
			// `CREATE TABLE "\u00e4"` both succeed in one SQLite database and
			// PRAGMA table_list reports two tables, while `CREATE TABLE DOCS`
			// beside `docs` fails with `table DOCS already exists`. SQLite folds
			// ASCII only. Folding Unicode here would invent a collision the
			// engine does not see and refuse a sound comparison.
			name:            "a Unicode near-twin is a different table, not a collision",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("\u00e4"),
			database:        []types.DBTable{{Name: "\u00c4", Type: "TABLE", VirtualModule: "fts5", VirtualArguments: "body"}},
			wantErr:         true,
			wantUnsupported: true,
			// The removal refusal, not the collision one: the two names are two
			// tables, so the virtual one is simply undeclared.
			wantContains: []string{"the desired schema does not declare"},
		},
		{
			// The ASCII control for the row above, on the same code path.
			name:            "an ASCII near-twin IS a collision",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("DOCS"),
			database:        []types.DBTable{fts5},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"cannot convert one kind into the other"},
		},
		{
			name:     "a database with no virtual table and a sound value is untouched",
			dialect:  "sqlite",
			env:      envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "false"),
			desired:  declaring("users"),
			database: []types.DBTable{users},
			wantErr:  false,
		},
		{
			name:     "a non-SQLite comparison does not invoke the subsystem, malformed value and all",
			dialect:  "postgres",
			env:      envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe"),
			desired:  declaring("users"),
			database: []types.DBTable{fts5, users},
			wantErr:  false,
		},
		{
			name:            "sqlite3 is the same dialect",
			dialect:         "sqlite3",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         declaring("users"),
			database:        []types.DBTable{fts5},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`virtual table "docs" (module fts5)`,
			},
		},
		{
			name:            "a nil desired state declares nothing and still refuses",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         nil,
			database:        []types.DBTable{fts5},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`virtual table "docs" (module fts5)`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			database := &types.DBSchema{Tables: tt.database}

			err := sqlitevirtual.ValidateComparison(tt.dialect, tt.desired, database)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(errors.Is(err, ptaherr.ErrUnsupportedFeature), qt.Equals, tt.wantUnsupported)
			for _, fragment := range tt.wantContains {
				c.Assert(errorText(err), qt.Contains, fragment)
			}
		})
	}
}

// TestTables reports the virtual tables in a stable order, so a diagnostic
// built from them does not reorder between runs over the same database.
func TestTables(t *testing.T) {
	tests := []struct {
		name     string
		database *types.DBSchema
		want     []sqlitevirtual.Table
	}{
		{
			name:     "a nil schema has none",
			database: nil,
			want:     nil,
		},
		{
			name: "ordinary tables are not virtual tables",
			database: &types.DBSchema{Tables: []types.DBTable{
				{Name: "users"},
				{Name: "docs_backup"},
			}},
			want: nil,
		},
		{
			name: "the order is by schema then name, not catalog order",
			database: &types.DBSchema{Tables: []types.DBTable{
				{Name: "zeta", VirtualModule: "rtree"},
				{Name: "alpha", VirtualModule: "fts5"},
				{Name: "beta", Schema: "aux", VirtualModule: "geopoly"},
			}},
			want: []sqlitevirtual.Table{
				{Name: "alpha", Module: "fts5"},
				{Name: "zeta", Module: "rtree"},
				{Schema: "aux", Name: "beta", Module: "geopoly"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlitevirtual.Tables(tt.database), qt.DeepEquals, tt.want)
		})
	}
}

// declaring builds a desired state naming the given tables. Only the names
// matter here: nothing a desired state can spell makes a table virtual.
func declaring(names ...string) *goschema.Database {
	tables := make([]goschema.Table, 0, len(names))
	for _, name := range names {
		tables = append(tables, goschema.Table{Name: name})
	}
	return &goschema.Database{Tables: tables}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
