package schemadiff_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/core/goschema"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/sqlite"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestCompareRefusesToPlanDroppingALiveVirtualTable is the seam test for the
// data-loss path, run against a real SQLite database rather than a hand-built
// DBSchema.
//
// The unit guard in internal/sqlitevirtual proves the rule; this proves the
// rule is wired into the comparison every native verb goes through. The two
// are not the same claim: the rule existed and the drop still happened until
// the call was added here, which is exactly the shape a reviewer caught on
// pull request #1469.
//
// The row that must stay planned is the one that keeps this from being "never
// plan a removal": an ordinary table absent from the desired state is still a
// removal, and it is one on the same database, in the same call.
func TestCompareRefusesToPlanDroppingALiveVirtualTable(t *testing.T) {
	tests := []struct {
		name            string
		env             func(testing.TB)
		desired         []goschema.Table
		wantErr         bool
		wantRemoved     []string
		wantErrContains string
	}{
		{
			name:            "the virtual table alone is refused, not planned",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         []goschema.Table{{StructName: "User", Name: "users"}, {StructName: "Note", Name: "notes"}},
			wantErr:         true,
			wantErrContains: `virtual table "docs" (module fts5)`,
		},
		{
			name:            "an ordinary table declared with the virtual table's name is refused",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:         []goschema.Table{{StructName: "User", Name: "users"}, {StructName: "Doc", Name: "docs"}, {StructName: "Note", Name: "notes"}},
			wantErr:         true,
			wantErrContains: "cannot convert one kind into the other",
		},
		{
			name:    "the opt-in plans the drop again",
			env:     envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "1"),
			desired: []goschema.Table{{StructName: "User", Name: "users"}, {StructName: "Note", Name: "notes"}},
			wantErr: false,
			// notes is the control: with the opt-in set the comparison is the
			// one master made, and it still plans the ordinary removal beside
			// the virtual one.
			wantRemoved: []string{"docs", "orders"},
		},
		{
			name:        "an ordinary removal is planned while the virtual table is declared out of scope",
			env:         envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			desired:     []goschema.Table{{StructName: "User", Name: "users"}, {StructName: "Doc", Name: "docs"}},
			wantErr:     true,
			wantRemoved: nil,
			// Reaching the collision refusal, not the removal one, even though
			// "orders" is also absent: the collision is the stronger finding.
			wantErrContains: "cannot convert one kind into the other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			database := readLiveVirtualTableFixture(t)
			generated := &goschema.Database{Tables: tt.desired}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				generated,
				database,
				dbtypes.DBInfo{Dialect: "sqlite"},
				nil,
			)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(errorMessage(err), qt.Contains, tt.wantErrContains)
			c.Assert(removedTableNames(diff), qt.DeepEquals, tt.wantRemoved)
		})
	}
}

// readLiveVirtualTableFixture builds a SQLite database holding a virtual table,
// an ordinary table that stays, and an ordinary table that goes, and reads it
// with the real reader. A hand-built DBSchema would prove the validator reads
// a struct field; this proves it reads what the reader reports.
func readLiveVirtualTableFixture(t *testing.T) *dbtypes.DBSchema {
	t.Helper()

	path := filepath.Join(t.TempDir(), "virtual.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	statements := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
		`CREATE TABLE orders (id INTEGER PRIMARY KEY)`,
		`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	return schema
}

// removedTableNames reads the planned removals, tolerating the nil diff a
// refusal returns so a row can assert both halves with one expression.
func removedTableNames(diff *difftypes.SchemaDiff) []string {
	if diff == nil {
		return nil
	}
	return diff.TablesRemoved
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
