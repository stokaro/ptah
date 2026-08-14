package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareForwardsEveryDiffSkipTheVirtualTableGuardReads is the seam test
// for the policy wiring, and it is a separate claim from the unit guard.
//
// internal/sqlitevirtual decides what a [sqlitevirtual.Policy] means; this
// proves the comparison actually builds one from the caller's
// [config.CompareOptions]. The two came apart once already: the guard learned
// about `skip drop_table` while the seam still handed it a zero policy, and the
// refusal it produced was for statements the caller deletes again.
//
// Reproduced on the command before the column-drop half was forwarded:
// `ptah migrations generate` with `diff.skip: [drop_table, drop_column]`
// against an fts4 database, whose desired state drops one column from an
// ordinary `users` table, exited 2 -- while the same run with
// PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE=1 exited 0 and wrote no
// migration file at all, because the policy had already emptied the plan.
//
// The database is hand-built rather than read from a file, because no build of
// Ptah can create an fts4 table: refusing to link the module is the condition
// under test.
func TestCompareForwardsEveryDiffSkipTheVirtualTableGuardReads(t *testing.T) {
	tests := []struct {
		name     string
		database *dbtypes.DBSchema
		desired  *goschema.Database
		options  *config.CompareOptions
		wantErr  bool
	}{
		{
			// The control for the pair below. `skip drop_table` alone leaves
			// the column drop in the plan, and SQLite converges a removed
			// column by rebuilding the table, which on the module's own
			// storage destroys the index.
			name:     "a column drop is refused when only table drops are skipped",
			database: unclassifiableFTS4Database(),
			desired:  desiredWithoutTheLegacyColumn(),
			options: &config.CompareOptions{
				Dialect:        "sqlite",
				SkipTableDrops: true,
			},
			wantErr: true,
		},
		{
			name:     "the same column drop is admitted when the caller skips column drops",
			database: unclassifiableFTS4Database(),
			desired:  desiredWithoutTheLegacyColumn(),
			options: &config.CompareOptions{
				Dialect:         "sqlite",
				SkipTableDrops:  true,
				SkipColumnDrops: true,
			},
			wantErr: false,
		},
		{
			// The index half, which the guard only started counting once
			// review pointed out that `removeIndexes` renders DROP INDEX for a
			// table it is not rebuilding. Measured on the command: against an
			// fts4 database whose desired side no longer names an index on the
			// module's storage, `ptah schema diff` planned
			// `DROP INDEX IF EXISTS "docs_content_title_idx";` at exit 0.
			name:     "an index drop is refused when only table drops are skipped",
			database: unclassifiableFTS4DatabaseWithIndex(),
			desired:  desiredWithoutTheIndex(),
			options: &config.CompareOptions{
				Dialect:        "sqlite",
				SkipTableDrops: true,
			},
			wantErr: true,
		},
		{
			name:     "the same index drop is admitted when the caller skips index drops",
			database: unclassifiableFTS4DatabaseWithIndex(),
			desired:  desiredWithoutTheIndex(),
			options: &config.CompareOptions{
				Dialect:        "sqlite",
				SkipTableDrops: true,
				SkipIndexDrops: true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar)(t)
			envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)

			_, err := schemadiff.CompareWithDatabaseInfo(
				tt.desired,
				tt.database,
				dbtypes.DBInfo{Dialect: "sqlite"},
				tt.options,
			)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
		})
	}
}

// unclassifiableFTS4Database is the description a build without fts4 produces
// for a database holding one: the virtual table itself, the module's storage
// described as an ordinary table, and the reader's record of what it could not
// classify.
func unclassifiableFTS4Database() *dbtypes.DBSchema {
	return &dbtypes.DBSchema{
		Tables: []dbtypes.DBTable{
			{Name: "docs", Type: "TABLE", VirtualModule: "fts4", VirtualArguments: "title, body"},
			{Name: "docs_content", Type: "TABLE", Columns: []dbtypes.DBColumn{
				{Name: "docid", DataType: "INTEGER", IsNullable: "YES", OrdinalPosition: 1},
			}},
			{Name: "users", Type: "TABLE", Columns: []dbtypes.DBColumn{
				{Name: "id", DataType: "INTEGER", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
				{Name: "name", DataType: "TEXT", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "legacy", DataType: "TEXT", IsNullable: "YES", OrdinalPosition: 3},
			}},
		},
		UnregisteredVirtualTables: []dbtypes.DBVirtualTable{
			{Name: "docs", Module: "fts4"},
		},
	}
}

// unclassifiableFTS4DatabaseWithIndex is the same database carrying an explicit
// index on the module's storage. Nothing here can say whether the module put it
// there, which is the point.
func unclassifiableFTS4DatabaseWithIndex() *dbtypes.DBSchema {
	database := unclassifiableFTS4Database()
	database.Indexes = []dbtypes.DBIndex{{
		Name:      "docs_content_docid_idx",
		TableName: "docs_content",
		Columns:   []string{"docid"},
	}}
	return database
}

// declaredLiveTables declares every live table this comparison must not be
// refused for. The module's storage is declared on purpose: without it the
// comparison never reaches the post-diff gate, because the pre-comparison half
// refuses an undeclared live table first.
func declaredLiveTables() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Doc", Name: "docs", VirtualModule: "fts4", VirtualArguments: "title, body"},
			{StructName: "DocContent", Name: "docs_content"},
			{StructName: "User", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "DocContent", Name: "docid", Type: "INTEGER", Nullable: true},
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "name", Type: "TEXT", Nullable: true},
			{StructName: "User", Name: "legacy", Type: "TEXT", Nullable: true},
		},
	}
}

// desiredWithoutTheLegacyColumn drops one column from the ordinary table and
// changes nothing else, so the diff carries exactly one ColumnsRemoved entry.
func desiredWithoutTheLegacyColumn() *goschema.Database {
	desired := declaredLiveTables()
	desired.Fields = desired.Fields[:len(desired.Fields)-1]
	return desired
}

// desiredWithoutTheIndex leaves every table and column as the database has
// them and omits only the index, so the diff carries exactly one IndexesRemoved
// entry and no table is dropped or rebuilt.
func desiredWithoutTheIndex() *goschema.Database {
	return declaredLiveTables()
}
