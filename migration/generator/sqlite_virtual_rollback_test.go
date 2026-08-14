package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/generator"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPlanBidirectionalSchemaDiffGatesTheRollbackItGenerates is the seam test
// for the down-direction half of the SQLite virtual-table guard.
//
// internal/sqlitevirtual decides what a destructive rollback is; this proves
// the reverse plan is actually handed to it. The two are separate claims, and
// this one is the claim that was missing: the forward direction has been gated
// inside the comparison since the guard existed, while the reverse diff -- a
// second plan, written to a down file for `migrations down` to execute -- was
// derived here and planned with nothing looking at it.
//
// Reproduced end to end through [generator.PlanMigration] on an fts4 database
// this build cannot load, with a programmatic desired schema naming the
// module's storage and one extra column on `docs_content`. The up file was the
// single statement
//
//	ALTER TABLE "docs_content" ADD COLUMN "spurious" TEXT;
//
// which the up-direction gate admits and should, since SQLite performs it in
// place. The down file written beside it was
//
//	CREATE TABLE "__ptah_rebuild_docs_content" (...);
//	INSERT INTO "__ptah_rebuild_docs_content" (...) SELECT ... FROM "docs_content";
//	DROP TABLE "docs_content";
//	ALTER TABLE "__ptah_rebuild_docs_content" RENAME TO "docs_content";
//
// at exit 0. Restoring the pre-exemption gate refused the same run at the
// comparison, so the gap belongs to the exemption rather than to anything older.
//
// The database is hand-built rather than read from a file for the reason
// migration/schemadiff's fixtures give: no build of Ptah can create an fts4
// table, because refusing to link the module is the condition under test.
func TestPlanBidirectionalSchemaDiffGatesTheRollbackItGenerates(t *testing.T) {
	tests := []struct {
		name         string
		env          func(testing.TB)
		diff         *difftypes.SchemaDiff
		desired      *goschema.Database
		wantErr      bool
		wantContains []string
	}{
		{
			// THE ROW THIS SEAM EXISTS FOR.
			name: "a rollback that rebuilds the module's storage is refused",
			env:  envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:    "docs_content",
				ColumnsAdded: []string{"spurious"},
			}}},
			desired: declaredLiveTablesWithSpuriousColumn(),
			wantErr: true,
			wantContains: []string{
				`the rollback generated beside this migration changes "docs_content"`,
				`virtual table "docs" (module fts4)`,
				sqlitevirtual.AllowUnregisteredModuleEnvVar,
			},
		},
		{
			// THE CONTROL. A migration that adds an ordinary table has a
			// rollback that drops it, and that table cannot be storage the
			// module already owns. Refusing it would be the same over-refusal
			// three earlier rounds closed, arriving by a new route.
			name:    "a rollback that drops the table the migration added is not refused",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			diff:    &difftypes.SchemaDiff{TablesAdded: []string{"audit"}},
			desired: declaredLiveTablesWithAuditTable(),
			wantErr: false,
		},
		{
			// The second control: the forward plan is untouched by this seam.
			// A diff whose reverse removes nothing plans both directions as
			// before.
			name:    "a migration that changes nothing destructive keeps planning",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			diff:    &difftypes.SchemaDiff{IndexesAdded: []difftypes.IndexRef{{Name: "users_name_idx", TableName: "users"}}},
			desired: declaredLiveTablesWithUserIndex(),
			wantErr: false,
		},
		{
			// The opt-in reaches this seam too, so the capability the guard
			// takes away is restorable in the down direction as well as the up.
			name: "the opt-in lifts the rollback refusal",
			env:  envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "1"),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:    "docs_content",
				ColumnsAdded: []string{"spurious"},
			}}},
			desired: declaredLiveTablesWithSpuriousColumn(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			_, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff:          tt.diff,
				DesiredSchema: tt.desired,
				CurrentSchema: rollbackFTS4Database(),
				Dialect:       "sqlite",
			})

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			for _, fragment := range tt.wantContains {
				c.Assert(err.Error(), qt.Contains, fragment)
			}
		})
	}
}

// rollbackFTS4Database is a database whose fts4 index this build cannot load:
// the virtual table is still recognized as virtual, and the module's private
// storage arrives as an ordinary user table because only the module could have
// said otherwise.
func rollbackFTS4Database() *dbtypes.DBSchema {
	return &dbtypes.DBSchema{
		Tables: []dbtypes.DBTable{
			{Name: "docs", Type: "TABLE", VirtualModule: "fts4", VirtualArguments: "title, body"},
			{Name: "docs_content", Type: "TABLE", Columns: []dbtypes.DBColumn{
				{Name: "docid", DataType: "INTEGER", IsNullable: "YES", OrdinalPosition: 1},
			}},
			{Name: "users", Type: "TABLE", Columns: []dbtypes.DBColumn{
				{Name: "id", DataType: "INTEGER", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
				{Name: "name", DataType: "TEXT", IsNullable: "YES", OrdinalPosition: 2},
			}},
		},
		UnregisteredVirtualTables: []dbtypes.DBVirtualTable{{Name: "docs", Module: "fts4"}},
	}
}

// rollbackDeclaredLiveTables declares every live table, module storage
// included. Without that the comparison would not reach a plan at all: the
// pre-comparison half of the guard refuses an undeclared live table first.
func rollbackDeclaredLiveTables() *goschema.Database {
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
		},
	}
}

func declaredLiveTablesWithSpuriousColumn() *goschema.Database {
	desired := rollbackDeclaredLiveTables()
	desired.Fields = append(desired.Fields, goschema.Field{
		StructName: "DocContent", Name: "spurious", Type: "TEXT", Nullable: true,
	})
	return desired
}

func declaredLiveTablesWithAuditTable() *goschema.Database {
	desired := rollbackDeclaredLiveTables()
	desired.Tables = append(desired.Tables, goschema.Table{StructName: "Audit", Name: "audit"})
	desired.Fields = append(desired.Fields, goschema.Field{
		StructName: "Audit", Name: "id", Type: "INTEGER", Primary: true,
	})
	return desired
}

func declaredLiveTablesWithUserIndex() *goschema.Database {
	desired := rollbackDeclaredLiveTables()
	desired.Indexes = append(desired.Indexes, goschema.Index{
		StructName: "User", Name: "users_name_idx", Fields: []string{"name"},
	})
	return desired
}
