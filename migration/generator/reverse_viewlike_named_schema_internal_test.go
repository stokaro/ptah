package generator

// White-box testing required: the rollback under test is produced by the
// unexported generateDownMigrationSQL from the unexported reverse plan builder,
// and the point of the test is what the down SQL says, which the exported
// GenerateMigration API only surfaces by writing files to disk.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateDownMigrationSQL_RestoresModifiedViewInANamedSchema pins the
// rollback of a modified view whose database side carries a schema name.
//
// Reaching the reverse diff is not the same as reaching the plan. The diff
// records a view by the name the Go schema spells, while the pre-change database
// schema the down direction plans against names it "<schema>.<view>"
// (dbschematypes.DBView.QualifiedName, applied by internal/convert/dbschematogo).
// A planner that compared those two strings found nothing for a view read from
// any named schema, and the modified entry rendered nothing at all -- the whole
// category rolled back to "No rollback operations needed" while the reflection
// gate stayed green, because that gate asserts the field reaches the reversed
// SchemaDiff and stops there.
//
// This is not a PostgreSQL corner. MySQL and MariaDB report a schema for EVERY
// view -- information_schema.VIEWS.TABLE_SCHEMA is the database name -- so the
// lookup missed on all of them. Measured live on MySQL 9.7 and MariaDB 11.8,
// with the view created bare and read back qualified:
//
//	before  DOWN  -- No rollback operations needed                      rc=0
//	              information_schema.VIEWS still held the post-up body
//	after   DOWN  CREATE OR REPLACE VIEW `db`.`probe_my_v` AS select ... rc=0
//	              information_schema.VIEWS back to the pre-up body
func TestGenerateDownMigrationSQL_RestoresModifiedViewInANamedSchema(t *testing.T) {
	c := qt.New(t)

	const priorBody = "SELECT id FROM rev_view_users"
	const targetBody = "SELECT id, email FROM rev_view_users"

	schema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "RevViewUser", Name: "rev_view_users"}},
		Fields: []goschema.Field{
			{StructName: "RevViewUser", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "RevViewUser", Name: "email", Type: "TEXT"},
		},
		Views: []goschema.View{
			{StructName: "RevActiveUsers", Name: "rev_active_users", Body: targetBody},
		},
	}
	goschema.Finalize(schema)

	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name: "rev_view_users",
			Type: "TABLE",
			Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "email", DataType: "text", IsNullable: "NO", OrdinalPosition: 2},
			},
		}},
		Views: []dbschematypes.DBView{
			{Name: "rev_active_users", Schema: "reporting", Body: priorBody},
		},
	}

	semantics := identifier.ForDialect("postgres")
	semantics.DefaultSchema = "reporting"
	upDiff, err := schemadiff.CompareWithDatabaseInfo(schema, db, dbschematypes.DBInfo{
		Dialect:             "postgres",
		IdentifierSemantics: semantics,
	}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(upDiff.ViewsModified, qt.HasLen, 1)

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "postgres")
	c.Assert(err, qt.IsNil)

	c.Assert(legacyRenderedSQL(downSQL), qt.Contains, priorBody,
		qt.Commentf("the rollback must restore the prior view body; down SQL:\n%s", downSQL))
}
