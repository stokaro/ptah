package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbschema/sqlite"
	"ptah.run/migration/schemadiff"
)

// valuedEnumSchema declares an ENUM column that carries its values and names
// no declared enum.
func valuedEnumSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "S", Name: "s", Type: "ENUM", Nullable: true, Enum: []string{"draft", "live"}},
		},
	}
}

// TestRenderedValuedEnumColumnConvergesThroughSQLite creates the table the
// render writes for that column and compares the declaration with what SQLite
// reports. The render and the comparison decide which columns are enums with
// one predicate, schemaprep.InlineEnumValues. A render that wrote a TEXT
// column without the CHECK the comparison expects left a change to plan.
func TestRenderedValuedEnumColumnConvergesThroughSQLite(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)

	statements, err := builtin.GetOrderedCreateStatements(valuedEnumSchema(), platform.SQLite)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	actual, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDialect(t.Context(), valuedEnumSchema(), actual, platform.SQLite, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("statements: %q, diff: %+v", statements, diff))
}
