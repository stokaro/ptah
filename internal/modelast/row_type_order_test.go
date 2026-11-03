package modelast_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/builtinlowering"
	"ptah.run/internal/modelast"
)

// rowTypeDatabase declares the tables keep and t, the composite type wrap
// whose attribute r is the row type of t, and the fields more adds.
func rowTypeDatabase(more ...schemamodel.Field) schemamodel.Database {
	database := schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Keep", Name: "keep"}, {StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "Keep", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "a", Type: "INTEGER", Nullable: true},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "wrap"),
			Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "r", Type: "t"}, {Name: "n", Type: "int"}}}})),
	}
	for _, field := range more {
		database.Tables = append(database.Tables, schemamodel.Table{StructName: field.StructName, Name: strings.ToLower(field.StructName)})
		database.Fields = append(database.Fields, field)
	}
	return database
}

func renderPostgres(database schemamodel.Database) (string, error) {
	lowering := builtinlowering.ForTarget(context.Background(), must.Must(builtin.New()), "postgres", capability.ForDialect("postgres"))
	statements, err := modelast.CollectDatabase(database, "postgres", lowering)
	if err != nil {
		return "", err
	}
	return builtin.RenderSQL("postgres", statements.Statements...)
}

// TestCollectDatabase_TypeOverATableRowType_HappyPath pins the render of a
// composite type whose attribute is a table's row type: the type
// follows the table, which it names, where every other type precedes the
// tables. Rendered first, it answered `type "t" does not exist`.
func TestCollectDatabase_TypeOverATableRowType_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderPostgres(rowTypeDatabase())

	c.Assert(err, qt.IsNil)
	table, typ := strings.Index(sql, `CREATE TABLE "t"`), strings.Index(sql, `CREATE TYPE "wrap" AS ("r" t, "n" int)`)
	c.Assert(table, qt.Not(qt.Equals), -1, qt.Commentf("%s", sql))
	c.Assert(typ > table, qt.IsTrue, qt.Commentf("%s", sql))
}

// TestCollectDatabase_TypeOverATableRowType_FailurePath pins the render no
// order serves: a table whose column is such a type would have to be
// created after the type, which waits for every table. The render is refused
// and names the type, the table it names and the table that uses it.
func TestCollectDatabase_TypeOverATableRowType_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderPostgres(rowTypeDatabase(schemamodel.Field{StructName: "T2", Name: "w", Type: "wrap", Nullable: true}))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*composite type wrap depends on the row type of table t, which the plan creates; table t2 uses it, .*`)
	c.Assert(sql, qt.Equals, "")
}
