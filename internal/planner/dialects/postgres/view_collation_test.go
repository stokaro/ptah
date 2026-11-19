package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestCollationChangeUnderView_DropsThenRecreatesTheView pins the plan for a
// change of items.sku's collation alone under a view that reads the column.
// The change is made with ALTER COLUMN ... TYPE, which PostgreSQL 18.6 answers
// with "cannot alter type of a column used by a view or rule" whether or not
// the type changes, so the view leaves before the ALTER and returns after it.
func TestCollationChangeUnderView_DropsThenRecreatesTheView(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "integer", Primary: true},
			{StructName: "Item", Name: "sku", Type: "varchar", Collate: "C"},
		},
		Views: []schemamodel.View{{Name: "item_skus", Body: "SELECT id, sku FROM items"}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "items",
			Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "sku", DataType: "character varying", UDTName: "varchar", IsNullable: "NO"},
			},
		}},
		Views: []catalog.View{{Name: "item_skus", Schema: "public", Body: "SELECT items.id, items.sku FROM items"}},
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, database,
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "postgres")

	c.Assert(err, qt.IsNil)
	plan := strings.Join(statements, "\n")
	alter := indexOfStatement(statements, `ALTER COLUMN "sku" TYPE varchar COLLATE "C"`)
	drop := indexOfStatement(statements, `DROP VIEW IF EXISTS "item_skus"`)
	create := indexOfStatement(statements, `VIEW "item_skus" AS`)
	c.Assert(alter >= 0, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(drop >= 0 && drop < alter, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
	c.Assert(create > alter, qt.IsTrue, qt.Commentf("plan:\n%s", plan))
}
