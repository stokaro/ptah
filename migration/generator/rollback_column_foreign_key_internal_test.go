package generator

// White-box testing required: generateDownMigrationSQL is package-local, and
// the property is about what the reversal renders rather than about any
// exported entry point.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateDownMigrationSQL_RestoresAColumnsForeignKeyOnce is the
// regression for stokaro/ptah#2404.
//
// Two independent paths recovered the same foreign key for a rolled-back
// column. The desired side of a reversal is built from the database read, which
// puts the key on the FIELD, and the constraint comparison reports the same key
// as a removal, which the reversal turns into an addition. Each is right on its
// own; nothing paired them, so the rollback carried `ADD CONSTRAINT` twice and
// PostgreSQL answers the second with `constraint ... already exists`.
//
// The column change carries the column now, and it deliberately carries no
// keys: the constraint comparison owns them. The count is what this asserts,
// because one statement and two statements both "contain" the constraint.
func TestGenerateDownMigrationSQL_RestoresAColumnsForeignKeyOnce(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "U", Name: "users"},
			{StructName: "O", Name: "orders"},
		},
		Fields: []schemamodel.Field{
			{StructName: "U", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "O", Name: "id", Type: "INTEGER", Primary: true},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "users", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "orders", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "owner_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "fk_orders_owner", Type: "FOREIGN KEY", TableName: "orders",
			ColumnName: "owner_id", ColumnNames: []string{"owner_id"},
			ForeignTable: new("users"), ForeignColumn: new("id"),
			ForeignColumns: []string{"id"},
			DeleteRule:     new("CASCADE"),
		}},
	}

	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.TablesModified, qt.HasLen, 1)
	c.Assert(upDiff.TablesModified[0].ColumnsRemoved.Names(), qt.DeepEquals, []string{"owner_id"})

	down, err := generateDownMigrationSQL(upDiff, desired, database, platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(down, `ADD CONSTRAINT "fk_orders_owner"`), qt.Equals, 1,
		qt.Commentf("the second one cannot apply: the constraint already exists\n%s", down))
	c.Assert(down, qt.Contains, `ADD COLUMN "owner_id" integer`,
		qt.Commentf("and the column still comes back with its type, which is what the carry is for"))
}
