package generator

// White-box testing required: downMigrationSQL is package-local, and the
// property is about which table and column the reversal resolves a column
// change against, which only the rendered rollback shows.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestGenerateDownMigrationSQL_RestoresAColumnTheComparisonMatchedByIdentity
// is the rollback of a column change where the declaration and the database
// read spell the table or the column differently and the comparison keys both
// spellings to one object: a table named with its default schema, which the
// read names bare, and a column whose name differs in case on an engine that
// folds it.
//
// The forward diff names the object as the declaration does. The reversal has
// to find the same object in the pre-change database; matched by name text it
// finds none, and the rollback writes an ERROR comment in place of the ALTER,
// or refuses to plan the rebuild at all.
func TestGenerateDownMigrationSQL_RestoresAColumnTheComparisonMatchedByIdentity(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		caps        capability.Capabilities
		tableSchema string
		column      string
		want        string
	}{
		{
			name:        "PostgreSQL table named with public",
			dialect:     platform.Postgres,
			caps:        capability.Postgres17(),
			tableSchema: "public",
			column:      "sku",
			want:        `ALTER TABLE "public"."items" ALTER COLUMN "sku" TYPE varchar(64);`,
		},
		{
			name:        "SQLite table named with main",
			dialect:     platform.SQLite,
			tableSchema: "main",
			column:      "sku",
			want:        `INSERT INTO "new_items" ("id", "sku") SELECT "id", "sku" FROM "items";`,
		},
		{
			name:    "MySQL column named in another case",
			dialect: platform.MySQL,
			column:  "SKU",
			want:    "ALTER TABLE `items` MODIFY COLUMN `sku` varchar(64);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Items", Name: "items", Schema: test.tableSchema}},
				Fields: []schemamodel.Field{
					{StructName: "Items", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "Items", Name: test.column, Type: "VARCHAR(128)", Nullable: true},
				},
			}
			database := &catalog.Database{
				Tables: []catalog.Table{{
					Name: "items", Type: "BASE TABLE",
					Columns: []catalog.Column{
						{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
						{Name: "sku", DataType: "varchar", FormattedType: "varchar(64)", IsNullable: "YES"},
					},
				}},
			}
			upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(),
				desired, database, test.dialect, must.Must(builtin.New()),
			))
			c.Assert(upDiff.TablesModified, qt.HasLen, 1)
			c.Assert(upDiff.TablesModified[0].ColumnsModified, qt.HasLen, 1)

			down, err := downMigrationSQL(t.Context(), must.Must(builtin.New()),
				upDiff, desired, database, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(down, qt.Contains, test.want,
				qt.Commentf("the rollback restores the column the database held:\n%s", down))
			c.Assert(down, qt.Not(qt.Contains), "ERROR",
				qt.Commentf("the rollback resolves the column it restores:\n%s", down))
		})
	}
}
