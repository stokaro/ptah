package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// plannedVector is the vector index a plan below adds, over column.
func plannedVector(name, column string, clusters uint64) schemamodel.Index {
	return schemamodel.Index{Name: name, Fields: []string{column}, Type: "vector_kmeans_tree",
		Vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: clusters}}
}

// TestGenerateMigrationAST_VectorIndex_HappyPath pins where a vector index
// sits in a plan: a changed one is dropped and added again under its name,
// because no setting of it changes in place; a new one comes after the column
// it reads; and one dropped with its column goes before the column, which YDB
// refuses to drop while an index names it (`Impossible drop column because
// table has an index with that column`, measured on 26.2.1.14 and 25.1.4.7).
// A plan of this shape applied on both lines and read back as declared.
func TestGenerateMigrationAST_VectorIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "items",
			Desired:        itemsDeclaration(field("emb", "vector(3)", true), field("note_emb", "vector(3)", true)),
			ColumnsAdded:   difftypes.ColumnChanges{field("note_emb", "vector(3)", true)},
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "old_emb"}},
		}},
		IndexesRemoved: []difftypes.IndexRef{{Name: "by_emb", TableName: "items"}, {Name: "by_old_emb", TableName: "items"}},
		IndexesAdded: difftypes.IndexChanges{
			{TableName: "items", Index: plannedVector("by_emb", "emb", 4)},
			{TableName: "items", Index: plannedVector("by_note_emb", "note_emb", 2)},
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "ALTER TABLE `items` DROP INDEX `by_emb`;\n"+
		"ALTER TABLE `items` DROP INDEX `by_old_emb`;\n"+
		"ALTER TABLE `items` ADD COLUMN `note_emb` String;\n"+
		"ALTER TABLE `items` DROP COLUMN `old_emb`;\n"+
		"ALTER TABLE `items` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) "+
		"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=4);\n"+
		"ALTER TABLE `items` ADD INDEX `by_note_emb` GLOBAL USING vector_kmeans_tree ON (`note_emb`) "+
		"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n")
}

// TestGenerateMigrationAST_VectorIndex_FailurePath refuses, before any node,
// a vector index added to a table the plan changes whose columns YDB would
// refuse it over.
func TestGenerateMigrationAST_VectorIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		column  schemamodel.Field
		wantErr string
	}{
		{name: "a text column", column: field("emb", "TEXT", true),
			wantErr: `adding index "by_emb" to table "items": its vector column "emb" is Utf8, and a YDB vector index reads a String column .*`},
		{name: "a vector of another dimension", column: field("emb", "vector(4)", true),
			wantErr: `adding index "by_emb" to table "items": its vector column "emb" is declared with dimension 4 and the index ` +
				`with vector_dimension 3; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName:    "items",
					Desired:      itemsDeclaration(test.column),
					ColumnsAdded: difftypes.ColumnChanges{test.column},
				}},
				IndexesAdded: difftypes.IndexChanges{{TableName: "items", Index: plannedVector("by_emb", "emb", 2)}},
			}

			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(diff)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
