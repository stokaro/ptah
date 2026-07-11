package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestPlanner_FieldLevelForeignKeyActions verifies that on_delete / on_update
// declared on a //migrator:schema:field annotation flow all the way through to
// the emitted ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY statement when
// the MySQL planner is in play.
//
// Sibling to the postgres test of the same name — guards the parallel edits
// in addRegularForeignKeys / addSelfReferencingForeignKeys / addNewTableColumns.
// Regression test for issue #117.
func TestPlanner_FieldLevelForeignKeyActions(t *testing.T) {
	tests := []struct {
		name             string
		diff             *types.SchemaDiff
		generated        *goschema.Database
		mustEmit         string
		constraintMarker string
		mustNotHit       string
	}{
		{
			name: "ON DELETE CASCADE on field annotation",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []goschema.Field{
					{StructName: "User", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{StructName: "Post", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INT",
						Foreign:        "users(id)",
						ForeignKeyName: "fk_post_owner",
						OnDelete:       "CASCADE",
					},
				},
			},
			mustEmit: "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE;",
		},
		{
			name: "ON DELETE SET NULL + ON UPDATE CASCADE",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []goschema.Field{
					{StructName: "User", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{StructName: "Post", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INT",
						Foreign:        "users(id)",
						ForeignKeyName: "fk_post_owner",
						OnDelete:       "SET NULL",
						OnUpdate:       "CASCADE",
					},
				},
			},
			mustEmit: "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE;",
		},
		{
			name: "no FK actions still emits a clean REFERENCES (no ON DELETE/UPDATE)",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []goschema.Field{
					{StructName: "User", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{StructName: "Post", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INT",
						Foreign:        "users(id)",
						ForeignKeyName: "fk_post_owner",
					},
				},
			},
			mustEmit:         "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id);",
			constraintMarker: "fk_post_owner",
			mustNotHit:       "ON DELETE",
		},
		{
			name: "self-referencing FK carries ON DELETE SET NULL",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "Category", Name: "categories"},
				},
				Fields: []goschema.Field{
					{StructName: "Category", Name: "id", Type: "INT", Primary: true, AutoInc: true},
				},
				SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{
					"categories": {
						{
							FieldName:      "parent_id",
							Foreign:        "categories(id)",
							ForeignKeyName: "fk_categories_parent",
							OnDelete:       "SET NULL",
						},
					},
				},
			},
			mustEmit: "ALTER TABLE categories ADD CONSTRAINT fk_categories_parent FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL;",
		},
		{
			name: "ALTER TABLE ADD COLUMN with FK carries ON DELETE RESTRICT",
			diff: &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{TableName: "posts", ColumnsAdded: []string{"owner_id"}},
				},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []goschema.Field{
					{StructName: "User", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{StructName: "Post", Name: "id", Type: "INT", Primary: true, AutoInc: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INT",
						Foreign:        "users(id)",
						ForeignKeyName: "fk_post_owner",
						OnDelete:       "RESTRICT",
					},
				},
			},
			mustEmit: "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE RESTRICT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := tt.diff
			if diff == nil {
				tablesAdded := make([]string, 0, len(tt.generated.Tables))
				for _, table := range tt.generated.Tables {
					tablesAdded = append(tablesAdded, table.Name)
				}
				diff = &types.SchemaDiff{TablesAdded: tablesAdded}
			}

			nodes := mysql.New().GenerateMigrationAST(diff, tt.generated)
			sql, err := renderer.RenderSQL("mysql", nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(sql, qt.Contains, tt.mustEmit,
				qt.Commentf("expected SQL to contain:\n  %s\ngot:\n%s", tt.mustEmit, sql))

			if tt.mustNotHit != "" {
				for line := range strings.SplitSeq(sql, "\n") {
					if strings.Contains(line, tt.constraintMarker) {
						c.Assert(line, qt.Not(qt.Contains), tt.mustNotHit,
							qt.Commentf("FK line should not mention %q: %s", tt.mustNotHit, line))
					}
				}
			}
		})
	}
}
