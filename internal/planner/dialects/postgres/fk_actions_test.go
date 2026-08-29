package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlanner_FieldLevelForeignKeyActions verifies that on_delete / on_update
// declared on a //ptah:schema:field annotation flow all the way through to
// the emitted ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY statement.
//
// Regression test for issue #117. Before the fix, the keys were whitelisted by
// the strict-attribute validator (added with #82) but never read by
// parseFieldComment, so the AST never carried OnDelete/OnUpdate and the
// rendered SQL silently dropped them.
func TestPlanner_FieldLevelForeignKeyActions(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		mustEmit string
		// constraintMarker filters the negative check so it only inspects the
		// ALTER TABLE line carrying this constraint name.
		constraintMarker string
		mustNotHit       string
	}{
		{
			name: "ON DELETE CASCADE on field annotation",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "Commodity", Name: "commodities"},
					{StructName: "CommodityService", Name: "commodity_services"},
				},
				Fields: []schemamodel.Field{
					{StructName: "Commodity", Name: "id", Type: "TEXT", Primary: true},
					{StructName: "CommodityService", Name: "id", Type: "TEXT", Primary: true},
					{
						StructName:     "CommodityService",
						Name:           "commodity_id",
						Type:           "TEXT",
						Foreign:        "commodities(id)",
						ForeignKeyName: "fk_cs_commodity",
						OnDelete:       "CASCADE",
					},
				},
			},
			mustEmit: "ALTER TABLE commodity_services ADD CONSTRAINT fk_cs_commodity FOREIGN KEY (commodity_id) REFERENCES commodities(id) ON DELETE CASCADE;",
		},
		{
			name: "ON DELETE SET NULL + ON UPDATE CASCADE",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
					{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INTEGER",
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
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
					{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INTEGER",
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
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "Category", Name: "categories"},
				},
				Fields: []schemamodel.Field{
					{StructName: "Category", Name: "id", Type: "SERIAL", Primary: true},
				},
				SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
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
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{TableName: "posts", ColumnsAdded: difftypes.ColumnChanges{{
						// The column travels WITH the change, and the
						// referential action this row is about is read off it.
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INTEGER",
						Foreign:        "users(id)",
						ForeignKeyName: "fk_post_owner",
						OnDelete:       "RESTRICT",
					}}},
				},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{StructName: "User", Name: "users"},
					{StructName: "Post", Name: "posts"},
				},
				Fields: []schemamodel.Field{
					{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
					{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
					{
						StructName:     "Post",
						Name:           "owner_id",
						Type:           "INTEGER",
						Foreign:        "users(id)",
						ForeignKeyName: "fk_post_owner",
						OnDelete:       "RESTRICT",
					},
				},
			},
			mustEmit: "ALTER TABLE posts ADD CONSTRAINT fk_post_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE RESTRICT;",
		},
	}
	// Embedded-relation mode coverage lives in
	// internal/convert/fromschema/fromschema_test.go (TestFromDatabase_EmbeddedRelationFKActions) —
	// the planner doesn't expand EmbeddedFields itself; field expansion happens
	// in fromschema.ProcessEmbeddedFields and (for diffs) compare.processEmbeddedFieldsForStruct.

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := tt.diff
			if diff == nil {
				// Default: emit FKs for all tables in TablesAdded.
				tablesAdded := make(difftypes.TableChanges, 0, len(tt.desired.Tables))
				for _, table := range tt.desired.Tables {
					tablesAdded = append(tablesAdded,
						difftypes.TableCreationFor(tt.desired, table, table.Name))
				}
				diff = &difftypes.SchemaDiff{
					TablesAdded: tablesAdded,
					// A foreign key names the table it references, and that is
					// resolved through the declared list rather than through
					// anything a creation carries (stokaro/ptah#2315).
					DeclaredTables:    tt.desired.Tables,
					DeclaredUserTypes: difftypes.UserTypeVocabularyOf(tt.desired),
				}
			}

			nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, tt.desired), tt.desired)
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			c.Assert(sql, qt.Contains, tt.mustEmit,
				qt.Commentf("expected SQL to contain:\n  %s\ngot:\n%s", tt.mustEmit, sql))

			if tt.mustNotHit != "" {
				// Restrict the negative check to the line carrying the named
				// constraint so we don't accidentally match unrelated noise.
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
