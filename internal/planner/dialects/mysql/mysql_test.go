package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPlanner_GenerateMigrationAST_EnumsAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "enum added generates warning comment",
			diff: &difftypes.SchemaDiff{
				EnumsAdded: difftypes.EnumChanges{{Name: "user_status", Values: []string{"active", "inactive"}}},
			},
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "user_status", Values: []string{"active", "inactive"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				commentNode, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}
				return commentNode.Text != ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := mysql.New()
			nodes, err := planner.GenerateMigrationAST(tt.diff, tt.desired)
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_EnumsModified(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "enum modification generates warning comments",
			diff: &difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{
					{
						EnumName:      "user_status",
						ValuesAdded:   []string{"suspended"},
						ValuesRemoved: []string{"deprecated"},
					},
				},
			},
			desired: &schemamodel.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}
				// Both should be warning comments for MySQL enum limitations
				comment1, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}
				comment2, ok := nodes[1].(*ast.CommentNode)
				if !ok {
					return false
				}
				return comment1.Text != "" && comment2.Text != ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := mysql.New()
			nodes, err := planner.GenerateMigrationAST(tt.diff, tt.desired)
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_TablesAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single table added",
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "users"}},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []schemamodel.Field{
					{Name: "id", Type: "INT", StructName: "User", Primary: true, AutoInc: true},
					{Name: "email", Type: "VARCHAR(255)", StructName: "User", Nullable: false},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				tableNode, ok := nodes[0].(*ast.CreateTableNode)
				if !ok {
					return false
				}
				return tableNode.Name == "users" && len(tableNode.Columns) == 2
			},
		},
		{
			name: "composite primary key is created with new table",
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "memberships"}},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{
					Name:       "memberships",
					StructName: "Membership",
					PrimaryKey: []string{"org_id", "user_id"},
				}},
				Fields: []schemamodel.Field{
					{Name: "org_id", Type: "INT", StructName: "Membership", Nullable: false},
					{Name: "user_id", Type: "INT", StructName: "Membership", Nullable: false},
					{Name: "role", Type: "TEXT", StructName: "Membership", Nullable: false},
				},
			},
			expected: func(nodes []ast.Node) bool {
				sql, err := renderer.RenderSQL("mysql", nodes...)
				if err != nil {
					return false
				}
				sql = legacyRenderedSQL(sql)
				return strings.Contains(sql, "PRIMARY KEY (org_id, user_id)")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := mysql.New()
			nodes, err := planner.GenerateMigrationAST(tt.diff, tt.desired)
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_TablesModified(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "table with columns added",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{
						TableName:    "users",
						ColumnsAdded: difftypes.ColumnChanges{{Name: "created_at", Type: "TIMESTAMP", StructName: "User", Nullable: false}},
					},
				},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []schemamodel.Field{
					{Name: "created_at", Type: "TIMESTAMP", StructName: "User", Nullable: false},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}

				// First should be comment
				_, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Second should be ALTER TABLE
				alterNode, ok := nodes[1].(*ast.AlterTableNode)
				if !ok {
					return false
				}

				return alterNode.Name == "users" && len(alterNode.Operations) == 1
			},
		},
		{
			name: "column with foreign key added",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{
						TableName: "posts",
						ColumnsAdded: difftypes.ColumnChanges{{
							Name:           "user_id",
							Type:           "INTEGER",
							StructName:     "Post",
							Nullable:       false,
							Foreign:        "users(id)",
							ForeignKeyName: "fk_posts_user",
						}},
					},
				},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "posts", StructName: "Post"},
				},
				Fields: []schemamodel.Field{
					{
						Name:           "user_id",
						Type:           "INTEGER",
						StructName:     "Post",
						Nullable:       false,
						Foreign:        "users(id)",
						ForeignKeyName: "fk_posts_user",
					},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}

				// First should be comment
				_, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Second should be ALTER TABLE with two operations
				alterNode, ok := nodes[1].(*ast.AlterTableNode)
				if !ok {
					return false
				}

				if alterNode.Name != "posts" || len(alterNode.Operations) != 2 {
					return false
				}

				// First operation should be ADD COLUMN
				addColOp, ok := alterNode.Operations[0].(*ast.AddColumnOperation)
				if !ok {
					return false
				}
				if addColOp.Column.Name != "user_id" {
					return false
				}

				// Second operation should be ADD CONSTRAINT
				addConstraintOp, ok := alterNode.Operations[1].(*ast.AddConstraintOperation)
				if !ok {
					return false
				}
				constraint := addConstraintOp.Constraint
				if constraint.Name != "fk_posts_user" ||
					constraint.Type != ast.ForeignKeyConstraint ||
					len(constraint.Columns) != 1 ||
					constraint.Columns[0] != "user_id" ||
					constraint.Reference == nil ||
					constraint.Reference.Table != "users" ||
					constraint.Reference.Column != "id" {
					return false
				}

				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := mysql.New()
			nodes, err := planner.GenerateMigrationAST(tt.diff, tt.desired)
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_IndexesAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single index added",
			diff: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "idx_users_email", TableName: "users"},
				},
			},
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email", StructName: "users", TableName: "users", Fields: []string{"email"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				indexNode, ok := nodes[0].(*ast.IndexNode)
				if !ok {
					return false
				}
				return indexNode.Name == "idx_users_email" &&
					indexNode.Table == "users" &&
					len(indexNode.Columns) == 1
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := mysql.New()
			nodes, err := planner.GenerateMigrationAST(tt.diff, tt.desired)
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_EnumsRemoved(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "enum removed generates warning comment",
			diff: &difftypes.SchemaDiff{
				EnumsRemoved: difftypes.EnumChanges{{Name: "old_enum"}},
			},
			desired: &schemamodel.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				commentNode, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}
				return commentNode.Text != ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := mysql.New()
			nodes, err := planner.GenerateMigrationAST(tt.diff, tt.desired)
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_AddNewTables_WithEmbeddedFields(t *testing.T) {
	c := qt.New(t)

	// Test data: schema with embedded fields (simulating the walker.go processing)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "TestTable", Name: "test_table"},
		},
		Fields: []schemamodel.Field{
			// Regular field
			{StructName: "TestTable", Name: "name", Type: "VARCHAR(255)", Nullable: false},
			// Embedded struct fields (original)
			{StructName: "TestID", Name: "id", Type: "INT", Primary: true, AutoInc: true},
			// Processed embedded field (what walker.go would generate)
			{StructName: "TestTable", Name: "id", Type: "INT", Primary: true, AutoInc: true},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{
			{
				StructName:       "TestTable",
				Mode:             "inline",
				EmbeddedTypeName: "TestID",
			},
		},
	}

	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "test_table"}},
	}

	planner := mysql.New()
	result, err := planner.GenerateMigrationAST(diff, desired)
	c.Assert(err, qt.IsNil)

	c.Assert(result, qt.HasLen, 1)

	// Convert AST to SQL to verify content
	sql, err := renderer.RenderSQL("mysql", result[0])
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// Verify table creation
	c.Assert(strings.Contains(sql, "CREATE TABLE test_table"), qt.Equals, true)

	// Verify regular field is included
	c.Assert(strings.Contains(sql, "name VARCHAR(255)"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "NOT NULL"), qt.Equals, true)

	// Verify embedded field is included (this was the bug)
	c.Assert(strings.Contains(sql, "id INT"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "AUTO_INCREMENT"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "PRIMARY KEY"), qt.Equals, true)
}
