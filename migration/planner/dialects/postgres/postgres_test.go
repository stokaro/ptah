package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationSQL_EnumsAdded(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single enum added",
			diff: &types.SchemaDiff{
				EnumsAdded: []string{"user_status"},
			},
			generated: &goschema.Database{
				Enums: []goschema.Enum{
					{Name: "user_status", Values: []string{"active", "inactive"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				enumNode, ok := nodes[0].(*ast.EnumNode)
				if !ok {
					return false
				}
				return enumNode.Name == "user_status" &&
					len(enumNode.Values) == 2 &&
					enumNode.Values[0] == "active" &&
					enumNode.Values[1] == "inactive"
			},
		},
		{
			name: "multiple enums added",
			diff: &types.SchemaDiff{
				EnumsAdded: []string{"user_status", "order_status"},
			},
			generated: &goschema.Database{
				Enums: []goschema.Enum{
					{Name: "user_status", Values: []string{"active", "inactive"}},
					{Name: "order_status", Values: []string{"pending", "completed", "cancelled"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}

				// Check first enum
				enum1, ok := nodes[0].(*ast.EnumNode)
				if !ok || enum1.Name != "user_status" || len(enum1.Values) != 2 {
					return false
				}

				// Check second enum
				enum2, ok := nodes[1].(*ast.EnumNode)
				if !ok || enum2.Name != "order_status" || len(enum2.Values) != 3 {
					return false
				}

				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_EnumsModified(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "enum with values added",
			diff: &types.SchemaDiff{
				EnumsModified: []types.EnumDiff{
					{
						EnumName:    "user_status",
						ValuesAdded: []string{"suspended"},
					},
				},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				alterNode, ok := nodes[0].(*ast.AlterTypeNode)
				if !ok {
					return false
				}
				return alterNode.Name == "user_status" && len(alterNode.Operations) == 1
			},
		},
		{
			name: "enum with values removed (should generate warning)",
			diff: &types.SchemaDiff{
				EnumsModified: []types.EnumDiff{
					{
						EnumName:      "user_status",
						ValuesRemoved: []string{"deprecated"},
					},
				},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}

				// First should be ALTER TYPE
				alterNode, ok := nodes[0].(*ast.AlterTypeNode)
				if !ok || alterNode.Name != "user_status" {
					return false
				}

				// Second should be warning comment
				commentNode, ok := nodes[1].(*ast.CommentNode)
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

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_TablesAdded(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single table added",
			diff: &types.SchemaDiff{
				TablesAdded: []string{"users"},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_TablesModified(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "table with columns added",
			diff: &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{
						TableName:    "users",
						ColumnsAdded: []string{"created_at"},
					},
				},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []goschema.Field{
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
			diff: &types.SchemaDiff{
				TablesModified: []types.TableDiff{
					{
						TableName:    "posts",
						ColumnsAdded: []string{"user_id"},
					},
				},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
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

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_IndexesAdded(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single index added",
			diff: &types.SchemaDiff{
				IndexesAdded: []string{"idx_users_email"},
			},
			generated: &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "idx_users_email", StructName: "users", Fields: []string{"email"}},
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
		{
			name: "unique index added",
			diff: &types.SchemaDiff{
				IndexesAdded: []string{"uk_users_email"},
			},
			generated: &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "uk_users_email", StructName: "users", Fields: []string{"email"}, Unique: true},
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
				return indexNode.Name == "uk_users_email" && indexNode.Unique
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_IndexesRemoved(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single index removed",
			diff: &types.SchemaDiff{
				IndexesRemoved: []string{"idx_old_index"},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				dropIndexNode, ok := nodes[0].(*ast.DropIndexNode)
				if !ok {
					return false
				}
				return dropIndexNode.Name == "idx_old_index" && dropIndexNode.IfExists
			},
		},
		{
			name: "multiple indexes removed",
			diff: &types.SchemaDiff{
				IndexesRemoved: []string{"idx_old1", "idx_old2"},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}

				drop1, ok := nodes[0].(*ast.DropIndexNode)
				if !ok || drop1.Name != "idx_old1" {
					return false
				}

				drop2, ok := nodes[1].(*ast.DropIndexNode)
				if !ok || drop2.Name != "idx_old2" {
					return false
				}

				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_TablesRemoved(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single table removed",
			diff: &types.SchemaDiff{
				TablesRemoved: []string{"old_table"},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				dropTableNode, ok := nodes[0].(*ast.DropTableNode)
				if !ok {
					return false
				}
				return dropTableNode.Name == "old_table" &&
					dropTableNode.IfExists &&
					dropTableNode.Cascade
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_EnumsRemoved(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single enum removed",
			diff: &types.SchemaDiff{
				EnumsRemoved: []string{"old_enum"},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				dropTypeNode, ok := nodes[0].(*ast.DropTypeNode)
				if !ok {
					return false
				}
				return dropTypeNode.Name == "old_enum" &&
					dropTypeNode.IfExists &&
					dropTypeNode.Cascade
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_ComplexScenario(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "complete migration with all operations",
			diff: &types.SchemaDiff{
				EnumsAdded:     []string{"user_status"},
				TablesAdded:    []string{"users"},
				IndexesAdded:   []string{"idx_users_email"},
				IndexesRemoved: []string{"idx_old"},
			},
			generated: &goschema.Database{
				Enums: []goschema.Enum{
					{Name: "user_status", Values: []string{"active", "inactive"}},
				},
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
					{Name: "email", Type: "VARCHAR(255)", StructName: "User", Nullable: false},
				},
				Indexes: []goschema.Index{
					{Name: "idx_users_email", StructName: "users", Fields: []string{"email"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 4 {
					return false
				}

				// Should have enum, table, index, drop index in that order
				_, enumOk := nodes[0].(*ast.EnumNode)
				_, tableOk := nodes[1].(*ast.CreateTableNode)
				_, indexOk := nodes[2].(*ast.IndexNode)
				_, dropOk := nodes[3].(*ast.DropIndexNode)

				return enumOk && tableOk && indexOk && dropOk
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name:      "empty diff should return empty result",
			diff:      &types.SchemaDiff{},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 0
			},
		},
		{
			name: "enum added but not found in generated schema",
			diff: &types.SchemaDiff{
				EnumsAdded: []string{"missing_enum"},
			},
			generated: &goschema.Database{
				Enums: []goschema.Enum{
					{Name: "other_enum", Values: []string{"value1"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 0 // Should not generate anything for missing enum
			},
		},
		{
			name: "table added but not found in generated schema",
			diff: &types.SchemaDiff{
				TablesAdded: []string{"missing_table"},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "other_table", StructName: "Other"},
				},
			},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 0 // Should not generate anything for missing table
			},
		},
		{
			name: "index added but not found in generated schema",
			diff: &types.SchemaDiff{
				IndexesAdded: []string{"missing_index"},
			},
			generated: &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "other_index", StructName: "other_table", Fields: []string{"field"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 0 // Should not generate anything for missing index
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_ExtensionsAdded(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single extension added",
			diff: &types.SchemaDiff{
				ExtensionsAdded: []string{"pg_trgm"},
			},
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				extNode, ok := nodes[0].(*ast.ExtensionNode)
				if !ok {
					return false
				}
				return extNode.Name == "pg_trgm" &&
					extNode.IfNotExists == true &&
					extNode.Comment == "Enable trigram similarity search"
			},
		},
		{
			name: "multiple extensions added",
			diff: &types.SchemaDiff{
				ExtensionsAdded: []string{"pg_trgm", "btree_gin"},
			},
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
					{Name: "btree_gin", IfNotExists: true, Comment: "Enable GIN indexes on btree types"},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}

				// Check first extension
				ext1, ok := nodes[0].(*ast.ExtensionNode)
				if !ok || ext1.Name != "pg_trgm" {
					return false
				}

				// Check second extension
				ext2, ok := nodes[1].(*ast.ExtensionNode)
				if !ok || ext2.Name != "btree_gin" {
					return false
				}

				return true
			},
		},
		{
			name: "extension with version",
			diff: &types.SchemaDiff{
				ExtensionsAdded: []string{"postgis"},
			},
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "postgis", Version: "3.0", IfNotExists: true, Comment: "Geographic data support"},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}
				extNode, ok := nodes[0].(*ast.ExtensionNode)
				if !ok {
					return false
				}
				return extNode.Name == "postgis" &&
					extNode.Version == "3.0" &&
					extNode.IfNotExists == true &&
					extNode.Comment == "Geographic data support"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_ExtensionsRemoved(t *testing.T) {
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		expected  func(nodes []ast.Node) bool
	}{
		{
			name: "single extension removed",
			diff: &types.SchemaDiff{
				ExtensionsRemoved: []string{"pg_trgm"},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				// Should have 3 warning comments + 1 drop extension statement
				if len(nodes) != 4 {
					return false
				}

				// Check warning comments
				for i := 0; i < 3; i++ {
					if _, ok := nodes[i].(*ast.CommentNode); !ok {
						return false
					}
				}

				// Check drop extension statement
				dropNode, ok := nodes[3].(*ast.DropExtensionNode)
				if !ok {
					return false
				}
				return dropNode.Name == "pg_trgm" &&
					dropNode.IfExists == true
			},
		},
		{
			name: "multiple extensions removed",
			diff: &types.SchemaDiff{
				ExtensionsRemoved: []string{"pg_trgm", "btree_gin"},
			},
			generated: &goschema.Database{},
			expected: func(nodes []ast.Node) bool {
				// Should have 3 warnings + 1 drop + blank line + 3 warnings + 1 drop = 9 nodes
				if len(nodes) != 9 {
					return false
				}

				// Check first extension removal (3 warnings + 1 drop)
				dropNode1, ok := nodes[3].(*ast.DropExtensionNode)
				if !ok || dropNode1.Name != "pg_trgm" {
					return false
				}

				// Check blank line at position 4
				blankComment, ok := nodes[4].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Check second extension removal (3 warnings + 1 drop)
				// Second extension drop is at position 8
				dropNode2, ok := nodes[8].(*ast.DropExtensionNode)
				if !ok || dropNode2.Name != "btree_gin" {
					return false
				}

				return blankComment != nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_ExtensionSQL_Generation(t *testing.T) {
	tests := []struct {
		name          string
		diff          *types.SchemaDiff
		generated     *goschema.Database
		expectedSQL   []string
		unexpectedSQL []string
	}{
		{
			name: "extension creation SQL",
			diff: &types.SchemaDiff{
				ExtensionsAdded: []string{"pg_trgm"},
			},
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
				},
			},
			expectedSQL: []string{
				"-- Enable trigram similarity search",
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			},
			unexpectedSQL: []string{
				"DROP EXTENSION",
			},
		},
		{
			name: "extension removal SQL",
			diff: &types.SchemaDiff{
				ExtensionsRemoved: []string{"pg_trgm"},
			},
			generated: &goschema.Database{},
			expectedSQL: []string{
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"Consider reviewing all database objects that use this extension",
				"Extension removal may cascade to dependent objects",
				"DROP EXTENSION IF EXISTS pg_trgm;",
			},
			unexpectedSQL: []string{
				"CREATE EXTENSION",
			},
		},
		{
			name: "extension with version SQL",
			diff: &types.SchemaDiff{
				ExtensionsAdded: []string{"postgis"},
			},
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "postgis", Version: "3.0", IfNotExists: true, Comment: "Geographic data support"},
				},
			},
			expectedSQL: []string{
				"-- Geographic data support",
				"CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.0';",
			},
			unexpectedSQL: []string{
				"DROP EXTENSION",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			// Render nodes to SQL
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			// Check expected SQL patterns
			for _, expected := range tt.expectedSQL {
				c.Assert(strings.Contains(sql, expected), qt.IsTrue,
					qt.Commentf("Expected SQL to contain: %s\nActual SQL:\n%s", expected, sql))
			}

			// Check unexpected SQL patterns
			for _, unexpected := range tt.unexpectedSQL {
				c.Assert(strings.Contains(sql, unexpected), qt.IsFalse,
					qt.Commentf("Expected SQL to NOT contain: %s\nActual SQL:\n%s", unexpected, sql))
			}
		})
	}
}

func TestPlanner_AddNewTables_WithEmbeddedFields(t *testing.T) {
	c := qt.New(t)

	// Test data: schema with embedded fields (simulating the walker.go processing)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "TestTable", Name: "test_table"},
		},
		Fields: []goschema.Field{
			// Regular field
			{StructName: "TestTable", Name: "name", Type: "TEXT", Nullable: false},
			// Embedded struct fields (original)
			{StructName: "TestID", Name: "id", Type: "TEXT", Primary: true},
			// Processed embedded field (what walker.go would generate)
			{StructName: "TestTable", Name: "id", Type: "TEXT", Primary: true},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{
				StructName:       "TestTable",
				Mode:             "inline",
				EmbeddedTypeName: "TestID",
			},
		},
	}

	diff := &types.SchemaDiff{
		TablesAdded: []string{"test_table"},
	}

	planner := &postgres.Planner{}
	result := planner.GenerateMigrationAST(diff, generated)

	c.Assert(len(result), qt.Equals, 1)

	// Convert AST to SQL to verify content
	sql, err := renderer.RenderSQL("postgresql", result[0])
	c.Assert(err, qt.IsNil)

	// Verify table creation
	c.Assert(strings.Contains(sql, "CREATE TABLE test_table"), qt.Equals, true)

	// Verify regular field is included
	c.Assert(strings.Contains(sql, "name TEXT"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "NOT NULL"), qt.Equals, true)

	// Verify embedded field is included (this was the bug)
	c.Assert(strings.Contains(sql, "id TEXT"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "PRIMARY KEY"), qt.Equals, true)
}
