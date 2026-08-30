package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPlanner_GenerateMigrationSQL_EnumsAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single enum added",
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
			diff: &difftypes.SchemaDiff{
				EnumsAdded: difftypes.EnumChanges{{Name: "user_status", Values: []string{"active", "inactive"}}, {Name: "order_status", Values: []string{"pending", "completed", "canceled"}}},
			},
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "user_status", Values: []string{"active", "inactive"}},
					{Name: "order_status", Values: []string{"pending", "completed", "canceled"}},
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

				return enum2.Values[0] == "pending" &&
					enum2.Values[1] == "completed" &&
					enum2.Values[2] == "canceled"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_EnumsModified(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "enum with values added",
			diff: &difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{
					{
						EnumName:    "user_status",
						ValuesAdded: []string{"suspended"},
					},
				},
			},
			desired: &schemamodel.Database{},
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
			name: "enum with values removed recreates type",
			diff: &difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{
					{
						EnumName:      "user_status",
						ValuesRemoved: []string{"deprecated"},
						// The columns a comparison carries for a value removal: the
						// type is recreated, so every column naming it is converted
						// across and its default put back.
						Usages: []difftypes.EnumColumnUsage{{
							Table: "users", Column: "status",
							Default: "active", DefaultSet: true,
						}},
					},
				},
				// The vocabulary a comparison fills on every run: recreating an enum
				// reads the values it should hold, and those are the declaration's
				// rather than anything the change carries.
				DeclaredUserTypes: difftypes.UserTypeVocabulary{
					Enums: []schemamodel.Enum{
						{Name: "user_status", Values: []string{"active", "suspended"}},
					},
				},
			},
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "user_status", Values: []string{"active", "suspended"}},
				},
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []schemamodel.Field{
					{
						Name:       "status",
						Type:       "user_status",
						StructName: "User",
						Default:    "active",
						DefaultSet: true,
					},
				},
			},
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 1 {
					return false
				}

				rawNode, ok := nodes[0].(*ast.RawSQLNode)
				if !ok {
					return false
				}
				return strings.Contains(rawNode.SQL, `ALTER TABLE "users" ALTER COLUMN "status" DROP DEFAULT;`) &&
					strings.Contains(rawNode.SQL, `ALTER TYPE "user_status" RENAME TO "user_status__ptah_old";`) &&
					strings.Contains(rawNode.SQL, `CREATE TYPE "user_status" AS ENUM ('active', 'suspended');`) &&
					strings.Contains(rawNode.SQL, `ALTER TABLE "users" ALTER COLUMN "status" TYPE "user_status" USING "status"::text::"user_status";`) &&
					strings.Contains(rawNode.SQL, `ALTER TABLE "users" ALTER COLUMN "status" SET DEFAULT 'active';`) &&
					strings.Contains(rawNode.SQL, `DROP TYPE "user_status__ptah_old";`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_TablesAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool

		// tablesAdded names the tables the diff creates. The creations are
		// assembled from the row's own desired schema in the loop below,
		// because a row cannot reference its own other field
		// (stokaro/ptah#2315).
		tablesAdded []string
	}{
		{
			name:        "single table added",
			tablesAdded: []string{"users"},
			diff:        &difftypes.SchemaDiff{},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []schemamodel.Field{
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
		{
			name:        "composite primary key is created with new table",
			tablesAdded: []string{"memberships"},
			diff:        &difftypes.SchemaDiff{},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{
					Name:       "memberships",
					StructName: "Membership",
					PrimaryKey: []string{"org_id", "user_id"},
				}},
				Fields: []schemamodel.Field{
					{Name: "org_id", Type: "INTEGER", StructName: "Membership", Nullable: false},
					{Name: "user_id", Type: "INTEGER", StructName: "Membership", Nullable: false},
					{Name: "role", Type: "TEXT", StructName: "Membership", Nullable: false},
				},
			},
			expected: func(nodes []ast.Node) bool {
				sql, err := renderer.RenderSQL("postgres", nodes...)
				return err == nil && strings.Contains(sql, `PRIMARY KEY ("org_id", "user_id")`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			tt.diff.TablesAdded = difftypes.TableCreationsFor(tt.desired, tt.tablesAdded...)

			planner := &postgres.Planner{}
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_TablesModified(t *testing.T) {
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
			name: "column with foreign key added - separated operations",
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
				// Should have 4 nodes: comment, ADD COLUMN, comment, ADD CONSTRAINT
				if len(nodes) != 4 {
					return false
				}

				// First should be comment for column addition
				_, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Second should be ALTER TABLE with ADD COLUMN operation only
				alterNode1, ok := nodes[1].(*ast.AlterTableNode)
				if !ok {
					return false
				}

				if alterNode1.Name != "posts" || len(alterNode1.Operations) != 1 {
					return false
				}

				// Should be ADD COLUMN operation
				addColOp, ok := alterNode1.Operations[0].(*ast.AddColumnOperation)
				if !ok {
					return false
				}
				if addColOp.Column.Name != "user_id" {
					return false
				}

				// Third should be comment for foreign key constraint
				_, ok = nodes[2].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Fourth should be ALTER TABLE with ADD CONSTRAINT operation only
				alterNode2, ok := nodes[3].(*ast.AlterTableNode)
				if !ok {
					return false
				}

				if alterNode2.Name != "posts" || len(alterNode2.Operations) != 1 {
					return false
				}

				// Should be ADD CONSTRAINT operation
				addConstraintOp, ok := alterNode2.Operations[0].(*ast.AddConstraintOperation)
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

// TestPlanner_ForeignKeyDependencyOrdering tests the fix for issue #47:
// Foreign key constraint generation before referenced column creation causes migration failure
func TestPlanner_ForeignKeyDependencyOrdering(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "foreign key references newly added column - proper ordering",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{
						TableName: "users",
						ColumnsAdded: difftypes.ColumnChanges{{Name: "id",
							Type:       "TEXT",
							StructName: "User",
							Primary:    true,
							Nullable:   false}},
					},
					{
						TableName: "restore_steps",
						ColumnsAdded: difftypes.ColumnChanges{{Name: "user_id",
							Type:           "TEXT",
							StructName:     "RestoreStep",
							Nullable:       false,
							Foreign:        "users(id)",
							ForeignKeyName: "fk_entity_user"}},
					},
				},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
					{Name: "restore_steps", StructName: "RestoreStep"},
				},
				Fields: []schemamodel.Field{
					{
						Name:       "id",
						Type:       "TEXT",
						StructName: "User",
						Primary:    true,
						Nullable:   false,
					},
					{
						Name:           "user_id",
						Type:           "TEXT",
						StructName:     "RestoreStep",
						Nullable:       false,
						Foreign:        "users(id)",
						ForeignKeyName: "fk_entity_user",
					},
				},
			},
			expected: func(nodes []ast.Node) bool {
				// Should have 6 nodes:
				// 1. Comment for users column addition
				// 2. ALTER TABLE users ADD COLUMN id
				// 3. Comment for restore_steps column addition
				// 4. ALTER TABLE restore_steps ADD COLUMN user_id
				// 5. Comment for foreign key constraints
				// 6. ALTER TABLE restore_steps ADD CONSTRAINT fk_entity_user
				if len(nodes) != 6 {
					return false
				}

				// Verify the ordering: all ADD COLUMN operations come before ADD CONSTRAINT operations

				// Node 1: Comment for users column
				_, ok := nodes[0].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Node 2: ADD COLUMN for users.id
				alterNode1, ok := nodes[1].(*ast.AlterTableNode)
				if !ok || alterNode1.Name != "users" || len(alterNode1.Operations) != 1 {
					return false
				}
				addColOp1, ok := alterNode1.Operations[0].(*ast.AddColumnOperation)
				if !ok || addColOp1.Column.Name != "id" {
					return false
				}

				// Node 3: Comment for restore_steps column
				_, ok = nodes[2].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Node 4: ADD COLUMN for restore_steps.user_id
				alterNode2, ok := nodes[3].(*ast.AlterTableNode)
				if !ok || alterNode2.Name != "restore_steps" || len(alterNode2.Operations) != 1 {
					return false
				}
				addColOp2, ok := alterNode2.Operations[0].(*ast.AddColumnOperation)
				if !ok || addColOp2.Column.Name != "user_id" {
					return false
				}

				// Node 5: Comment for foreign key constraints
				_, ok = nodes[4].(*ast.CommentNode)
				if !ok {
					return false
				}

				// Node 6: ADD CONSTRAINT for foreign key
				alterNode3, ok := nodes[5].(*ast.AlterTableNode)
				if !ok || alterNode3.Name != "restore_steps" || len(alterNode3.Operations) != 1 {
					return false
				}
				addConstraintOp, ok := alterNode3.Operations[0].(*ast.AddConstraintOperation)
				if !ok {
					return false
				}
				constraint := addConstraintOp.Constraint
				if constraint.Name != "fk_entity_user" ||
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

// TestPlanner_ForeignKeyDependencyOrdering_SQLOutput tests the actual SQL output
// to ensure the fix generates correct SQL statements in the right order
func TestPlanner_ForeignKeyDependencyOrdering_SQLOutput(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{
				TableName: "users",
				ColumnsAdded: difftypes.ColumnChanges{{Name: "id",
					Type:       "TEXT",
					StructName: "User",
					Primary:    true,
					Nullable:   false}},
			},
			{
				TableName: "restore_steps",
				ColumnsAdded: difftypes.ColumnChanges{{Name: "user_id",
					Type:           "TEXT",
					StructName:     "RestoreStep",
					Nullable:       false,
					Foreign:        "users(id)",
					ForeignKeyName: "fk_entity_user"}},
			},
		},
	}

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "users", StructName: "User"},
			{Name: "restore_steps", StructName: "RestoreStep"},
		},
		Fields: []schemamodel.Field{
			{
				Name:       "id",
				Type:       "TEXT",
				StructName: "User",
				Primary:    true,
				Nullable:   false,
			},
			{
				Name:           "user_id",
				Type:           "TEXT",
				StructName:     "RestoreStep",
				Nullable:       false,
				Foreign:        "users(id)",
				ForeignKeyName: "fk_entity_user",
			},
		},
	}

	planner := &postgres.Planner{}
	nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	// Render to SQL to verify the actual output
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// Verify that ADD COLUMN operations come before ADD CONSTRAINT operations
	lines := strings.Split(sql, "\n")
	var addColumnLines []int
	var addConstraintLines []int

	for i, line := range lines {
		if strings.Contains(line, "ADD COLUMN") {
			addColumnLines = append(addColumnLines, i)
		}
		if strings.Contains(line, "ADD CONSTRAINT") {
			addConstraintLines = append(addConstraintLines, i)
		}
	}

	// All ADD COLUMN operations should come before all ADD CONSTRAINT operations
	c.Assert(addColumnLines, qt.HasLen, 2)
	c.Assert(addConstraintLines, qt.HasLen, 1)

	// The last ADD COLUMN should come before the first ADD CONSTRAINT
	lastAddColumn := addColumnLines[len(addColumnLines)-1]
	firstAddConstraint := addConstraintLines[0]
	c.Assert(lastAddColumn < firstAddConstraint, qt.IsTrue)

	// Verify specific SQL content
	c.Assert(sql, qt.Contains, "ALTER TABLE users ADD COLUMN id TEXT PRIMARY KEY")
	c.Assert(sql, qt.Contains, "ALTER TABLE restore_steps ADD COLUMN user_id TEXT NOT NULL")
	c.Assert(sql, qt.Contains, "ALTER TABLE restore_steps ADD CONSTRAINT fk_entity_user FOREIGN KEY (user_id) REFERENCES users(id)")
}

func TestPlanner_GenerateMigrationSQL_IndexesAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single index added",
			diff: &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_email", Fields: []string{"email"}}, TableName: "users"}},
			},
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email", TableName: "users", Fields: []string{"email"}},
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
			diff: &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "uk_users_email", TableName: "users", Fields: []string{"email"}, Unique: true}, TableName: "users"}},
			},
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "uk_users_email", TableName: "users", Fields: []string{"email"}, Unique: true},
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
		{
			name: "same name index replacement drops before create",
			diff: &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_c", TableName: "users", Fields: []string{"c"}, Unique: true, NullsDistinct: new(false)}, TableName: "users"}},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_users_c", TableName: "users"},
				},
			},
			desired: func() *schemamodel.Database {
				nullsDistinct := false
				return &schemamodel.Database{
					Indexes: []schemamodel.Index{
						{Name: "idx_users_c", TableName: "users", Fields: []string{"c"}, Unique: true, NullsDistinct: &nullsDistinct},
					},
				}
			}(),
			expected: func(nodes []ast.Node) bool {
				if len(nodes) != 2 {
					return false
				}
				dropNode, dropOk := nodes[0].(*ast.DropIndexNode)
				indexNode, indexOk := nodes[1].(*ast.IndexNode)
				return dropOk && indexOk &&
					dropNode.Name == "idx_users_c" &&
					indexNode.Name == "idx_users_c" &&
					indexNode.NullsDistinct != nil &&
					!*indexNode.NullsDistinct
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_IndexesRemoved(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single index removed",
			diff: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_old_index", TableName: "users"},
				},
			},
			desired: &schemamodel.Database{},
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
			diff: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_old1", TableName: "users"},
					{Name: "idx_old2", TableName: "orders"},
				},
			},
			desired: &schemamodel.Database{},
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_RecreatesGeneratedColumnOnExpressionChange(t *testing.T) {
	c := qt.New(t)

	// Declared once and used twice: the column the plan renders is the operand
	// the modification carries, and an operand disagreeing with the declaration
	// it is applied against would be a state no comparison produces.
	slug := schemamodel.Field{
		StructName:          "User",
		Name:                "slug",
		Type:                "TEXT",
		Nullable:            true,
		GeneratedExpression: "lower(name)",
		GeneratedKind:       "STORED",
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{
				TableName: "users",
				ColumnsModified: []difftypes.ColumnDiff{
					{
						ColumnName: "slug",
						Desired:    slug,
						Changes: map[string]string{
							"generated": "STORED upper(name) -> STORED lower(name)",
						},
					},
				},
			},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
		},
		Fields: []schemamodel.Field{slug},
	}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "slug" SET EXPRESSION AS (lower(name));`)
	c.Assert(sql, qt.Not(qt.Contains), `DROP COLUMN "slug"`)
	c.Assert(sql, qt.Not(qt.Contains), `ADD COLUMN "slug"`)
}

func TestPlanner_GeneratedColumnExpressionChangeOnPostgres16RequiresManualMigration(t *testing.T) {
	c := qt.New(t)

	slug := schemamodel.Field{
		StructName:          "User",
		Name:                "slug",
		Type:                "TEXT",
		Nullable:            true,
		GeneratedExpression: "lower(name)",
		GeneratedKind:       "STORED",
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{
				TableName: "users",
				ColumnsModified: []difftypes.ColumnDiff{
					{
						ColumnName: "slug",
						Desired:    slug,
						Changes: map[string]string{
							"generated": "STORED upper(name) -> STORED lower(name)",
						},
					},
				},
			},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
		},
		Fields: []schemamodel.Field{slug},
	}

	nodes, err := postgres.NewWithCapabilities(capability.Postgres16()).GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities("postgres", capability.Postgres16(), nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "WARNING: Generated column users.slug changed, but ALTER COLUMN SET EXPRESSION requires target capability "+
		string(capability.AlterGeneratedColumnExpression)+
		", unavailable on this target (PostgreSQL added it in 17); manual migration required.")
	c.Assert(sql, qt.Not(qt.Contains), `DROP COLUMN "slug"`)
	c.Assert(sql, qt.Not(qt.Contains), `ADD COLUMN "slug"`)
	c.Assert(sql, qt.Not(qt.Contains), "SET EXPRESSION AS")
}

func TestPlanner_RecreatesEmbeddedGeneratedColumnOnExpressionChange(t *testing.T) {
	c := qt.New(t)

	// The operand is the column as the declaration writes it, inside the embedded
	// struct. Folding that struct into its host is the COMPARISON's job now, and
	// TestCompare_AnEmbeddedColumnsModificationCarriesTheFoldedColumn is where it
	// is asserted; what this test holds is that the plan renders the operand it is
	// handed.
	slug := schemamodel.Field{
		StructName:          "ComputedFields",
		Name:                "slug",
		Type:                "TEXT",
		Nullable:            true,
		GeneratedExpression: "lower(name)",
		GeneratedKind:       "STORED",
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{
				TableName: "users",
				ColumnsModified: []difftypes.ColumnDiff{
					{
						ColumnName: "slug",
						Desired:    slug,
						Changes: map[string]string{
							"generated": "STORED upper(name) -> STORED lower(name)",
						},
					},
				},
			},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
		},
		Fields: []schemamodel.Field{slug},
		EmbeddedFields: []schemamodel.EmbeddedField{
			{
				StructName:       "User",
				Mode:             "inline",
				EmbeddedTypeName: "ComputedFields",
			},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "users" ALTER COLUMN "slug" SET EXPRESSION AS (lower(name));`)
	c.Assert(sql, qt.Not(qt.Contains), "carries no column definition")
}

func TestPlanner_GenerateMigrationSQL_TablesRemoved(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single table removed",
			diff: &difftypes.SchemaDiff{
				TablesRemoved: []string{"old_table"},
			},
			desired: &schemamodel.Database{},
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_EnumsRemoved(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single enum removed",
			diff: &difftypes.SchemaDiff{
				EnumsRemoved: difftypes.EnumChanges{{Name: "old_enum"}},
			},
			desired: &schemamodel.Database{},
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_ComplexScenario(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool

		// tablesAdded names the tables the diff creates. The creations are
		// assembled from the row's own desired schema in the loop below,
		// because a row cannot reference its own other field
		// (stokaro/ptah#2315).
		tablesAdded []string
	}{
		{
			name:        "complete migration with all operations",
			tablesAdded: []string{"users"},
			diff: &difftypes.SchemaDiff{
				EnumsAdded:   difftypes.EnumChanges{{Name: "user_status", Values: []string{"active", "inactive"}}},
				IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_email", Fields: []string{"email"}}, TableName: "users"}},
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "idx_old", TableName: "old_users"},
				},
			},
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "user_status", Values: []string{"active", "inactive"}},
				},
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []schemamodel.Field{
					{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
					{Name: "email", Type: "VARCHAR(255)", StructName: "User", Nullable: false},
				},
				Indexes: []schemamodel.Index{
					{Name: "idx_users_email", TableName: "users", Fields: []string{"email"}},
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

			tt.diff.TablesAdded = difftypes.TableCreationsFor(tt.desired, tt.tablesAdded...)

			planner := &postgres.Planner{}
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationSQL_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name:    "empty diff should return empty result",
			diff:    &difftypes.SchemaDiff{},
			desired: &schemamodel.Database{},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 0
			},
		},
		{
			// This row used to assert that an enum the desired schema does
			// not declare plans NOTHING -- the planner looked it up by name
			// and silently skipped what it could not find. The change carries
			// the enum now (stokaro/ptah#2315), so it is planned from the
			// operand and the desired schema has no say. The silent skip was
			// the defect, not the behaviour to keep.
			name: "an enum the desired schema does not declare is still planned",
			diff: &difftypes.SchemaDiff{
				EnumsAdded: difftypes.EnumChanges{{Name: "missing_enum", Values: []string{"a", "b"}}},
			},
			desired: &schemamodel.Database{
				Enums: []schemamodel.Enum{
					{Name: "other_enum", Values: []string{"value1"}},
				},
			},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 1
			},
		},
		{
			name: "table added but not found in generated schema",
			diff: &difftypes.SchemaDiff{
				// A name no declared table answers to, which is what this row is
				// about: the creation carries nothing to render.
				TablesAdded: difftypes.TableChanges{{Name: "missing_table"}},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "other_table", StructName: "Other"},
				},
			},
			expected: func(nodes []ast.Node) bool {
				return len(nodes) == 0 // Should not generate anything for missing table
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := &postgres.Planner{}
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_GenerateMigrationAST_UndescribedIndexRejected(t *testing.T) {
	c := qt.New(t)
	// An addition the diff does not describe: a name, a table, and nothing to
	// create. It used to be refused for being absent from the declaration; an
	// addition carries its own definition now, so the question is asked of the
	// addition and a declaration is not consulted at all (stokaro/ptah#2315).
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "missing_index"}, TableName: "users"}},
	}

	nodes, err := (&postgres.Planner{}).GenerateMigrationAST(diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*added index users\.missing_index is not described by the diff.*`)
	c.Assert(nodes, qt.IsNil)
}

// TestPlanner_GenerateMigrationAST_AnIndexTheDeclarationLacksIsStillCreated is
// the control for the refusal above, and it states a behavior change.
//
// A described addition is planned whether or not the declaration holds it. The
// declaration is not an input to this planner any more, so "the diff says
// create it" is the whole question.
func TestPlanner_GenerateMigrationAST_AnIndexTheDeclarationLacksIsStillCreated(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{
			Index:     schemamodel.Index{Name: "idx_users_email", Fields: []string{"email"}},
			TableName: "users",
		}},
	}

	nodes, err := (&postgres.Planner{}).GenerateMigrationAST(diff)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
}

func TestPlanner_GenerateMigrationAST_ExtensionInstallationSchema(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ExtensionsAdded: difftypes.ExtensionChanges{
		{Name: "pgcrypto", Schema: " Extension Store "},
	}}
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{
		Name: "pgcrypto", Schema: " Extension Store ",
	}}}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	schema, ok := nodes[0].(*ast.CreateSchemaNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(schema.Name, qt.Equals, " Extension Store ")
	c.Assert(schema.IfNotExists, qt.IsTrue)
	extension, ok := nodes[1].(*ast.ExtensionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(extension.Name, qt.Equals, "pgcrypto")
	c.Assert(extension.Schema, qt.Equals, " Extension Store ")
}

func TestPlanner_GenerateMigrationAST_WhitespaceOnlyExtensionInstallationSchema(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ExtensionsAdded: difftypes.ExtensionChanges{
		{Name: "pgcrypto", Schema: " "},
	}}
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{
		Name: "pgcrypto", Schema: " ",
	}}}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	schema, ok := nodes[0].(*ast.CreateSchemaNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(schema.Name, qt.Equals, " ")
	c.Assert(schema.IfNotExists, qt.IsTrue)
	extension, ok := nodes[1].(*ast.ExtensionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(extension.Name, qt.Equals, "pgcrypto")
	c.Assert(extension.Schema, qt.Equals, " ")
}

func TestPlanner_GenerateMigrationAST_SystemExtensionInstallationSchemaNeedsNoPrecondition(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ExtensionsAdded: difftypes.ExtensionChanges{
		{Name: "plpgsql", Schema: "pg_catalog", Version: "1.0", IfNotExists: true},
	}}
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{
		Name: "plpgsql", Schema: "pg_catalog", Version: "1.0", IfNotExists: true,
	}}}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	extension, ok := nodes[0].(*ast.ExtensionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(extension.Name, qt.Equals, "plpgsql")
	c.Assert(extension.Schema, qt.Equals, "pg_catalog")
}

func TestPlanner_GenerateMigrationAST_ExtensionsAdded(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single extension added",
			diff: &difftypes.SchemaDiff{
				ExtensionsAdded: difftypes.ExtensionChanges{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
				},
			},
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
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
			diff: &difftypes.SchemaDiff{
				ExtensionsAdded: difftypes.ExtensionChanges{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
					{Name: "btree_gin", IfNotExists: true, Comment: "Enable GIN indexes on btree types"},
				},
			},
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
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
			diff: &difftypes.SchemaDiff{
				ExtensionsAdded: difftypes.ExtensionChanges{
					{Name: "postgis", Version: "3.0", IfNotExists: true, Comment: "Geographic data support"},
				},
			},
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

// TestPlanner_GenerateMigrationAST_ExtensionChanges covers what replaced a
// blanket refusal.
//
// Any non-empty ExtensionsModified used to end the whole plan with "extension
// schema moves are not yet supported", which was wrong on the engine the
// message named. PostgreSQL has both ALTER EXTENSION forms, and the reader
// already captures what decides between them (stokaro/ptah#1718).
func TestPlanner_GenerateMigrationAST_ExtensionChanges(t *testing.T) {
	tests := []struct {
		name   string
		change difftypes.ExtensionDiff
		want   string
	}{{
		name: "a relocatable move becomes SET SCHEMA",
		change: difftypes.ExtensionDiff{
			Name: "pgcrypto", FromSchema: "public", ToSchema: "extensions", Relocatable: true,
		},
		want: `ALTER EXTENSION "pgcrypto" SET SCHEMA "extensions";`,
	}, {
		name: "a raised version becomes UPDATE TO",
		change: difftypes.ExtensionDiff{
			Name: "pg_trgm", FromSchema: "public", ToSchema: "public",
			FromVersion: "1.5", ToVersion: "1.6",
		},
		want: `ALTER EXTENSION "pg_trgm" UPDATE TO '1.6';`,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationAST(&difftypes.SchemaDiff{ExtensionsModified: []difftypes.ExtensionDiff{test.change}})
			c.Assert(err, qt.IsNil)

			rendered, renderErr := renderer.RenderSQL(platform.Postgres, nodes...)
			c.Assert(renderErr, qt.IsNil)
			c.Assert(rendered, qt.Contains, test.want)
		})
	}
}

// TestPlanner_GenerateMigrationAST_ExtensionChangeRefusals covers the two
// shapes that stay refused.
//
// They are refusals rather than skips because the server answers each with an
// error of its own, measured on PostgreSQL 18: a downgrade is "has no update
// path", and moving a fixed extension is "does not support SET SCHEMA". Ptah's
// message names the reason the server's cannot.
func TestPlanner_GenerateMigrationAST_ExtensionChangeRefusals(t *testing.T) {
	tests := []struct {
		name   string
		change difftypes.ExtensionDiff
		want   string
	}{{
		name: "a fixed extension is refused, naming why the server would",
		change: difftypes.ExtensionDiff{
			Name: "pgcrypto", FromSchema: "public", ToSchema: "extensions",
		},
		want: `(?s).*not relocatable.*does not support SET SCHEMA.*`,
	}, {
		name: "a lowered version is refused rather than attempted",
		change: difftypes.ExtensionDiff{
			Name: "pg_trgm", FromSchema: "public", ToSchema: "public",
			FromVersion: "1.6", ToVersion: "1.5",
		},
		want: `(?s).*one direction only.*has no update path.*`,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationAST(&difftypes.SchemaDiff{ExtensionsModified: []difftypes.ExtensionDiff{test.change}})

			c.Assert(nodes, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestPlanner_GenerateMigrationAST_ExtensionsRemoved(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		desired  *schemamodel.Database
		expected func(nodes []ast.Node) bool
	}{
		{
			name: "single extension removed",
			diff: &difftypes.SchemaDiff{
				ExtensionsRemoved: difftypes.ExtensionChanges{{Name: "pg_trgm"}},
			},
			desired: &schemamodel.Database{},
			expected: func(nodes []ast.Node) bool {
				// Should have 3 warning comments + 1 drop extension statement
				if len(nodes) != 4 {
					return false
				}

				// Check warning comments
				for i := range 3 {
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
			diff: &difftypes.SchemaDiff{
				ExtensionsRemoved: difftypes.ExtensionChanges{{Name: "pg_trgm"}, {Name: "btree_gin"}},
			},
			desired: &schemamodel.Database{},
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			c.Assert(tt.expected(nodes), qt.IsTrue)
		})
	}
}

func TestPlanner_ExtensionSQL_Generation(t *testing.T) {
	tests := []struct {
		name          string
		diff          *difftypes.SchemaDiff
		desired       *schemamodel.Database
		expectedSQL   []string
		unexpectedSQL []string
	}{
		{
			name: "extension creation SQL",
			diff: &difftypes.SchemaDiff{
				ExtensionsAdded: difftypes.ExtensionChanges{
					{Name: "pg_trgm", IfNotExists: true, Comment: "Enable trigram similarity search"},
				},
			},
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
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
			diff: &difftypes.SchemaDiff{
				ExtensionsRemoved: difftypes.ExtensionChanges{{Name: "pg_trgm"}},
			},
			desired: &schemamodel.Database{},
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
			diff: &difftypes.SchemaDiff{
				ExtensionsAdded: difftypes.ExtensionChanges{
					{Name: "postgis", Version: "3.0", IfNotExists: true, Comment: "Geographic data support"},
				},
			},
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
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
			nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(tt.diff, tt.desired))
			c.Assert(err, qt.IsNil)

			// Render nodes to SQL
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			// Check expected SQL patterns
			for _, expected := range tt.expectedSQL {
				c.Assert(sql, qt.Contains, expected,
					qt.Commentf("Expected SQL to contain: %s\nActual SQL:\n%s", expected, sql))
			}

			// Check unexpected SQL patterns
			for _, unexpected := range tt.unexpectedSQL {
				c.Assert(sql, qt.Not(qt.Contains), unexpected,
					qt.Commentf("Expected SQL to NOT contain: %s\nActual SQL:\n%s", unexpected, sql))
			}
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
			{StructName: "TestTable", Name: "name", Type: "TEXT", Nullable: false},
			// Embedded struct fields (original)
			{StructName: "TestID", Name: "id", Type: "TEXT", Primary: true},
			// Processed embedded field (what walker.go would generate)
			{StructName: "TestTable", Name: "id", Type: "TEXT", Primary: true},
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
		TablesAdded: difftypes.TableCreationsFor(desired, "test_table"),
	}

	planner := &postgres.Planner{}
	result, err := planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	c.Assert(result, qt.HasLen, 1)

	// Convert AST to SQL to verify content
	sql, err := renderer.RenderSQL("postgresql", result[0])
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// Verify table creation
	c.Assert(strings.Contains(sql, "CREATE TABLE test_table"), qt.Equals, true)

	// Verify regular field is included
	c.Assert(strings.Contains(sql, "name TEXT"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "NOT NULL"), qt.Equals, true)

	// Verify embedded field is included (this was the bug)
	c.Assert(strings.Contains(sql, "id TEXT"), qt.Equals, true)
	c.Assert(strings.Contains(sql, "PRIMARY KEY"), qt.Equals, true)
}

// withDeclaredObjects fills a fixture diff's schema-wide carries from the
// declaration the plan is applied against, leaving one the fixture already
// supplied alone.
//
// A planner asks these lists about objects no diff entry names: whether the
// table a change names is declared at all, and which views a cascade reaches.
// A comparison fills them on every run. A fixture states the CHANGE, and
// restating the declaration beside it would put the same objects in two places
// where a reader has to check they agree.
func withDeclaredObjects(diff *difftypes.SchemaDiff, desired *schemamodel.Database) *difftypes.SchemaDiff {
	if diff == nil || desired == nil {
		return diff
	}
	diff = withConstraintRecords(diff, desired)
	completed := *diff
	if len(completed.DeclaredTables) == 0 {
		completed.DeclaredTables = desired.Tables
	}
	if len(completed.DeclaredTableDependencies) == 0 {
		completed.DeclaredTableDependencies = deporder.GeneratedTableDependencies(desired)
	}
	if len(completed.DeclaredFunctions.Order) == 0 {
		completed.DeclaredFunctions = difftypes.FunctionOrderingOf(desired)
	}
	if len(completed.DeclaredViewLikes.Views) == 0 && len(completed.DeclaredViewLikes.MaterializedViews) == 0 {
		completed.DeclaredViewLikes = difftypes.ViewLikeVocabularyOf(desired)
	}
	return &completed
}

// withConstraintRecords fills a fixture diff's constraint additions from the
// declaration, the way a comparison does.
//
// A comparison describes every constraint it adds: it resolves the host table,
// folds in the ones synthesized from a field, and carries the body. A fixture
// that states only names is standing in for that, so this does the same
// resolution once rather than each fixture spelling the record out
// (stokaro/ptah#2315).
//
// A name the declaration does not describe is left without a record, which is
// what a test about a diff naming something undeclared needs.
func withConstraintRecords(diff *difftypes.SchemaDiff, desired *schemamodel.Database) *difftypes.SchemaDiff {
	if diff == nil || desired == nil || len(diff.ConstraintsAdded) == 0 {
		return diff
	}
	completed := *diff
	records := append([]difftypes.ConstraintAdditionInfo(nil), diff.ConstraintsAddedWithTables...)
	described := make(map[string]bool, len(records))
	for _, record := range records {
		described[record.Name] = true
	}
	for _, name := range diff.ConstraintsAdded {
		if described[name] {
			continue
		}
		declared, ok := declaredConstraintNamed(desired, name)
		if !ok {
			continue
		}
		described[name] = true
		records = append(records, difftypes.ConstraintAdditionInfo{
			Name:            declared.Name,
			TableName:       constraintHostTable(desired, declared),
			Type:            declared.Type,
			Columns:         append([]string(nil), declared.Columns...),
			IncludeColumns:  append([]string(nil), declared.IncludeColumns...),
			CheckExpression: declared.CheckExpression,
			ForeignTable:    declared.ForeignTable,
			ForeignColumn:   declared.ForeignColumn,
			ForeignColumns:  append([]string(nil), declared.ForeignColumns...),
			OnDelete:        declared.OnDelete,
			OnUpdate:        declared.OnUpdate,
			Deferrable:      declared.Deferrable,
			Initially:       declared.Initially,
			UsingMethod:     declared.UsingMethod,
			ExcludeElements: declared.ExcludeElements,
			WhereCondition:  declared.WhereCondition,
		})
	}
	completed.ConstraintsAddedWithTables = records
	return &completed
}

func declaredConstraintNamed(desired *schemamodel.Database, name string) (schemamodel.Constraint, bool) {
	for _, constraint := range desired.Constraints {
		if constraint.Name == name {
			return constraint, true
		}
	}
	return schemamodel.Constraint{}, false
}

// constraintHostTable resolves the table a declared constraint belongs to: the
// one it names, or the one its struct declares.
func constraintHostTable(desired *schemamodel.Database, constraint schemamodel.Constraint) string {
	if constraint.Table != "" {
		return constraint.Table
	}
	for _, table := range desired.Tables {
		if table.StructName == constraint.StructName {
			return table.QualifiedName()
		}
	}
	return ""
}
