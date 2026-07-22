package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlannerCreatesTableWithInlineConstraints(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "accounts", StructName: "Account", Strict: true},
			{Name: "users", StructName: "User", Strict: true},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "accounts", Primary: true},
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "account_id", Type: "INTEGER", StructName: "User", Nullable: false},
			{Name: "email", Type: "TEXT", StructName: "User", Nullable: false},
		},
		Constraints: []goschema.Constraint{
			{
				Name:            "users_email_check",
				Type:            "CHECK",
				StructName:      "User",
				CheckExpression: "email <> ''",
			},
			{
				Type:          "FOREIGN KEY",
				StructName:    "User",
				Columns:       []string{"account_id"},
				ForeignTable:  "accounts",
				ForeignColumn: "id",
				OnDelete:      "CASCADE",
			},
		},
	}
	diff := &types.SchemaDiff{TablesAdded: []string{"users"}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)

	table, ok := nodes[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Constraints, qt.HasLen, 2)

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "users_email_check" CHECK (email <> '')`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "fk_users_account_id" FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") ON DELETE CASCADE`)
	c.Assert(sql, qt.Contains, "STRICT")
}

func TestPlannerAddsColumnsAndIndexes(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{
			{Name: "display_name", Type: "TEXT", StructName: "User", Nullable: true},
		},
		Indexes: []goschema.Index{
			{
				Name:       "idx_users_display_name",
				StructName: "User",
				Fields:     []string{"display_name"},
				Unique:     true,
				Condition:  "display_name IS NOT NULL",
			},
		},
	}
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{TableName: "users", ColumnsAdded: []string{"display_name"}},
		},
		IndexesAdded: []string{"idx_users_display_name"},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "users" ADD COLUMN "display_name" TEXT`)
	c.Assert(sql, qt.Contains, `CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_display_name" ON "users" ("display_name") WHERE display_name IS NOT NULL`)
}

func TestPlannerRebuildsTableWhenDroppingColumn(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "email", Type: "TEXT", StructName: "User", Nullable: false},
		},
		Indexes: []goschema.Index{{
			Name:       "idx_users_email",
			StructName: "User",
			Fields:     []string{"email"},
		}},
		Triggers: []goschema.Trigger{{
			Name:    "trg_users_email",
			Table:   "users",
			Timing:  "AFTER",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "BEGIN SELECT NEW.email; END",
		}},
	}
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: []string{"name"},
	}}}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
	c.Assert(sql, qt.Contains, `INSERT INTO "__ptah_rebuild_users" ("id", "email") SELECT "id", "email" FROM "users";`)
	c.Assert(sql, qt.Contains, `DROP TABLE "users";`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users" RENAME TO "users";`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(sql, qt.Contains, `CREATE TRIGGER "trg_users_email" AFTER UPDATE ON "users" FOR EACH ROW BEGIN SELECT NEW.email; END;`)
	c.Assert(sql, qt.Not(qt.Contains), "DROP COLUMN")
}

func TestPlannerRejectsUnsafeTableRebuildPreconditions(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		generated *goschema.Database
		want      string
	}{
		{
			name: "temporary table name collision",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "__ptah_rebuild_users", StructName: "RebuildUser"},
				},
				Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
			},
			want: `sqlite: rebuilding table users would collide with existing table __ptah_rebuild_users`,
		},
		{
			name: "inbound field foreign key",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
					{Name: "user_id", Type: "INTEGER", StructName: "Post", Foreign: "users(id)"},
				},
			},
			want: `sqlite: rebuilding table users with inbound foreign keys requires a manual rebuild plan`,
		},
		{
			name: "inbound table foreign key",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "memberships", StructName: "Membership"},
				},
				Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
				Constraints: []goschema.Constraint{{
					Type:           "FOREIGN KEY",
					Table:          "memberships",
					Columns:        []string{"user_id", "tenant_id"},
					ForeignTable:   "users",
					ForeignColumns: []string{"id", "tenant_id"},
				}},
			},
			want: `sqlite: rebuilding table users with inbound foreign keys requires a manual rebuild plan`,
		},
		{
			name: "unsupported trigger syntax",
			generated: &goschema.Database{
				Tables: []goschema.Table{{Name: "users", StructName: "User"}},
				Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
				Triggers: []goschema.Trigger{{
					Name:  "trg_users_email",
					Table: "users",
					Body:  "CREATE TRIGGER trg_users_email AFTER UPDATE OF email ON users BEGIN SELECT NEW.email; END",
				}},
			},
			want: `sqlite: rebuilding table users with trigger trg_users_email requires a manual rebuild plan`,
		},
	}
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: []string{"name"},
	}}}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			nodes, err := planner.GenerateSchemaDiffAST(diff, tt.generated, platform.SQLite)
			c.Assert(nodes, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestPlannerRejectsTableRebuildTempNameRemovedTableCollision(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
	}
	diff := &types.SchemaDiff{
		TablesRemoved: []string{"__ptah_rebuild_users"},
		TablesModified: []types.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: []string{"name"},
		}},
	}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `sqlite: rebuilding table users would collide with existing table __ptah_rebuild_users`)
}

func TestPlannerRejectsAddColumnShapesThatNeedRebuild(t *testing.T) {
	tests := []struct {
		name  string
		field goschema.Field
	}{
		{
			name:  "primary key",
			field: goschema.Field{Name: "account_id", Type: "INTEGER", StructName: "User", Primary: true},
		},
		{
			name:  "unique",
			field: goschema.Field{Name: "email", Type: "TEXT", StructName: "User", Unique: true},
		},
		{
			name:  "not null without default",
			field: goschema.Field{Name: "email", Type: "TEXT", StructName: "User", Nullable: false},
		},
		{
			name:  "foreign key with non null default",
			field: goschema.Field{Name: "account_id", Type: "INTEGER", StructName: "User", Nullable: true, Foreign: "accounts(id)", Default: "1"},
		},
		{
			name:  "expression default",
			field: goschema.Field{Name: "created_at", Type: "TEXT", StructName: "User", Nullable: true, DefaultExpr: "CURRENT_TIMESTAMP"},
		},
		{
			name: "stored generated column",
			field: goschema.Field{
				Name:                "slug",
				Type:                "TEXT",
				StructName:          "User",
				Nullable:            true,
				GeneratedExpression: "lower(name)",
				GeneratedKind:       "STORED",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "users", StructName: "User"}},
				Fields: []goschema.Field{tt.field},
			}
			diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:    "users",
				ColumnsAdded: []string{tt.field.Name},
			}}}

			nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)
			c.Assert(nodes, qt.IsNil)
			var planErr *ptaherr.PlanError
			c.Assert(err, qt.ErrorAs, &planErr)
			c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `sqlite: adding column `+tt.field.Name+` to table users requires a table rebuild plan`)
		})
	}
}

func TestPlannerDropsIndexesAndTables(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesRemovedWithTables: []types.IndexRemovalInfo{{Name: "idx_users_email", TableName: "users"}},
		TablesRemoved:            []string{"old_users"},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, &goschema.Database{}, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP INDEX IF EXISTS "idx_users_email"`)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "old_users"`)
}

func TestPlannerRejectsRebuildOnlyTableChanges(t *testing.T) {
	tests := []struct {
		name string
		diff *types.SchemaDiff
		want string
	}{
		{
			name: "modify column",
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{
				{TableName: "users", ColumnsModified: []types.ColumnDiff{{ColumnName: "name"}}},
			}},
			want: "sqlite: modifying columns on table users requires a table rebuild plan",
		},
		{
			name: "change constraints",
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{
				{TableName: "users", ConstraintsAdded: []string{"users_name_key"}},
			}},
			want: "sqlite: changing constraints on table users requires a table rebuild plan",
		},
		{
			name: "enum check drift",
			diff: &types.SchemaDiff{EnumsModified: []types.EnumDiff{{EnumName: "enum_users_status"}}},
			want: "sqlite: changing enum-backed CHECK constraints requires a table rebuild plan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := planner.GenerateSchemaDiffAST(tt.diff, &goschema.Database{}, platform.SQLite)
			c.Assert(nodes, qt.IsNil)
			var planErr *ptaherr.PlanError
			c.Assert(err, qt.ErrorAs, &planErr)
			c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestPlannerRejectsSQLiteExcludeConstraint(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "bookings", StructName: "Booking"}},
		Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "Booking", Primary: true}},
		Constraints: []goschema.Constraint{{
			Name:            "no_overlap",
			Type:            "EXCLUDE",
			StructName:      "Booking",
			UsingMethod:     "gist",
			ExcludeElements: "room_id WITH =",
		}},
	}
	diff := &types.SchemaDiff{TablesAdded: []string{"bookings"}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "sqlite: EXCLUDE constraints are not supported")
}
