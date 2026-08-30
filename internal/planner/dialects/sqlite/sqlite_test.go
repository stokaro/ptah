package sqlite_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPlannerCreatesTableWithInlineConstraints(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "accounts", StructName: "Account", Strict: true},
			{Name: "users", StructName: "User", Strict: true},
		},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "Account", Primary: true},
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "account_id", Type: "INTEGER", StructName: "User", Nullable: false},
			{Name: "email", Type: "TEXT", StructName: "User", Nullable: false},
		},
		Constraints: []schemamodel.Constraint{
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
	diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(desired, "users")}

	nodes, err := planner.GenerateSchemaDiffAST(withDeclaredTable(diff, desired), platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)

	table, ok := nodes[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Constraints, qt.HasLen, 2)

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "users_email_check" CHECK (email <> '')`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "fk_users_account_id" FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") ON DELETE CASCADE`)
	c.Assert(sql, qt.Contains, "STRICT")
}

func TestPlannerCreatesAddedTablesWithQualifiedConstraintDiffs(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "users", StructName: "users"},
			{Name: "posts", StructName: "posts"},
		},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "users", Primary: true},
			{Name: "id", Type: "INTEGER", StructName: "posts", Primary: true},
			{Name: "user_id", Type: "INTEGER", StructName: "posts", Nullable: false},
		},
		Constraints: []schemamodel.Constraint{{
			Name:          "fk_posts_user",
			Type:          "FOREIGN KEY",
			StructName:    "posts",
			Table:         "posts",
			Columns:       []string{"user_id"},
			ForeignTable:  "users",
			ForeignColumn: "id",
			OnDelete:      "CASCADE",
		}},
	}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, "posts", "users"),
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:          "fk_posts_user",
			TableName:     "posts",
			Type:          "FOREIGN KEY",
			Columns:       []string{"user_id"},
			ForeignTable:  "users",
			ForeignColumn: "id",
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(sql, qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE`)
}

func TestPlannerDropsTablesWithQualifiedConstraintDiffs(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"posts"},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
			Name:      "fk_posts_user",
			TableName: "posts",
			Type:      "FOREIGN KEY",
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "posts";`)
}

func TestPlannerAddsColumnsAndIndexes(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "display_name", Type: "TEXT", StructName: "User", Nullable: true},
		},
		Indexes: []schemamodel.Index{
			{
				Name:       "idx_users_display_name",
				StructName: "User",
				Fields:     []string{"display_name"},
				Unique:     true,
				Condition:  "display_name IS NOT NULL",
			},
		},
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{TableName: "users", ColumnsAdded: difftypes.ColumnChanges{{Name: "display_name", Type: "TEXT", StructName: "User", Nullable: true}}},
		},
		IndexesAdded: difftypes.IndexAdditionsFor(desired, difftypes.IndexRef{Name: "idx_users_display_name", TableName: "users"}),
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "users" ADD COLUMN "display_name" TEXT`)
	c.Assert(sql, qt.Contains, `CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_display_name" ON "users" ("display_name") WHERE display_name IS NOT NULL`)
}

func TestPlannerRebuildsTableWhenDroppingColumn(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "email", Type: "TEXT", StructName: "User", Nullable: false},
		},
		Indexes: []schemamodel.Index{{
			Name:       "idx_users_email",
			StructName: "User",
			Fields:     []string{"email"},
		}},
		Triggers: []schemamodel.Trigger{{
			Name:    "trg_users_email",
			Table:   "users",
			Timing:  "AFTER",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "BEGIN SELECT NEW.email; END",
		}},
	}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
	}}}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
	c.Assert(sql, qt.Contains, `INSERT INTO "__ptah_rebuild_users" ("id", "email") SELECT "id", "email" FROM "users";`)
	c.Assert(sql, qt.Contains, `DROP TABLE "users";`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users" RENAME TO "users";`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	c.Assert(sql, qt.Contains, `CREATE TRIGGER "trg_users_email" AFTER UPDATE ON "users" FOR EACH ROW BEGIN SELECT NEW.email; END;`)
	c.Assert(sql, qt.Not(qt.Contains), "DROP COLUMN")
}

// TestPlannerRebuildStepsAsideFromADeclaredTableName covers stokaro/ptah#1707.
//
// __ptah_rebuild_users is an ordinary identifier and a schema is allowed to
// contain a table by that name. Refusing asked the operator to rename their own
// table so that a name Ptah chose was free; the collision is Ptah's to resolve,
// so the scratch table takes the next free name instead.
func TestPlannerRebuildStepsAsideFromADeclaredTableName(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "users", StructName: "User"},
			{Name: "__ptah_rebuild_users", StructName: "RebuildUser"},
		},
		Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
		}},
		// The declared tables a comparison fills. The rebuild asks this list
		// which names are taken, and the operator's own `__ptah_rebuild_users`
		// being in it is the whole subject of this test.
		DeclaredTables: desired.Tables,
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users_1"`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users_1" RENAME TO "users"`)
	// The operator's own table is untouched by the rebuild that stepped around
	// it: nothing drops it and nothing renames over it.
	c.Assert(sql, qt.Not(qt.Contains), `DROP TABLE "__ptah_rebuild_users"`)
}

func TestPlannerRejectsUnsafeTableRebuildPreconditions(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name: "unsupported trigger syntax",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
				Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
				Triggers: []schemamodel.Trigger{{
					Name:  "trg_users_email",
					Table: "users",
					Body:  "CREATE TRIGGER trg_users_email AFTER UPDATE OF email ON users BEGIN SELECT NEW.email; END",
				}},
			},
			want: `(?s)sqlite: rebuilding table users cannot recreate trigger trg_users_email: its body is itself a CREATE TRIGGER statement.*`,
		},
	}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
	}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(withDeclaredTable(diff, tt.desired), platform.SQLite)
			c.Assert(nodes, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

// TestPlannerRebuildsATableOtherTablesReferTo pins what used to be a refusal.
// A rebuild drops the original table, which another table's foreign key makes
// illegal while enforcement is on, so the plan brackets itself in the pragmas
// SQLite's own ALTER TABLE procedure prescribes -- the same bracket the pinned
// community binary emits. See stokaro/ptah#1561.
func TestPlannerRebuildsATableOtherTablesReferTo(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
	}{
		{
			name: "a field declares the reference",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
					{Name: "posts", StructName: "Post"},
				},
				Fields: []schemamodel.Field{
					{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
					{Name: "user_id", Type: "INTEGER", StructName: "Post", Foreign: "users(id)"},
				},
			},
		},
		{
			name: "a table constraint declares the reference",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
					{Name: "memberships", StructName: "Membership"},
				},
				Fields: []schemamodel.Field{
					{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
					{Name: "tenant_id", Type: "INTEGER", StructName: "User", Primary: true},
					{Name: "user_id", Type: "INTEGER", StructName: "Membership"},
					{Name: "tenant_id", Type: "INTEGER", StructName: "Membership"},
				},
				Constraints: []schemamodel.Constraint{{
					Type:           "FOREIGN KEY",
					Table:          "memberships",
					Columns:        []string{"user_id", "tenant_id"},
					ForeignTable:   "users",
					ForeignColumns: []string{"id", "tenant_id"},
				}},
			},
		},
	}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
	}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, tt.desired), platform.SQLite)
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "PRAGMA foreign_keys = off;")
			c.Assert(sql, qt.Contains, "PRAGMA foreign_keys = on;")
			c.Assert(sql, qt.Contains, `DROP TABLE "users";`)

			// The bracket has to enclose the rebuild, not merely accompany it.
			c.Assert(
				strings.Index(sql, "PRAGMA foreign_keys = off;") < strings.Index(sql, `DROP TABLE "users";`),
				qt.IsTrue,
			)
			c.Assert(
				strings.Index(sql, `DROP TABLE "users";`) < strings.Index(sql, "PRAGMA foreign_keys = on;"),
				qt.IsTrue,
			)
		})
	}
}

// TestPlannerRebuildStepsAsideFromARemovedTableName covers stokaro/ptah#1707.
//
// A name the diff is DROPPING is still unusable: the drop and the rebuild land
// in one plan and their order is not the rebuild's to assume. So the scratch
// table steps aside rather than refusing, and the rebuild proceeds.
//
// The assertion names both halves. Reverting to the refusal fails on the nil
// error; picking the taken name anyway fails on the second assertion, which is
// the one no single-name test could make.
func TestPlannerRebuildStepsAsideFromARemovedTableName(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
	}
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"__ptah_rebuild_users"},
		TablesModified: []difftypes.TableDiff{{
			TableName:      "users",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "name"}},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users_1"`)
	c.Assert(sql, qt.Not(qt.Contains), `CREATE TABLE "__ptah_rebuild_users"`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users_1" RENAME TO "users"`)
}

// TestPlannerRebuildsForAddColumnShapesAlterCannotExpress covers
// stokaro/ptah#1707.
//
// Each row is a column shape `ALTER TABLE ... ADD COLUMN` rejects and
// CREATE TABLE accepts. Every one of them used to be answered
// "adding column X to table users requires a table rebuild plan" -- by a tool
// that writes rebuild plans, and that had always taken the same column without
// comment when the table was already being rebuilt for another reason.
//
// The assertion is that a rebuild is planned, named by the scratch table the
// rebuild moves through. Reverting the decision restores the refusal and every
// row fails on the nil error.
func TestPlannerRebuildsForAddColumnShapesAlterCannotExpress(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
	}{
		{
			name:  "primary key",
			field: schemamodel.Field{Name: "account_id", Type: "INTEGER", StructName: "User", Primary: true},
		},
		{
			name:  "unique",
			field: schemamodel.Field{Name: "email", Type: "TEXT", StructName: "User", Nullable: true, Unique: true},
		},
		{
			name:  "foreign key with non null default",
			field: schemamodel.Field{Name: "account_id", Type: "INTEGER", StructName: "User", Nullable: true, Foreign: "accounts(id)", Default: "1"},
		},
		{
			name:  "expression default",
			field: schemamodel.Field{Name: "created_at", Type: "TEXT", StructName: "User", Nullable: true, DefaultExpr: "CURRENT_TIMESTAMP"},
		},
		{
			name: "stored generated column",
			field: schemamodel.Field{
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
			desired := addColumnRebuildSchema(tt.field)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:    "users",
				ColumnsAdded: difftypes.ColumnChanges{tt.field},
			}}}

			sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
			c.Assert(sql, qt.Contains, `DROP TABLE "users"`)
			c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users" RENAME TO "users"`)
			// The new column has to reach the new table's definition. Copying it
			// out of the old table instead is the shape stokaro/ptah#930 names,
			// where SQLite reads the unknown identifier as a string literal and
			// every row silently receives the column's own name.
			c.Assert(sql, qt.Contains, tt.field.Name)
			c.Assert(sql, qt.Not(qt.Contains), "ADD COLUMN")
		})
	}
}

// TestPlannerRefusesRebuiltNotNullAddWithoutDefault is the one shape from
// stokaro/ptah#1707 that a rebuild cannot perform either: the copied rows have
// no value for the column, so the new table's NOT NULL is violated the moment
// the INSERT ... SELECT runs.
//
// It is refused before any SQL, and by the rebuild path rather than the
// ADD COLUMN path -- so the message names what the rebuild cannot do rather
// than asking for a rebuild that is already happening.
func TestPlannerRefusesRebuiltNotNullAddWithoutDefault(t *testing.T) {
	c := qt.New(t)

	field := schemamodel.Field{Name: "email", Type: "TEXT", StructName: "User", Nullable: false}
	desired := addColumnRebuildSchema(field)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:    "users",
		ColumnsAdded: difftypes.ColumnChanges{field},
	}}}

	nodes, err := planner.GenerateSchemaDiffAST(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s)sqlite: rebuilding table users cannot add NOT NULL column email without a default.*`)
}

// addColumnRebuildSchema is the two-table declaration both tests above vary a
// single field against.
//
// `users` keeps a column of its own. A rebuild copies the retained columns into
// the new table, so a users declared with nothing but the added field is a
// table with nothing to copy -- which is refused for that reason and would hide
// the shape under test behind an unrelated message.
func addColumnRebuildSchema(field schemamodel.Field) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "users", StructName: "User"},
			{Name: "accounts", StructName: "Account"},
		},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "id", Type: "INTEGER", StructName: "Account", Primary: true},
			field,
		},
	}
}

func TestPlannerDropsIndexesAndTables(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
		},
		TablesRemoved: []string{"old_users"},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP INDEX IF EXISTS "idx_users_email"`)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "old_users"`)
}

// usersRebuildSchema is the desired shape the rebuild tests converge on: a
// two-column table whose definition the caller adjusts per case.
func usersRebuildSchema(fields []schemamodel.Field, constraints []schemamodel.Constraint) *schemamodel.Database {
	return &schemamodel.Database{
		Tables:      []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields:      fields,
		Constraints: constraints,
	}
}

func usersNameField(name schemamodel.Field) []schemamodel.Field {
	return []schemamodel.Field{
		{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
		name,
	}
}

// TestPlannerRebuildsTableForChangesAlterTableCannotExpress pins one rebuild per
// diff shape that SQLite's ALTER TABLE grammar cannot carry. Each row asserts
// the create/copy/drop/rename sequence plus the shape-specific detail. Before
// the rebuild generalization every row returned an unsupported-feature error
// instead: "sqlite: modifying columns on table users requires a table rebuild
// plan" for the three column rows, "sqlite: changing constraints on table users
// requires a table rebuild plan" for the table-level constraint row, and
// "sqlite: changing constraints on existing tables requires a table rebuild
// plan" for the schema-level constraint and enum rows.
func TestPlannerRebuildsTableForChangesAlterTableCannotExpress(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		diff    *difftypes.SchemaDiff
		// wantSQL is the shape-specific detail the rebuild must carry, and
		// wantNoSQL what it must have left behind. The create/copy/drop/rename
		// sequence every row shares is asserted once, below.
		wantSQL   []string
		wantNoSQL []string
	}{
		{
			name: "column type change",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{Name: "name", Type: "INTEGER", StructName: "User"}),
				nil,
			),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:       "users",
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"type": "text -> integer"}}},
			}}},
			wantSQL: []string{
				`"name" INTEGER NOT NULL`,
				`SELECT "id", "name" FROM "users";`,
			},
		},
		{
			name: "column nullability change",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{Name: "name", Type: "TEXT", StructName: "User", Nullable: true}),
				nil,
			),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:       "users",
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"nullable": "false -> true"}}},
			}}},
			wantSQL:   []string{`"name" TEXT`},
			wantNoSQL: []string{`"name" TEXT NOT NULL`},
		},
		{
			name: "not null default addition backfills the copy",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{Name: "name", Type: "TEXT", StructName: "User", Default: "'x'"}),
				nil,
			),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: "users",
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "name",
					Changes:    map[string]string{"nullable": "true -> false", "default": " -> 'x'"},
				}},
			}}},
			wantSQL: []string{`SELECT "id", IFNULL("name", 'x') AS "name" FROM "users";`},
		},
		{
			name: "table-level constraint change",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{Name: "name", Type: "TEXT", StructName: "User"}),
				[]schemamodel.Constraint{{
					Name:            "users_name_check",
					Type:            "CHECK",
					StructName:      "User",
					CheckExpression: "length(name) > 2",
				}},
			),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:        "users",
				ConstraintsAdded: []string{"users_name_check"},
			}}},
			wantSQL: []string{`CONSTRAINT "users_name_check" CHECK (length(name) > 2)`},
		},
		{
			name: "schema-level constraint change on an existing table",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{Name: "name", Type: "TEXT", StructName: "User"}),
				[]schemamodel.Constraint{{
					Name:            "users_name_check",
					Type:            "CHECK",
					StructName:      "User",
					CheckExpression: "length(name) > 2",
				}},
			),
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
					Name:            "users_name_check",
					TableName:       "users",
					Type:            "CHECK",
					CheckExpression: "length(name) > 2",
				}},
			},
			wantSQL: []string{`CONSTRAINT "users_name_check" CHECK (length(name) > 2)`},
		},
		{
			name: "enum-backed check constraint change",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{
					Name:       "status",
					Type:       "TEXT",
					StructName: "User",
					Check:      "status IN ('draft', 'published')",
					CheckName:  "users_status_check",
				}),
				nil,
			),
			diff: &difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{{EnumName: "enum_users_status", ValuesRemoved: []string{"archived"}}},
				ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
					Name:            "users_status_check",
					TableName:       "users",
					Type:            "CHECK",
					CheckExpression: "status IN ('draft', 'published')",
				}},
				ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
					Name:      "users_status_check",
					TableName: "users",
					Type:      "CHECK",
				}},
			},
			wantSQL:   []string{`CHECK (status IN ('draft', 'published'))`},
			wantNoSQL: []string{"archived"},
		},
		{
			name: "dropped column combined with an added column",
			desired: usersRebuildSchema(
				usersNameField(schemamodel.Field{Name: "nickname", Type: "TEXT", StructName: "User", Nullable: true}),
				nil,
			),
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "users",
				ColumnsAdded:   difftypes.ColumnChanges{{Name: "nickname", Type: "TEXT", StructName: "User", Nullable: true}},
				ColumnsRemoved: difftypes.ColumnChanges{{Name: "email", Type: "TEXT", StructName: "User"}},
			}}},
			wantSQL: []string{
				`"nickname" TEXT`,
				`INSERT INTO "__ptah_rebuild_users" ("id") SELECT "id" FROM "users";`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(test.diff, test.desired), platform.SQLite)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
			c.Assert(sql, qt.Contains, `INSERT INTO "__ptah_rebuild_users"`)
			c.Assert(sql, qt.Contains, `DROP TABLE "users";`)
			c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users" RENAME TO "users";`)
			for _, want := range test.wantSQL {
				c.Assert(sql, qt.Contains, want)
			}
			for _, unwanted := range test.wantNoSQL {
				c.Assert(sql, qt.Not(qt.Contains), unwanted)
			}
		})
	}
}

// TestPlannerRebuildRefusesAddedNotNullColumnWithoutDefault keeps the rebuild
// from emitting a copy that cannot execute: the added column is absent from the
// INSERT list, so SQLite would fill it with NULL and abort on the first row.
// Reverting the guard prints a plan containing
// `INSERT INTO "__ptah_rebuild_users" ("id") SELECT "id" FROM "users";` and a
// nil error instead of the refusal.
func TestPlannerRebuildRefusesAddedNotNullColumnWithoutDefault(t *testing.T) {
	c := qt.New(t)

	desired := usersRebuildSchema(
		usersNameField(schemamodel.Field{Name: "email", Type: "TEXT", StructName: "User"}),
		nil,
	)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:      "users",
		ColumnsAdded:   difftypes.ColumnChanges{{Name: "email", Type: "TEXT", StructName: "User"}},
		ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy"}},
	}}}

	nodes, err := planner.GenerateSchemaDiffAST(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s)sqlite: rebuilding table users cannot add NOT NULL column email without a default.*`)
}

// TestPlannerRebuildEmitsIndexesAndTriggersOnce pins that a rebuilt table
// recreates its desired indexes and triggers exactly once. The rebuild drops
// the table, so the diff's own index and trigger additions must not be replayed
// on top. Reverting the skip prints the CREATE INDEX line twice and the CREATE
// TRIGGER statement twice.
func TestPlannerRebuildEmitsIndexesAndTriggersOnce(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "name", Type: "INTEGER", StructName: "User"},
		},
		Indexes: []schemamodel.Index{{
			Name:       "idx_users_name",
			StructName: "User",
			Fields:     []string{"name"},
		}},
		Triggers: []schemamodel.Trigger{{
			Name:    "trg_users_name",
			Table:   "users",
			Timing:  "AFTER",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "BEGIN SELECT NEW.name; END",
		}},
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:       "users",
			ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"type": "text -> integer"}}},
		}},
		IndexesAdded: difftypes.IndexAdditionsFor(desired, difftypes.IndexRef{Name: "idx_users_name", TableName: "users"}),
		TriggersAdded: []difftypes.TriggerRef{{
			TriggerName: "trg_users_name", TableName: "users",
			Desired: schemamodel.Trigger{
				StructName: "User", Name: "trg_users_name", Table: "users",
				Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "BEGIN SELECT NEW.id; END",
			},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, `CREATE INDEX IF NOT EXISTS "idx_users_name"`), qt.Equals, 1)
	c.Assert(strings.Count(sql, `CREATE TRIGGER "trg_users_name"`), qt.Equals, 1)
}

// TestPlannerEmitsTriggerChangesWithoutRebuild is the non-interference control
// for the rebuild skip in addTriggers and modifyTriggers: a table that is not
// being rebuilt must still get its added and replaced triggers. Inverting the
// skip so that only rebuilt tables emit prints zero occurrences of both
// statements and fails on the first count.
func TestPlannerEmitsTriggerChangesWithoutRebuild(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
		Triggers: []schemamodel.Trigger{
			{
				Name:    "trg_users_insert",
				Table:   "users",
				Timing:  "AFTER",
				Event:   "INSERT",
				ForEach: "ROW",
				Body:    "BEGIN SELECT NEW.id; END",
			},
			{
				Name:    "trg_users_update",
				Table:   "users",
				Timing:  "AFTER",
				Event:   "UPDATE",
				ForEach: "ROW",
				Body:    "BEGIN SELECT NEW.id; END",
			},
		},
	}
	diff := &difftypes.SchemaDiff{
		TriggersAdded: []difftypes.TriggerRef{{
			TriggerName: "trg_users_insert", TableName: "users",
			Desired: desired.Triggers[0],
		}},
		TriggersModified: []difftypes.TriggerDiff{{
			TriggerName: "trg_users_update", TableName: "users",
			Desired: desired.Triggers[1],
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, `CREATE TRIGGER "trg_users_insert"`), qt.Equals, 1)
	c.Assert(strings.Count(sql, `TRIGGER "trg_users_update"`), qt.Equals, 1)
}

func TestPlannerRejectsUnqualifiedExistingTableConstraintChanges(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "users_name_key"}}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "sqlite: changing constraints on existing tables requires a table rebuild plan")
}

func TestPlannerRejectsExtensionPlacementChanges(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ExtensionsModified: []difftypes.ExtensionDiff{{
		Name: "pgcrypto", FromSchema: "public", ToSchema: "extensions",
	}}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	// The name is part of the refusal now: "extensions are not supported" told
	// an operator that something in their schema is an extension, not which one
	// to remove (stokaro/ptah#1628).
	c.Assert(err, qt.ErrorMatches, "sqlite: extensions are not supported: pgcrypto")
}

func TestPlannerRejectsSQLiteExcludeConstraint(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "bookings", StructName: "Booking"}},
		Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", StructName: "Booking", Primary: true}},
		Constraints: []schemamodel.Constraint{{
			Name:            "no_overlap",
			Type:            "EXCLUDE",
			StructName:      "Booking",
			UsingMethod:     "gist",
			ExcludeElements: "room_id WITH =",
		}},
	}
	diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(desired, "bookings")}

	nodes, err := planner.GenerateSchemaDiffAST(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s)sqlite: table bookings declares EXCLUDE constraint no_overlap: SQLite has no EXCLUDE constraint.*`)
}

// TestPlannerRebuildExcludesColumnsAddedBesideAConstraintChange pins the
// intersection that corrupted data: one diff that BOTH adds a column and changes
// a table-level constraint on the same existing table.
//
// The added column must not appear in the rebuild's SELECT, because it does not
// exist in the old table. SQLite reads an unknown double-quoted identifier as a
// STRING LITERAL rather than refusing it, so copying `"note"` out of a table
// without that column writes the text `note` into every row — and `schema apply`
// exits 0 reporting success. Measured on a seeded database before the fix:
// (1,'alpha','note'), (2,'beta','note'), typeof text.
//
// Reverted, the INSERT reads `("id", "name", "note") SELECT "id", "name", "note"`
// and this row fails on the first assertion. The suite covers each half of the
// shape separately — a column added alone, a constraint changed alone — and
// neither reaches the bad SELECT, which is why the branch was green.
func TestPlannerRebuildExcludesColumnsAddedBesideAConstraintChange(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "name", Type: "TEXT", StructName: "User", Nullable: false},
			{Name: "note", Type: "TEXT", StructName: "User", Nullable: true},
		},
	}
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:    "users",
			ColumnsAdded: difftypes.ColumnChanges{{Name: "note", Type: "TEXT", StructName: "User", Nullable: true}},
		}},
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:      "uq_users_name",
			TableName: "users",
			Type:      "UNIQUE",
			Columns:   []string{"name"},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		`INSERT INTO "__ptah_rebuild_users" ("id", "name") SELECT "id", "name" FROM "users";`)
	c.Assert(sql, qt.Not(qt.Contains), `SELECT "id", "name", "note"`)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
	c.Assert(sql, qt.Contains, `"note" TEXT`)
}

// TestPlanner_RefusesAColumnOnARelationTheSchemaDoesNotDeclare is SQLite's
// answer to the property the PostgreSQL planner holds by writing nothing.
//
// A column travels WITH its change now (stokaro/ptah#2315), so the lookup that
// used to fail for an undeclared table cannot fail any more. On PostgreSQL the
// guard that lookup provided had to be restored explicitly; here it did not,
// because SQLite already refuses -- and refusing is the better answer, since an
// operator is told which table to declare rather than being handed a plan that
// quietly does less than they asked.
//
// Measured: adding the PostgreSQL-style guard here changed no test, because
// this refusal happens first.
func TestPlanner_RefusesAColumnOnARelationTheSchemaDoesNotDeclare(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users", Schema: "main"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER", Primary: true}},
	}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:    "other.users",
		ColumnsAdded: difftypes.ColumnChanges{{StructName: "User", Name: "note", Type: "TEXT"}},
	}}}

	sql, err := planner.GenerateSchemaDiffSQL(withDeclaredTable(diff, desired), platform.SQLite)

	c.Assert(err, qt.ErrorMatches, `.*requires its desired definition.*`)
	c.Assert(sql, qt.Equals, "",
		qt.Commentf("no DDL for a relation the desired schema never declared"))
}

// withDeclaredTable fills a fixture diff's per-table declaration from the
// declaration the plan is applied against, leaving one the fixture already
// supplied alone.
//
// A rebuild recreates the whole table -- its columns, constraints, indexes and
// triggers -- so the modification carries all four, and a comparison fills them
// on every run. A fixture states the CHANGE; restating the declaration beside it
// would put the same table in two places where a reader has to check they agree.
func withDeclaredTable(diff *difftypes.SchemaDiff, desired *schemamodel.Database) *difftypes.SchemaDiff {
	if diff == nil || desired == nil {
		return diff
	}
	completed := *diff
	completed.TablesModified = make([]difftypes.TableDiff, len(diff.TablesModified))
	copy(completed.TablesModified, diff.TablesModified)
	for i, tableDiff := range completed.TablesModified {
		if tableDiff.Desired.HasTable() {
			continue
		}
		if table, ok := declaredTableNamed(desired, tableDiff.TableName); ok {
			completed.TablesModified[i].Desired = difftypes.TableDeclarationFor(desired, table)
		}
	}
	if len(completed.DeclaredTables) == 0 {
		completed.DeclaredTables = desired.Tables
	}
	if len(completed.DeclaredConstraintHosts) == 0 {
		completed.DeclaredConstraintHosts = difftypes.ConstraintHostDeclarationsOf(
			desired, diff.ConstraintsAdded, diff.ConstraintsRemoved,
			diff.EffectiveIdentifierSemantics(platform.SQLite),
		)
	}
	return &completed
}

// declaredTableNamed resolves the table a fixture's modification names.
//
// Both spellings the declaration answers to are tried, and nothing looser. A
// fixture whose diff names a table by a spelling the declaration does not use
// carries no declaration, which is correct: a modification naming a relation
// the schema never declared must write no DDL, and a helper that resolved it by
// bare name would hand the planner the very table those tests assert it does not
// touch.
func declaredTableNamed(desired *schemamodel.Database, tableName string) (schemamodel.Table, bool) {
	for _, table := range desired.Tables {
		if table.Name == tableName || table.QualifiedName() == tableName {
			return table, true
		}
	}
	return schemamodel.Table{}, false
}

// declaringTheOnlyTable fills a fixture's modification with the declaration of
// the one table the fixture declares.
//
// It is for the identity fixtures, whose whole point is that the diff spells the
// table differently from the declaration: `main.notes` against a declared
// `notes`. A comparison resolves that and produces the modification from the
// table it matched, so the fixture says which table it means rather than asking
// a helper to guess -- and withDeclaredTable deliberately does not guess.
func declaringTheOnlyTable(diff *difftypes.SchemaDiff, desired *schemamodel.Database) *difftypes.SchemaDiff {
	if len(desired.Tables) != 1 {
		panic("declaringTheOnlyTable wants a declaration holding exactly one table")
	}
	completed := *diff
	completed.TablesModified = make([]difftypes.TableDiff, len(diff.TablesModified))
	copy(completed.TablesModified, diff.TablesModified)
	for i := range completed.TablesModified {
		completed.TablesModified[i].Desired = difftypes.TableDeclarationFor(desired, desired.Tables[0])
	}
	completed.DeclaredTables = desired.Tables
	// The one table is the only host a constraint change here can name, and
	// naming it by identity is the whole point of the fixtures that use this:
	// the diff and the constraint spell the table differently on purpose.
	completed.DeclaredConstraintHosts = []difftypes.TableDeclaration{
		difftypes.TableDeclarationFor(desired, desired.Tables[0]),
	}
	return &completed
}

// TestPlannerInlineConstraintsComeFromTheCreation pins WHERE a created table's
// constraints are read from.
//
// Every other creation fixture builds the diff from the same declaration it
// hands the planner, so either source answers alike and none of them separates
// the two. These do. It matters because the two disagree by direction: a
// rollback recreates the table the PRE-CHANGE database held, with the
// constraints that database had, and the creation is what carries them.
//
// SQLite has no ADD CONSTRAINT, so a constraint missing from the CREATE has no
// second chance -- the table comes back without it and the migration reports
// success (stokaro/ptah#2315).
func TestPlannerInlineConstraintsComeFromTheCreation(t *testing.T) {
	t.Run("a constraint only the creation carries is rendered", func(t *testing.T) {
		c := qt.New(t)
		// The declaration holds no constraint at all: this one is the
		// pre-change database's, which is what a rollback hands over.
		desired := &schemamodel.Database{
			Tables: []schemamodel.Table{{Name: "bookings", StructName: "Booking"}},
			Fields: []schemamodel.Field{{Name: "code", Type: "TEXT", StructName: "Booking"}},
		}
		diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(desired, "bookings")}
		diff.TablesAdded[0].Constraints = []schemamodel.Constraint{{
			Name: "uq_bookings_code", Type: "UNIQUE", StructName: "Booking",
			Table: "bookings", Columns: []string{"code"},
		}}

		nodes, err := planner.GenerateSchemaDiffAST(diff, platform.SQLite)

		c.Assert(err, qt.IsNil)
		sql, err := renderer.RenderSQL(platform.SQLite, nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `CONSTRAINT "uq_bookings_code" UNIQUE ("code")`)
	})

	t.Run("a constraint only the declaration holds is not rendered", func(t *testing.T) {
		c := qt.New(t)
		desired := &schemamodel.Database{
			Tables: []schemamodel.Table{{Name: "bookings", StructName: "Booking"}},
			Fields: []schemamodel.Field{{Name: "code", Type: "TEXT", StructName: "Booking"}},
			Constraints: []schemamodel.Constraint{{
				Name: "uq_bookings_code", Type: "UNIQUE", StructName: "Booking",
				Table: "bookings", Columns: []string{"code"},
			}},
		}
		// A creation built without the constraint, which is what a diff
		// describing a different schema than the one handed over looks like.
		diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{
			Name: "bookings", Table: desired.Tables[0], Fields: desired.Fields,
		}}}

		nodes, err := planner.GenerateSchemaDiffAST(diff, platform.SQLite)

		c.Assert(err, qt.IsNil)
		sql, err := renderer.RenderSQL(platform.SQLite, nodes...)
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `CREATE TABLE "bookings"`)
		c.Assert(sql, qt.Not(qt.Contains), "uq_bookings_code")
	})
}
