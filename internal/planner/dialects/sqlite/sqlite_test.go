package sqlite_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlannerCreatesTableWithInlineConstraints(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "accounts", StructName: "Account", Strict: true},
			{Name: "users", StructName: "User", Strict: true},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "Account", Primary: true},
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

func TestPlannerCreatesAddedTablesWithQualifiedConstraintDiffs(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "users"},
			{Name: "posts", StructName: "posts"},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "users", Primary: true},
			{Name: "id", Type: "INTEGER", StructName: "posts", Primary: true},
			{Name: "user_id", Type: "INTEGER", StructName: "posts", Nullable: false},
		},
		Constraints: []goschema.Constraint{{
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
	diff := &types.SchemaDiff{
		TablesAdded:      []string{"posts", "users"},
		ConstraintsAdded: []string{"fk_posts_user"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name:          "fk_posts_user",
			TableName:     "posts",
			Type:          "FOREIGN KEY",
			Columns:       []string{"user_id"},
			ForeignTable:  "users",
			ForeignColumn: "id",
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(sql, qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE`)
}

func TestPlannerDropsTablesWithQualifiedConstraintDiffs(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		TablesRemoved:      []string{"posts"},
		ConstraintsRemoved: []string{"fk_posts_user"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
			Name:      "fk_posts_user",
			TableName: "posts",
			Type:      "FOREIGN KEY",
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, &goschema.Database{}, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "posts";`)
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
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_display_name", TableName: "users"},
		},
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

// TestPlannerRebuildStepsAsideFromADeclaredTableName covers stokaro/ptah#1707.
//
// __ptah_rebuild_users is an ordinary identifier and a schema is allowed to
// contain a table by that name. Refusing asked the operator to rename their own
// table so that a name Ptah chose was free; the collision is Ptah's to resolve,
// so the scratch table takes the next free name instead.
func TestPlannerRebuildStepsAsideFromADeclaredTableName(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "__ptah_rebuild_users", StructName: "RebuildUser"},
		},
		Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
	}
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: []string{"name"},
	}}}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users_1"`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "__ptah_rebuild_users_1" RENAME TO "users"`)
	// The operator's own table is untouched by the rebuild that stepped around
	// it: nothing drops it and nothing renames over it.
	c.Assert(sql, qt.Not(qt.Contains), `DROP TABLE "__ptah_rebuild_users"`)
}

func TestPlannerRejectsUnsafeTableRebuildPreconditions(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		want      string
	}{
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
			want: `(?s)sqlite: rebuilding table users cannot recreate trigger trg_users_email: its body is itself a CREATE TRIGGER statement.*`,
		},
	}
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: []string{"name"},
	}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(diff, tt.generated, platform.SQLite)
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
		name      string
		generated *goschema.Database
	}{
		{
			name: "a field declares the reference",
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
		},
		{
			name: "a table constraint declares the reference",
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
					{Name: "memberships", StructName: "Membership"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
					{Name: "tenant_id", Type: "INTEGER", StructName: "User", Primary: true},
					{Name: "user_id", Type: "INTEGER", StructName: "Membership"},
					{Name: "tenant_id", Type: "INTEGER", StructName: "Membership"},
				},
				Constraints: []goschema.Constraint{{
					Type:           "FOREIGN KEY",
					Table:          "memberships",
					Columns:        []string{"user_id", "tenant_id"},
					ForeignTable:   "users",
					ForeignColumns: []string{"id", "tenant_id"},
				}},
			},
		},
	}
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:      "users",
		ColumnsRemoved: []string{"name"},
	}}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(diff, tt.generated, platform.SQLite)
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

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

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
		field goschema.Field
	}{
		{
			name:  "primary key",
			field: goschema.Field{Name: "account_id", Type: "INTEGER", StructName: "User", Primary: true},
		},
		{
			name:  "unique",
			field: goschema.Field{Name: "email", Type: "TEXT", StructName: "User", Nullable: true, Unique: true},
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
			generated := addColumnRebuildSchema(tt.field)
			diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:    "users",
				ColumnsAdded: []string{tt.field.Name},
			}}}

			sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

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

	field := goschema.Field{Name: "email", Type: "TEXT", StructName: "User", Nullable: false}
	generated := addColumnRebuildSchema(field)
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:    "users",
		ColumnsAdded: []string{field.Name},
	}}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)

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
func addColumnRebuildSchema(field goschema.Field) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "accounts", StructName: "Account"},
		},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "id", Type: "INTEGER", StructName: "Account", Primary: true},
			field,
		},
	}
}

func TestPlannerDropsIndexesAndTables(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
		},
		TablesRemoved: []string{"old_users"},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, &goschema.Database{}, platform.SQLite)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP INDEX IF EXISTS "idx_users_email"`)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "old_users"`)
}

// usersRebuildSchema is the desired shape the rebuild tests converge on: a
// two-column table whose definition the caller adjusts per case.
func usersRebuildSchema(fields []goschema.Field, constraints []goschema.Constraint) *goschema.Database {
	return &goschema.Database{
		Tables:      []goschema.Table{{Name: "users", StructName: "User"}},
		Fields:      fields,
		Constraints: constraints,
	}
}

func usersNameField(name goschema.Field) []goschema.Field {
	return []goschema.Field{
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
		name      string
		generated *goschema.Database
		diff      *types.SchemaDiff
		// wantSQL is the shape-specific detail the rebuild must carry, and
		// wantNoSQL what it must have left behind. The create/copy/drop/rename
		// sequence every row shares is asserted once, below.
		wantSQL   []string
		wantNoSQL []string
	}{
		{
			name: "column type change",
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{Name: "name", Type: "INTEGER", StructName: "User"}),
				nil,
			),
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:       "users",
				ColumnsModified: []types.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"type": "text -> integer"}}},
			}}},
			wantSQL: []string{
				`"name" INTEGER NOT NULL`,
				`SELECT "id", "name" FROM "users";`,
			},
		},
		{
			name: "column nullability change",
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{Name: "name", Type: "TEXT", StructName: "User", Nullable: true}),
				nil,
			),
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:       "users",
				ColumnsModified: []types.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"nullable": "false -> true"}}},
			}}},
			wantSQL:   []string{`"name" TEXT`},
			wantNoSQL: []string{`"name" TEXT NOT NULL`},
		},
		{
			name: "not null default addition backfills the copy",
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{Name: "name", Type: "TEXT", StructName: "User", Default: "'x'"}),
				nil,
			),
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName: "users",
				ColumnsModified: []types.ColumnDiff{{
					ColumnName: "name",
					Changes:    map[string]string{"nullable": "true -> false", "default": " -> 'x'"},
				}},
			}}},
			wantSQL: []string{`SELECT "id", IFNULL("name", 'x') AS "name" FROM "users";`},
		},
		{
			name: "table-level constraint change",
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{Name: "name", Type: "TEXT", StructName: "User"}),
				[]goschema.Constraint{{
					Name:            "users_name_check",
					Type:            "CHECK",
					StructName:      "User",
					CheckExpression: "length(name) > 2",
				}},
			),
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:        "users",
				ConstraintsAdded: []string{"users_name_check"},
			}}},
			wantSQL: []string{`CONSTRAINT "users_name_check" CHECK (length(name) > 2)`},
		},
		{
			name: "schema-level constraint change on an existing table",
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{Name: "name", Type: "TEXT", StructName: "User"}),
				[]goschema.Constraint{{
					Name:            "users_name_check",
					Type:            "CHECK",
					StructName:      "User",
					CheckExpression: "length(name) > 2",
				}},
			),
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"users_name_check"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
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
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{
					Name:       "status",
					Type:       "TEXT",
					StructName: "User",
					Check:      "status IN ('draft', 'published')",
					CheckName:  "users_status_check",
				}),
				nil,
			),
			diff: &types.SchemaDiff{
				EnumsModified:      []types.EnumDiff{{EnumName: "enum_users_status", ValuesRemoved: []string{"archived"}}},
				ConstraintsAdded:   []string{"users_status_check"},
				ConstraintsRemoved: []string{"users_status_check"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:            "users_status_check",
					TableName:       "users",
					Type:            "CHECK",
					CheckExpression: "status IN ('draft', 'published')",
				}},
				ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{{
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
			generated: usersRebuildSchema(
				usersNameField(goschema.Field{Name: "nickname", Type: "TEXT", StructName: "User", Nullable: true}),
				nil,
			),
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:      "users",
				ColumnsAdded:   []string{"nickname"},
				ColumnsRemoved: []string{"email"},
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

			sql, err := planner.GenerateSchemaDiffSQL(test.diff, test.generated, platform.SQLite)

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

	generated := usersRebuildSchema(
		usersNameField(goschema.Field{Name: "email", Type: "TEXT", StructName: "User"}),
		nil,
	)
	diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
		TableName:      "users",
		ColumnsAdded:   []string{"email"},
		ColumnsRemoved: []string{"legacy"},
	}}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.SQLite)

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

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "name", Type: "INTEGER", StructName: "User"},
		},
		Indexes: []goschema.Index{{
			Name:       "idx_users_name",
			StructName: "User",
			Fields:     []string{"name"},
		}},
		Triggers: []goschema.Trigger{{
			Name:    "trg_users_name",
			Table:   "users",
			Timing:  "AFTER",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "BEGIN SELECT NEW.name; END",
		}},
	}
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:       "users",
			ColumnsModified: []types.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"type": "text -> integer"}}},
		}},
		IndexesAdded:  []types.IndexRef{{Name: "idx_users_name", TableName: "users"}},
		TriggersAdded: []types.TriggerRef{{TriggerName: "trg_users_name", TableName: "users"}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

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

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{{Name: "id", Type: "INTEGER", StructName: "User", Primary: true}},
		Triggers: []goschema.Trigger{
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
	diff := &types.SchemaDiff{
		TriggersAdded:    []types.TriggerRef{{TriggerName: "trg_users_insert", TableName: "users"}},
		TriggersModified: []types.TriggerDiff{{TriggerName: "trg_users_update", TableName: "users"}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, `CREATE TRIGGER "trg_users_insert"`), qt.Equals, 1)
	c.Assert(strings.Count(sql, `TRIGGER "trg_users_update"`), qt.Equals, 1)
}

func TestPlannerRejectsUnqualifiedExistingTableConstraintChanges(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{ConstraintsAdded: []string{"users_name_key"}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, &goschema.Database{}, platform.SQLite)

	c.Assert(nodes, qt.IsNil)
	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, platform.SQLite)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "sqlite: changing constraints on existing tables requires a table rebuild plan")
}

func TestPlannerRejectsExtensionPlacementChanges(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{ExtensionsModified: []types.ExtensionDiff{{
		Name: "pgcrypto", FromSchema: "public", ToSchema: "extensions",
	}}}

	nodes, err := planner.GenerateSchemaDiffAST(diff, &goschema.Database{}, platform.SQLite)

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

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Fields: []goschema.Field{
			{Name: "id", Type: "INTEGER", StructName: "User", Primary: true},
			{Name: "name", Type: "TEXT", StructName: "User", Nullable: false},
			{Name: "note", Type: "TEXT", StructName: "User", Nullable: true},
		},
	}
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:    "users",
			ColumnsAdded: []string{"note"},
		}},
		ConstraintsAdded: []string{"uq_users_name"},
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
			Name:      "uq_users_name",
			TableName: "users",
			Type:      "UNIQUE",
			Columns:   []string{"name"},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		`INSERT INTO "__ptah_rebuild_users" ("id", "name") SELECT "id", "name" FROM "users";`)
	c.Assert(sql, qt.Not(qt.Contains), `SELECT "id", "name", "note"`)
	c.Assert(sql, qt.Contains, `CREATE TABLE "__ptah_rebuild_users"`)
	c.Assert(sql, qt.Contains, `"note" TEXT`)
}
