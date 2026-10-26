package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// styleTable is a table of the database the statement style rows plan
// against, with the columns the rows read.
func styleTable(name string, columns ...catalog.Column) catalog.Table {
	for i := range columns {
		columns[i].OrdinalPosition = i + 1
	}
	return catalog.Table{Name: name, Schema: "public", Type: "BASE TABLE", Columns: columns}
}

// styleKey is an integer primary key column.
func styleKey(name string) catalog.Column {
	return catalog.Column{Name: name, DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true}
}

// styleColumn is a nullable varchar column.
func styleColumn(name string) catalog.Column {
	return catalog.Column{Name: name, DataType: "character varying", UDTName: "varchar", IsNullable: "YES"}
}

// The two tables every row starts from, as the declaration spells them and as
// the database holds them: owners(id, name) and items(id, sku varchar).
func styleDeclared() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Owner", Name: "owners"}, {StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Owner", Name: "id", Type: "integer", Primary: true},
			{StructName: "Owner", Name: "name", Type: "varchar", Nullable: true},
			{StructName: "Item", Name: "id", Type: "integer", Primary: true},
			{StructName: "Item", Name: "sku", Type: "varchar", Nullable: true},
		},
	}
}

func styleHeld() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{
		styleTable("owners", styleKey("id"), styleColumn("name")),
		styleTable("items", styleKey("id"), styleColumn("sku")),
	}}
}

// stylePlan plans the change from held to declared in style.
func stylePlan(c *qt.C, declared *schemamodel.Database, held *catalog.Database, opts planext.Options) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, held,
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Postgres, opts)
	c.Assert(err, qt.IsNil)
	return statements
}

// styleChange is one change the statement style rows plan: the declaration,
// the database it is planned against, and the planning options.
type styleChange struct {
	declared *schemamodel.Database
	held     *catalog.Database
	opts     planext.Options
}

func styleDeclaredWith(change func(*schemamodel.Database)) *schemamodel.Database {
	declared := styleDeclared()
	change(declared)
	return declared
}

func styleHeldWith(change func(*catalog.Database)) *catalog.Database {
	held := styleHeld()
	change(held)
	return held
}

func droppedColumn() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
	})}
}

func droppedTable() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables = append(h.Tables, styleTable("archive", styleKey("id")))
	})}
}

func newIndex() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Indexes = append(d.Indexes, schemamodel.Index{StructName: "Owner", Name: "owners_name_idx", Fields: []string{"name"}})
	}), held: styleHeld()}
}

func droppedIndex() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Indexes = append(h.Indexes, catalog.Index{Name: "items_sku_idx", TableName: "items", Schema: "public", Columns: []string{"sku"}})
	})}
}

// retypedUnderViews retypes items.sku under item_skus and a view that reads
// item_skus, neither of which changes.
func retypedUnderViews() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Fields[3].Type = "VARCHAR(128)"
		d.Views = []schemamodel.View{
			{Name: "item_skus", Body: "SELECT id, sku FROM items"},
			{Name: "item_sku_count", Body: "SELECT sku, count(*) AS n FROM item_skus GROUP BY sku"},
		}
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{
			{Name: "item_skus", Schema: "public", Body: "SELECT id, sku FROM items"},
			{Name: "item_sku_count", Schema: "public", Body: "SELECT sku, count(*) AS n FROM item_skus GROUP BY sku"},
		}
	})}
}

// droppedUnderRemovedView drops items.sku, which a view the declaration
// removes reads. The removal step drops the view before the column.
func droppedUnderRemovedView() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Fields = d.Fields[:3]
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{{Name: "old_skus", Schema: "public", Body: "SELECT items.sku FROM items"}}
	})}
}

// checkDroppedWithItsColumn drops owners.email and the CHECK on it. The
// column drop takes the CHECK with it, and the constraint step runs after.
func checkDroppedWithItsColumn() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, styleColumn("email"))
		h.Constraints = append(h.Constraints, catalog.Constraint{Name: "owners_email_check", TableName: "owners", Schema: "public",
			Type: "CHECK", ColumnName: "email", ColumnNames: []string{"email"}, CheckClause: new("(email <> '')")})
	})}
}

// viewDroppedUnderADeclaredReader swaps the columns of owner_names, which
// CREATE OR REPLACE VIEW refuses, so the view is dropped; owner_ids reads it
// and is put back after it.
func viewDroppedUnderADeclaredReader() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Views = []schemamodel.View{
			{Name: "owner_names", Body: "SELECT name, id FROM owners"},
			{Name: "owner_ids", Body: "SELECT id FROM owner_names"},
		}
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{
			{Name: "owner_names", Schema: "public", Body: "SELECT id, name FROM owners"},
			{Name: "owner_ids", Schema: "public", Body: "SELECT id FROM owner_names"},
		}
	})}
}

func skippedTableDrop() styleChange {
	change := droppedTable()
	change.opts.SkipChangeKinds = []planext.ChangeKind{planext.DropTable}
	return change
}

func droppedEnum() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
	})}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyle_HappyPath pins the
// statements of the exact style: no comment that only describes a statement,
// no guard and no CASCADE the plan does not rely on.
//
// A removed view goes before the column it reads, so neither drop needs
// CASCADE or a guard. Two rows keep what the plan relies on, each because of a
// step that runs later in the plan. A CHECK on a dropped column is gone with
// the column when its drop runs, so that drop keeps IF EXISTS. A view dropped
// under a declared reader relies on CASCADE to take the reader, which the plan
// puts back with CREATE OR REPLACE. The skip row pins that a comment reporting
// what the plan left out is written in this style too. Every other reason to
// keep CASCADE or a guard has a row in
// [TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleReliance_HappyPath].
func TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyle_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		change styleChange
		want   []string
	}{
		{name: "a dropped column", change: droppedColumn(), want: []string{`ALTER TABLE "owners" DROP COLUMN "email"`}},
		{name: "a dropped table", change: droppedTable(), want: []string{`DROP TABLE "public"."archive"`}},
		{name: "a new index", change: newIndex(), want: []string{`CREATE INDEX "owners_name_idx" ON "owners" ("name")`}},
		{name: "a dropped index", change: droppedIndex(), want: []string{`DROP INDEX "public"."items_sku_idx"`}},
		{name: "a column retyped under views", change: retypedUnderViews(), want: []string{
			`DROP VIEW "item_sku_count"`,
			`DROP VIEW "item_skus"`,
			`ALTER TABLE "items" ALTER COLUMN "sku" TYPE VARCHAR(128)`,
			"CREATE VIEW \"item_skus\" AS\nSELECT id, sku FROM items",
			"CREATE VIEW \"item_sku_count\" AS\nSELECT sku, count(*) AS n FROM item_skus GROUP BY sku",
		}},
		{name: "a column dropped under a removed view", change: droppedUnderRemovedView(), want: []string{
			`DROP VIEW "public"."old_skus"`,
			`ALTER TABLE "items" DROP COLUMN "sku"`,
		}},
		{name: "a check dropped with its column", change: checkDroppedWithItsColumn(), want: []string{
			`ALTER TABLE "public"."owners" DROP CONSTRAINT "owners_email_check"`,
			`ALTER TABLE "owners" DROP COLUMN "email"`,
		}},
		{name: "a view dropped under a declared reader", change: viewDroppedUnderADeclaredReader(), want: []string{
			`DROP VIEW "owner_names" CASCADE`,
			"CREATE VIEW \"owner_names\" AS\nSELECT name, id FROM owners",
			"CREATE OR REPLACE VIEW \"owner_ids\" AS\nSELECT id FROM owner_names",
		}},
		{name: "a skipped table drop", change: skippedTableDrop(), want: []string{
			"-- SKIP: DROP TABLE of public.archive omitted by diff policy (skip: drop_table)",
		}},
		{name: "a dropped enum", change: droppedEnum(), want: []string{`DROP TYPE "public"."status"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			opts := tt.change.opts
			opts.Statements = planext.StatementStyleExact

			got := stylePlan(c, tt.change.declared, tt.change.held, opts)

			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_AnnotatedStyle_HappyPath is
// the control: the zero style keeps Ptah's descriptive comments, guards and
// CASCADE. Its retype row also pins what both styles share: the views leave
// readers first and come back with a plain CREATE, since the plan dropped
// them by name.
func TestGenerateSchemaDiffSQLStatementsWithOptions_AnnotatedStyle_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		change styleChange
		want   []string
	}{
		{name: "a dropped column", change: droppedColumn(), want: []string{
			"-- Remove columns from table: owners\nALTER TABLE \"owners\" DROP COLUMN \"email\" CASCADE",
			"-- WARNING: Dropping column owners.email with CASCADE - This will delete data and dependent objects!",
		}},
		{name: "a dropped table", change: droppedTable(), want: []string{
			"-- WARNING: This will delete all data!\nDROP TABLE IF EXISTS \"public\".\"archive\" CASCADE",
		}},
		{name: "a new index", change: newIndex(), want: []string{`CREATE INDEX IF NOT EXISTS "owners_name_idx" ON "owners" ("name")`}},
		{name: "a column retyped under views", change: retypedUnderViews(), want: []string{
			`DROP VIEW IF EXISTS "item_sku_count" CASCADE`,
			`DROP VIEW IF EXISTS "item_skus" CASCADE`,
			"-- Add/modify columns for table: items\n-- Modify column items.sku: type: varchar -> VARCHAR(128)\n" +
				`ALTER TABLE "items" ALTER COLUMN "sku" TYPE VARCHAR(128)`,
			"CREATE VIEW \"item_skus\" AS\nSELECT id, sku FROM items",
			"CREATE VIEW \"item_sku_count\" AS\nSELECT sku, count(*) AS n FROM item_skus GROUP BY sku",
		}},
		{name: "a dropped enum", change: droppedEnum(), want: []string{
			"-- WARNING: Make sure no tables use this enum!\nDROP TYPE IF EXISTS \"public\".\"status\" CASCADE",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := stylePlan(c, tt.change.declared, tt.change.held, tt.change.opts)

			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleOnMySQL_HappyPath
// pins the style on a planner that only marks its descriptive comments: the
// MySQL-family one leaves out its table header and its warnings, and keeps its
// own spelling of a drop, guard included, since it settles no guard.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleOnMySQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Owner", Name: "owners"}},
		Fields: []schemamodel.Field{
			{StructName: "Owner", Name: "id", Type: "INT", Primary: true},
			{StructName: "Owner", Name: "name", Type: "VARCHAR(255)", Nullable: true},
		},
	}
	held := &catalog.Database{Tables: []catalog.Table{
		{Name: "owners", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "name", DataType: "varchar", ColumnType: "varchar(255)", IsNullable: "YES", OrdinalPosition: 2},
			{Name: "email", DataType: "varchar", ColumnType: "varchar(255)", IsNullable: "YES", OrdinalPosition: 3},
		}},
		{Name: "archive", Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}},
	}}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, held,
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"}, nil, runtime)
	c.Assert(err, qt.IsNil)

	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.MySQL,
		planext.Options{Statements: planext.StatementStyleExact})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"ALTER TABLE `owners` DROP COLUMN `email`", "DROP TABLE IF EXISTS `archive`"})
}
