package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// sequenceOwnedByDroppedTable removes the table archive and the sequence
// archive_id_seq its id column owns.
func sequenceOwnedByDroppedTable() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		key := styleKey("id")
		key.ColumnDefault = new("nextval('archive_id_seq'::regclass)")
		h.Tables = append(h.Tables, styleTable("archive", key))
		h.Sequences = append(h.Sequences, catalog.Sequence{Name: "archive_id_seq", Schema: "public", DataType: "integer", OwnedBy: "archive.id"})
	})}
}

// triggerOnRebuiltView retypes items.sku under item_skus, which the plan
// drops and creates again, and removes a trigger on the view.
func triggerOnRebuiltView() styleChange {
	change := retypedUnderViews()
	change.held.Triggers = []catalog.Trigger{{Name: "item_skus_insert", Schema: "public", Table: "item_skus", Timing: "INSTEAD OF",
		Event: "INSERT", ForEach: "ROW", ExecuteFunction: "insert_item_sku"}}
	return change
}

// withEnumRebuild adds to change the removal of the enum value b, which the plan
// does by rebuilding the enum in raw SQL ahead of every other drop.
func withEnumRebuild(change styleChange) styleChange {
	change.declared.Enums = append(change.declared.Enums, schemamodel.Enum{Name: "status", Values: []string{"a"}})
	change.held.Enums = append(change.held.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
	return change
}

// removedViewUnderRemovedView removes owner_ids and owner_list, which reads
// it. The reader's name sorts after the view it reads.
func removedViewUnderRemovedView() styleChange {
	return styleChange{declared: styleDeclared(), held: styleHeldWith(func(h *catalog.Database) {
		h.Views = []catalog.View{
			{Name: "owner_ids", Schema: "public", Body: "SELECT id FROM owners"},
			{Name: "owner_list", Schema: "public", Body: "SELECT id FROM owner_ids"},
		}
	})}
}

// drops is the statements of plan that drop something, in plan order.
func drops(plan []string) []string {
	var dropped []string
	for _, statement := range plan {
		if strings.HasPrefix(statement, "DROP ") || strings.Contains(statement, " DROP ") {
			dropped = append(dropped, statement)
		}
	}
	return dropped
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleKeepsGuards_HappyPath
// pins each reason the exact style keeps IF EXISTS on a drop: an earlier
// statement of the plan may have removed the object already. A dropped table
// takes the sequence its column owns, and a view a column type change rebuilds
// takes its triggers. Removed views are dropped readers first, so neither drop
// needs a guard or CASCADE.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleKeepsGuards_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		change styleChange
		want   []string
	}{
		{name: "a sequence a dropped table owned", change: sequenceOwnedByDroppedTable(), want: []string{
			`DROP TABLE "public"."archive"`,
			`DROP SEQUENCE IF EXISTS "public"."archive_id_seq"`,
		}},
		{name: "a trigger on a view a column type change rebuilds", change: triggerOnRebuiltView(), want: []string{
			`DROP VIEW "item_sku_count"`,
			`DROP VIEW "item_skus"`,
			`DROP TRIGGER IF EXISTS "item_skus_insert" ON "public"."item_skus"`,
		}},
		{name: "a view a removed view reads", change: removedViewUnderRemovedView(), want: []string{
			`DROP VIEW "public"."owner_list"`,
			`DROP VIEW "public"."owner_ids"`,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			opts := tt.change.opts
			opts.Statements = planext.StatementStyleExact

			got := stylePlan(c, tt.change.declared, tt.change.held, opts)

			c.Assert(drops(got), qt.DeepEquals, tt.want, qt.Commentf("%q", got))
		})
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_AnEnumRebuildMakesNoGuardNecessary_HappyPath
// holds the drops of each kind behind an enum rebuild to the statements they get
// without it. The rebuild is raw SQL, which the guard pass cannot read, and the
// only object it removes is the type it renamed. The last two rows are the guards a
// structured statement of the plan makes necessary, which stay.
func TestGenerateSchemaDiffSQLStatementsWithOptions_AnEnumRebuildMakesNoGuardNecessary_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		change styleChange
		want   []string
	}{
		{name: "an index", change: droppedIndex(), want: []string{`DROP INDEX "public"."items_sku_idx"`}},
		{name: "a table", change: droppedTable(), want: []string{`DROP TABLE "public"."archive"`}},
		{name: "a view and a view it reads", change: removedViewUnderRemovedView(), want: []string{
			`DROP VIEW "public"."owner_list"`,
			`DROP VIEW "public"."owner_ids"`,
		}},
		{name: "a sequence a dropped table owned", change: sequenceOwnedByDroppedTable(), want: []string{
			`DROP TABLE "public"."archive"`,
			`DROP SEQUENCE IF EXISTS "public"."archive_id_seq"`,
		}},
		{name: "a trigger on a view a column type change rebuilds", change: triggerOnRebuiltView(), want: []string{
			`DROP VIEW "item_sku_count"`,
			`DROP VIEW "item_skus"`,
			`DROP TRIGGER IF EXISTS "item_skus_insert" ON "public"."item_skus"`,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			change := withEnumRebuild(tt.change)
			opts := change.opts
			opts.Statements = planext.StatementStyleExact

			got := stylePlan(c, change.declared, change.held, opts)

			c.Assert(drops(got), qt.DeepEquals, append([]string{`DROP TYPE "status__ptah_old"`}, tt.want...), qt.Commentf("%q", got))
		})
	}
}

// enumRebuildDiff rebuilds the enum status with its one column, and removes the
// index i and the table archive. The two drops read the guard from the statements
// ahead of them.
func enumRebuildDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		EnumsModified:     []difftypes.EnumDiff{{EnumName: "status", ValuesRemoved: []string{"b"}, Usages: []difftypes.EnumColumnUsage{{Table: "t", Column: "s"}}}},
		DeclaredUserTypes: difftypes.UserTypeVocabulary{Enums: []schemamodel.Enum{{Name: "status", Values: []string{"a"}}}},
		IndexesRemoved:    []difftypes.IndexRef{{Name: "i", TableName: "t"}},
		TablesRemoved:     difftypes.TableRemovals{{Name: "archive"}},
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_AnEnumRebuildMakesNoGuardNecessaryOnTheFamily_HappyPath
// holds every dialect the PostgreSQL planner serves to the same statements,
// because each passes through the same guard pass.
func TestGenerateSchemaDiffSQLStatementsWithOptions_AnEnumRebuildMakesNoGuardNecessaryOnTheFamily_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		// index is how the dialect names an index it drops.
		index string
	}{
		{dialect: platform.Postgres, index: `"i"`},
		{dialect: platform.CockroachDB, index: `"t"@"i"`},
		{dialect: platform.YugabyteDB, index: `"i"`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), enumRebuildDiff(), test.dialect,
				planext.Options{Capabilities: capability.ForDialect(test.dialect), Statements: planext.StatementStyleExact})

			c.Assert(err, qt.IsNil)
			c.Assert(drops(got), qt.DeepEquals, []string{`DROP TYPE "status__ptah_old"`, "DROP INDEX " + test.index, `DROP TABLE "archive"`}, qt.Commentf("%q", got))
		})
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ARawStatementBesideAnEnumRebuildKeepsTheGuard_HappyPath
// is the reason the rebuild is recognized by its whole text. A constraint with no
// table is dropped by a DO block that finds its table when it runs, which may take
// the index behind it, so what follows keeps its guard, and a rebuild in the same
// plan does not vouch for it.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ARawStatementBesideAnEnumRebuildKeepsTheGuard_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := enumRebuildDiff()
	diff.ConstraintsRemoved = difftypes.ConstraintRemovals{{Name: "uq"}}

	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Contains, `DROP INDEX "i"`, qt.Commentf("%q", got))
	c.Assert(got, qt.Contains, `DROP TABLE IF EXISTS "archive"`, qt.Commentf("%q", got))
}

// keyLeavingForRowIDDiff removes the primary key of table, which CockroachDB holds on
// every table, so the plan moves it to a hidden column in a raw statement. It removes
// the view v and the table archive after that.
func keyLeavingForRowIDDiff(table string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "t_pkey", TableName: table, Type: "PRIMARY KEY"}},
		ViewsRemoved:       difftypes.ViewChanges{{Name: "v"}},
		TablesRemoved:      difftypes.TableRemovals{{Name: "archive"}},
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ACockroachDBKeyLeavingForRowIDMakesNoGuardNecessary_HappyPath
// holds the drops behind that raw statement to what they get without it. ADD COLUMN
// removes nothing a later drop names.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ACockroachDBKeyLeavingForRowIDMakesNoGuardNecessary_HappyPath(t *testing.T) {
	tests := []struct {
		name, table string
		// quoted is how the statements write the table.
		quoted string
	}{
		{name: "a table in the default schema", table: "t", quoted: `"t"`},
		{name: "a table of a named schema", table: "app.t", quoted: `"app"."t"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), keyLeavingForRowIDDiff(test.table), platform.CockroachDB,
				planext.Options{Capabilities: capability.ForDialect(platform.CockroachDB), Statements: planext.StatementStyleExact})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, []string{
				"ALTER TABLE " + test.quoted + ` ADD COLUMN "rowid" INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid()`,
				"ALTER TABLE " + test.quoted + ` DROP CONSTRAINT "t_pkey", ADD CONSTRAINT "t_pkey" PRIMARY KEY ("rowid")`,
				`DROP VIEW "v"`,
				`DROP TABLE "archive"`,
			})
		})
	}
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ARawStatementBesideAKeyLeavingForRowIDKeepsTheGuard_HappyPath
// is the name-only constraint drop again, which may take an index, in a plan that
// also moves a key: only the statement that adds the column is known to remove
// nothing.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ARawStatementBesideAKeyLeavingForRowIDKeepsTheGuard_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := keyLeavingForRowIDDiff("t")
	diff.ConstraintsRemoved = append(diff.ConstraintsRemoved, difftypes.ConstraintRemovalInfo{Name: "uq"})

	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, platform.CockroachDB,
		planext.Options{Capabilities: capability.ForDialect(platform.CockroachDB), Statements: planext.StatementStyleExact})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Contains, `DROP VIEW "v"`, qt.Commentf("%q", got))
	c.Assert(got, qt.Contains, `DROP TABLE IF EXISTS "archive"`, qt.Commentf("%q", got))
}

// TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleGuardsTheTriggerFunction_HappyPath
// pins the guard on a routine a trigger's drop already took. A rollback of a
// trigger declared with a body removes the trigger, whose statement drops the
// function Ptah wrote for it too, so a removal of that function keeps IF
// EXISTS. The diff is built by hand: a read of the database names the function
// a trigger runs, which the trigger's drop leaves alone.
func TestGenerateSchemaDiffSQLStatementsWithOptions_ExactStyleGuardsTheTriggerFunction_HappyPath(t *testing.T) {
	c := qt.New(t)
	function := schemamodel.Trigger{Name: "owners_audit", Table: "owners"}.FunctionName()
	diff := &difftypes.SchemaDiff{
		TriggersRemoved:  []difftypes.TriggerRef{{TriggerName: "owners_audit", TableName: "owners"}},
		FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: function}, Signature: new("")}},
	}

	got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: planext.StatementStyleExact})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		`DROP TRIGGER "owners_audit" ON "owners"`,
		`DROP FUNCTION "` + function + `"()`,
		`DROP FUNCTION IF EXISTS "` + function + `"()`,
	})
}
