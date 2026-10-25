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

// tableDroppedAfterRawSQL removes the enum value b, which the plan does in raw
// SQL, and the table archive.
func tableDroppedAfterRawSQL() styleChange {
	return styleChange{declared: styleDeclaredWith(func(d *schemamodel.Database) {
		d.Enums = append(d.Enums, schemamodel.Enum{Name: "status", Values: []string{"a"}})
	}), held: styleHeldWith(func(h *catalog.Database) {
		h.Enums = append(h.Enums, catalog.Enum{Name: "status", Schema: "public", Values: []string{"a", "b"}})
		h.Tables = append(h.Tables, styleTable("archive", styleKey("id")))
	})}
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
// takes the sequence its column owns, a view a column type change rebuilds
// takes its triggers, and after a statement in raw SQL any object may be gone.
// Removed views are dropped readers first, so neither drop needs a guard or
// CASCADE.
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
		{name: "a table dropped after a statement in raw SQL", change: tableDroppedAfterRawSQL(), want: []string{
			`DROP TYPE "status__ptah_old"`,
			`DROP TABLE IF EXISTS "public"."archive"`,
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
