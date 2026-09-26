package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_TriggerClausesRoundTrip keeps a trigger's event list, WHEN
// condition and transition tables through an export to Go annotations and
// back. Without them, `ptah introspect` would write a trigger that fires on
// rows the condition excludes, and whose function reads transition tables that
// no longer exist.
func TestRender_TriggerClausesRoundTrip(t *testing.T) {
	c := qt.New(t)
	declared := []schemamodel.Trigger{
		{
			StructName: "Orders", Name: "orders_touch", Table: "orders", Timing: "BEFORE",
			Event: `INSERT OR UPDATE OF "Total", b`, ForEach: "ROW", When: "NEW.total > 0",
			Body: "RETURN NEW;",
		},
		{
			StructName: "Orders", Name: "orders_audit", Table: "orders", Timing: "AFTER",
			Event: "UPDATE", ForEach: "STATEMENT", OldTable: "old_rows", NewTable: "new_rows",
			Body: "RETURN NULL;",
		},
	}
	database := &schemamodel.Database{
		Tables:   []schemamodel.Table{{StructName: "Orders", Name: "orders"}},
		Fields:   []schemamodel.Field{{StructName: "Orders", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true}},
		Triggers: declared,
	}

	files, err := goschematogo.Render(database, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	parsed, err := goschema.ParseSource("schema.go", source.String())

	c.Assert(err, qt.IsNil, qt.Commentf("the exported source does not parse:\n%s", source.String()))
	got := make([][]string, len(parsed.Triggers))
	for i, trigger := range parsed.Triggers {
		got[i] = []string{trigger.Name, trigger.Event, trigger.ForEach, trigger.When, trigger.OldTable, trigger.NewTable}
	}
	c.Assert(got, qt.ContentEquals, [][]string{
		{"orders_touch", `INSERT OR UPDATE OF "Total", b`, "ROW", "NEW.total > 0", "", ""},
		{"orders_audit", "UPDATE", "STATEMENT", "", "old_rows", "new_rows"},
	})
}
