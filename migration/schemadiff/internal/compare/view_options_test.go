package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// viewOptionRows are the option lists a declaration and a database can hold,
// with the change the comparison reports for them. The database side is what
// PostgreSQL 18.6 stores for the spelling a statement wrote: `security_invoker
// = on` is held as security_invoker=on, and `fillfactor = '070'` as
// fillfactor=070.
var viewOptionRows = []struct {
	name     string
	declared []string
	held     []string
	want     map[string]string
}{
	{
		name:     "the declaration adds an option",
		declared: []string{"security_invoker=true"},
		want:     map[string]string{difftypes.OptionsChange: "(none) -> security_invoker=true"},
	},
	{
		// A view declared without options is a view without them, as one
		// declared without a check option is.
		name: "the declaration leaves out an option the database holds",
		held: []string{"security_barrier=true", "security_invoker=true"},
		want: map[string]string{difftypes.OptionsChange: "security_barrier=true, security_invoker=true -> (none)"},
	},
	{
		name:     "another value",
		declared: []string{"security_invoker=false"},
		held:     []string{"security_invoker=true"},
		want:     map[string]string{difftypes.OptionsChange: "security_invoker=true -> security_invoker=false"},
	},
	{
		name:     "the same options in another order and spelling",
		declared: []string{"security_invoker=true", "security_barrier=true"},
		held:     []string{"security_barrier=yes", "security_invoker=on"},
		want:     make(map[string]string),
	},
	{
		name: "neither side has one",
		want: make(map[string]string),
	},
}

// TestViewDefinitions_ComparesOptions pins that a view's options are part of
// its definition, and that the database's spelling of a value is not a
// difference.
func TestViewDefinitions_ComparesOptions(t *testing.T) {
	for _, test := range viewOptionRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitions(
				schemamodel.View{Name: "v", Body: "SELECT id FROM t", Options: test.declared},
				catalog.View{Name: "v", Schema: "public", Body: "SELECT id FROM t", Options: test.held},
			)

			c.Assert(diff.Changes, qt.DeepEquals, test.want)
			c.Assert(diff.PreviousOptions, qt.DeepEquals, test.held)
			c.Assert(diff.OptionsOnly(), qt.Equals, len(test.want) == 1)
		})
	}
}

// TestMaterializedViewDefinitions_ComparesOptions pins the same for a
// materialized view's storage parameters, and that a change to them alone
// keeps the view: a replacement would drop its rows.
func TestMaterializedViewDefinitions_ComparesOptions(t *testing.T) {
	for _, test := range viewOptionRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.MaterializedViewDefinitions(
				schemamodel.MaterializedView{Name: "m", Body: "SELECT id FROM t", Options: test.declared},
				catalog.MaterializedView{Name: "m", Schema: "public", Body: "SELECT id FROM t", Options: test.held},
			)

			c.Assert(diff.Changes, qt.DeepEquals, test.want)
			c.Assert(diff.PreviousOptions, qt.DeepEquals, test.held)
			c.Assert(diff.ChangesOptions(), qt.Equals, len(test.want) == 1)
		})
	}
}
