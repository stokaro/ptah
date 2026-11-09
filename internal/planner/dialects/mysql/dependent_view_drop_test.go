package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// MySQL keeps a view after its table is dropped, but querying the view fails.
// The plan must name that consequence when the declaration still holds the view.
func TestDropTableReadByRetainedMySQLViewWarns(t *testing.T) {
	runtime := must.Must(builtin.New())
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Type: "BASE TABLE", Columns: []catalog.Column{{Name: "id", DataType: "int"}}}},
		Views:  []catalog.View{{Name: "v", Body: "SELECT id FROM t"}},
	}

	for _, test := range []struct {
		name       string
		desired    *schemamodel.Database
		wantWarned bool
	}{
		{name: "retained view", desired: &schemamodel.Database{Views: []schemamodel.View{{Name: "v", Body: "SELECT id FROM t"}}}, wantWarned: true},
		{name: "removed view", desired: &schemamodel.Database{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, current, platform.MySQL, runtime))
			c.Assert(diff.TablesRemoved, qt.HasLen, 1)
			plan := must.Must(planner.GenerateSchemaDiffSQL(t.Context(), runtime, diff, platform.MySQL))
			c.Assert(strings.Contains(plan, "leaves view v invalid"), qt.Equals, test.wantWarned, qt.Commentf("plan:\n%s", plan))
			c.Assert(plan, qt.Contains, "DROP TABLE")
			if test.wantWarned {
				c.Assert(strings.Index(plan, "leaves view v invalid") < strings.Index(plan, "DROP TABLE"), qt.IsTrue,
					qt.Commentf("plan:\n%s", plan))
			}
		})
	}
}
