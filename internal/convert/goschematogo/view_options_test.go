package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
)

// TestRender_WritesViewOptionsAsAnnotations pins that a view's options and a
// materialized view's storage parameters are written in the options
// attribute, and that the written source declares them again. A description
// of a database written without them would declare views without options, and
// a plan from it would remove what the database holds.
func TestRender_WritesViewOptionsAsAnnotations(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Views:             []schemamodel.View{{Name: "report", Body: "SELECT 1 AS x", Options: []string{"security_invoker=true", "security_barrier=on"}}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "totals", Body: "SELECT 2 AS y", Options: []string{"fillfactor=70"}}},
	}

	source, err := renderOneFile(c, db)

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Contains, `options="security_invoker=true,security_barrier=on"`)
	c.Assert(source, qt.Contains, `options="fillfactor=70"`)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", source)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.Views[0].Options, qt.DeepEquals, db.Views[0].Options)
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews[0].Options, qt.DeepEquals, db.MaterializedViews[0].Options)
}

// TestRender_WritesNoOptionsForAViewWithout is the control: a view without
// options gets no attribute.
func TestRender_WritesNoOptionsForAViewWithout(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, &schemamodel.Database{Views: []schemamodel.View{{Name: "report", Body: "SELECT 1 AS x"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Contains, `//ptah:schema:view name="report"`)
	c.Assert(source, qt.Not(qt.Contains), `options=`)
}
