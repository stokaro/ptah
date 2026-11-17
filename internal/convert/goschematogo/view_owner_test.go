package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
)

// TestRender_WritesViewOwnersAsAnnotations pins that the owner a view and a
// materialized view declare is written in the owner attribute, and that the
// written source declares it again. A desired schema written as Go without it
// would leave the owner unmanaged.
func TestRender_WritesViewOwnersAsAnnotations(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Views:             []schemamodel.View{{Name: "report", Body: "SELECT 1 AS x", Owner: "reporting"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "totals", Body: "SELECT 2 AS y", Owner: "Report Owner"}},
	}

	source, err := renderOneFile(c, db)

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Contains, `owner="reporting"`)
	c.Assert(source, qt.Contains, `owner="Report Owner"`)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", source)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.Views[0].Owner, qt.Equals, "reporting")
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews[0].Owner, qt.Equals, "Report Owner")
}

// TestRender_WritesNoOwnerForAViewWithout is the control: a view that
// declares no owner gets no attribute.
func TestRender_WritesNoOwnerForAViewWithout(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, &schemamodel.Database{Views: []schemamodel.View{{Name: "report", Body: "SELECT 1 AS x"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Not(qt.Contains), `owner=`)
}
