package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
)

// TestRender_WritesTheCheckOptionLevel pins that a view's check option is
// written in with_check at the level the view holds, and that the written
// source declares the same view again. A LOCAL view written as
// with_check="true" would come back as the stricter CASCADED one.
func TestRender_WritesTheCheckOptionLevel(t *testing.T) {
	tests := []struct {
		name          string
		withCheck     bool
		local         bool
		wantAttribute string
	}{
		{name: "CASCADED", withCheck: true, wantAttribute: `with_check="true"`},
		{name: "LOCAL", withCheck: true, local: true, wantAttribute: `with_check="local"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Views: []schemamodel.View{{Name: "own_rows", Body: "SELECT 1 AS x", WithCheck: test.withCheck, LocalCheck: test.local}}}

			source, err := renderOneFile(c, db)
			c.Assert(err, qt.IsNil)
			parsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", source)

			c.Assert(source, qt.Contains, test.wantAttribute)
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Views, qt.HasLen, 1)
			c.Assert(parsed.Views[0].WithCheck, qt.Equals, test.withCheck)
			c.Assert(parsed.Views[0].LocalCheck, qt.Equals, test.local)
		})
	}
}

// TestRender_WritesNoCheckOptionForAViewWithoutOne is the control: a view
// without a check option gets no attribute, whatever LocalCheck says.
func TestRender_WritesNoCheckOptionForAViewWithoutOne(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, &schemamodel.Database{Views: []schemamodel.View{{Name: "own_rows", Body: "SELECT 1 AS x", LocalCheck: true}}})

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.Contains, `//ptah:schema:view name="own_rows"`)
	c.Assert(source, qt.Not(qt.Contains), `with_check=`)
}
