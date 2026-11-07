package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
)

// TestRender_WritesTheCheckOptionLevel pins that an inspected view's check
// option is written at the level the view holds, CASCADED as Atlas writes it,
// and that the document reads back as the same view. Writing LOCAL for every
// check option stated a level a CASCADED view does not hold
// (stokaro/ptah#5647).
func TestRender_WritesTheCheckOptionLevel(t *testing.T) {
	tests := []struct {
		name          string
		withCheck     bool
		local         bool
		wantAttribute string
	}{
		{name: "CASCADED", withCheck: true, wantAttribute: "  check_option = CASCADED\n"},
		{name: "LOCAL", withCheck: true, local: true, wantAttribute: "  check_option = LOCAL\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Views: []schemamodel.View{{
				Name: "public.own_rows", Body: "SELECT 1 AS v", WithCheck: test.withCheck, LocalCheck: test.local,
			}}}

			result := renderViewOptions(c, db)
			parsed, err := atlashcl.Parse(c.Context(), result.Data, "schema.hcl")

			c.Assert(string(result.Data), qt.Contains, test.wantAttribute)
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Views, qt.HasLen, 1)
			c.Assert(parsed.Views[0].WithCheck, qt.Equals, test.withCheck)
			c.Assert(parsed.Views[0].LocalCheck, qt.Equals, test.local)
		})
	}
}

// TestRender_WritesNoCheckOptionForAViewWithoutOne is the control for the
// level rows: a view without a check option gets no attribute.
func TestRender_WritesNoCheckOptionForAViewWithoutOne(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Views: []schemamodel.View{{Name: "public.own_rows", Body: "SELECT 1 AS v", LocalCheck: true}}}

	result := renderViewOptions(c, db)

	c.Assert(string(result.Data), qt.Not(qt.Contains), "check_option")
}
