package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// An enum's comment survives an export to HCL and back, so a schema inspected
// from a server that comments its enums does not plan the comments away when
// it is applied (stokaro/ptah#3646).
func TestRender_EnumCommentRoundTrip(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Enums: []schemamodel.Enum{
		{Name: "mood", Schema: "public", Values: []string{"ok"}, Comment: "how it went"},
		{Name: "plain", Schema: "public", Values: []string{"a"}},
	}}

	result, err := atlashclrender.RenderForDialect(database, "postgres")
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(result.Data, "schema.hcl")

	c.Assert(err, qt.IsNil, qt.Commentf("the exported HCL does not parse:\n%s", result.Data))
	c.Assert(parsed.Enums, qt.HasLen, 2)
	c.Assert([]string{parsed.Enums[0].Comment, parsed.Enums[1].Comment}, qt.DeepEquals, []string{"how it went", ""})
	c.Assert(string(result.Data), qt.Contains, `comment = "how it went"`)
}
