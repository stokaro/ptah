package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/internal/atlashcl"
)

func localColumnOwners(requests *[]hclext.Request) hclext.Set {
	return must.Must(hclext.NewSet(hclext.Extension{
		Owner: "example.org/custom", Decoder: shapeDecoder{requests: requests},
		Blocks: []hclext.Block{{Site: hclext.InTable, Type: "layout",
			Attributes: []hclext.Attribute{{Name: "columns", Kind: hclext.Refs, Refers: "column"},
				{Name: "anchor", Kind: hclext.Ref, Refers: "column", Optional: true}},
			Children: []hclext.Child{{Type: "by", Unlabeled: true, Attributes: []hclext.Attribute{
				{Name: "column", Kind: hclext.Ref, Refers: "column"},
				{Name: "also", Kind: hclext.Ref, Refers: "column", Optional: true},
			}}},
		}},
	}))
}

func TestParseWithOptions_OwnerLocalColumns_HappyPath(t *testing.T) {
	for _, spelling := range []string{`column["tenant.id"]`, `(column["tenant.id"])`, `true ? column["tenant.id"] : column.other`, `"tenant.id"`} {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			document := "table \"t\" {\n layout {\n columns = [" + spelling + ", column.z]\n anchor = " + spelling +
				"\n by { column = " + spelling + " }\n by { column = column.z }\n }\n}"
			_, err := atlashcl.ParseWithOptions([]byte(document), "schema.hcl", atlashcl.Options{HCL: localColumnOwners(&requests)})
			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 1)
			claim := requests[0].Claims[0]
			c.Assert(claim.Table.Name, qt.Equals, "t")
			c.Assert(claim.Attributes["columns"].References, qt.DeepEquals, []hclext.Reference{{Kind: "column", Name: "tenant.id"}, {Kind: "column", Name: "z"}})
			c.Assert(claim.Attributes["anchor"].Reference, qt.DeepEquals, &hclext.Reference{Kind: "column", Name: "tenant.id"})
			c.Assert(claim.Children, qt.HasLen, 2)
			c.Assert(claim.Children[0].Name, qt.Equals, "")
			c.Assert(claim.Children[0].Attributes["column"].Reference, qt.DeepEquals, &hclext.Reference{Kind: "column", Name: "tenant.id"})
			c.Assert(claim.Children[1].Attributes["column"].Reference, qt.DeepEquals, &hclext.Reference{Kind: "column", Name: "z"})
			c.Assert(claim.Children[0].At, qt.Equals, "schema.hcl:5")
			c.Assert(claim.Children[0].Attributes, qt.HasLen, 1)
		})
	}
}

func TestParseWithOptions_OwnerLocalColumns_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, body, want string }{
		{name: "label", body: `by "x" { column = column.a }`, want: "layout by does not accept labels"},
		{name: "missing required", body: `by {}`, want: "layout by requires column"},
		{name: "table reference", body: `by { column = table.t }`, want: `by attribute "column" requires a local column reference`},
		{name: "qualified reference", body: `by { column = table.other.column.a }`, want: `by attribute "column" requires a local column reference`},
		{name: "expression", body: `by { column = sql("a") }`, want: `by attribute "column" requires a local column reference`},
		{name: "numeric", body: `by { column = 42 }`, want: `by attribute "column" requires a local column reference`},
		{name: "invalid list", body: `columns = [column.a, role.b]`, want: `layout attribute "columns" requires a local column reference`},
		{name: "invalid optional", body: `anchor = table.t.column.a`, want: `layout attribute "anchor" requires a local column reference`},
		{name: "unknown attribute", body: `by { alien = true }`, want: `.*alien.*`},
		{name: "nested block", body: "by {\n nested {}\n}", want: `.*nested.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			db, err := atlashcl.ParseWithOptions([]byte("table \"t\" {\n layout {\n"+test.body+"\n }\n}"), "schema.hcl", atlashcl.Options{HCL: localColumnOwners(&requests)})
			c.Assert(err, qt.ErrorMatches, "(?s).*schema.hcl:.*"+test.want+".*")
			c.Assert(db, qt.IsNil)
			c.Assert(requests, qt.HasLen, 0)
		})
	}
}
