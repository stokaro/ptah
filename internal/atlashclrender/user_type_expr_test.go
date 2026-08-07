package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderWritesUserTypeBasesAsSQLCalls pins how the type a user-defined type
// is built on reaches HCL (stokaro/ptah#1260 item 2).
//
// A domain's base type, a composite field's type and a range's subtype are not
// column type positions and do not follow the column rules. Measured against the
// pinned Atlas community binary v1.3.0, one attribute varied at a time in a
// document it otherwise accepts:
//
//	domain    type = text          refused, There is no variable named "text"
//	domain    type = "text"        refused, schemahcl: failed reading spec
//	domain    type = sql("text")   accepted
//	composite type = text          refused
//	composite type = "text"        refused
//	composite type = sql("text")   accepted
//	range     subtype = int4       refused
//	range     subtype = "int4"     ACCEPTED
//	range     subtype = sql("int4") accepted
//
// The range row is why this was measured rather than inferred from the other
// two: it takes the quoted string they refuse. All three take sql(), so one rule
// covers them, which is what the last three rows below pin.
func TestRenderWritesUserTypeBasesAsSQLCalls(t *testing.T) {
	tests := []struct {
		name string
		db   *goschema.Database
		want string
	}{
		{
			name: "a domain's base type",
			db:   &goschema.Database{Domains: []goschema.Domain{{Name: "d", BaseType: "text"}}},
			want: `  type = sql("text")` + "\n",
		},
		{
			name: "a composite field's type",
			db: &goschema.Database{CompositeTypes: []goschema.CompositeType{{
				Name:   "c",
				Fields: []goschema.CompositeTypeField{{Name: "f", Type: "text"}},
			}}},
			want: `    type = sql("text")` + "\n",
		},
		{
			name: "a range's subtype",
			db:   &goschema.Database{Ranges: []goschema.Range{{Name: "r", Subtype: "int4"}}},
			want: `  subtype = sql("int4")` + "\n",
		},
		{
			// A type carrying a size or a space would be a syntax error bare and
			// a refused string quoted; the call takes it verbatim.
			name: "a sized type survives verbatim",
			db:   &goschema.Database{Domains: []goschema.Domain{{Name: "d", BaseType: "character varying(100)"}}},
			want: `  type = sql("character varying(100)")` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.Render(test.db)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
		})
	}
}

// TestRenderedUserTypeBasesRoundTrip pins that the call costs Ptah nothing on
// its own side: the parser reads each position back to the bare type name.
//
// Without this the change would be measured only against the other binary, and
// a rendering Ptah itself could no longer read would still look like progress.
func TestRenderedUserTypeBasesRoundTrip(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Schemas:        []goschema.Schema{{Name: "public"}},
		Domains:        []goschema.Domain{{Name: "d", Schema: "public", BaseType: "text"}},
		CompositeTypes: []goschema.CompositeType{{Name: "c", Schema: "public", Fields: []goschema.CompositeTypeField{{Name: "f", Type: "text"}}}},
		Ranges:         []goschema.Range{{Name: "r", Schema: "public", Subtype: "int4"}},
	}

	result, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)

	parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Domains, qt.HasLen, 1)
	c.Assert(parsed.Domains[0].BaseType, qt.Equals, "text")
	c.Assert(parsed.CompositeTypes, qt.HasLen, 1)
	c.Assert(parsed.CompositeTypes[0].Fields, qt.HasLen, 1)
	c.Assert(parsed.CompositeTypes[0].Fields[0].Type, qt.Equals, "text")
	c.Assert(parsed.Ranges, qt.HasLen, 1)
	c.Assert(parsed.Ranges[0].Subtype, qt.Equals, "int4")
}

// TestRenderKeepsAnEmptyUserTypeBaseUnwrapped pins the one value that must not
// become a call: sql("") would round trip as a type named nothing, where the
// existing fallback renders something readable.
func TestRenderKeepsAnEmptyUserTypeBaseUnwrapped(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.Render(&goschema.Database{
		Domains: []goschema.Domain{{Name: "d", BaseType: ""}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Not(qt.Contains), `sql("")`)
}
