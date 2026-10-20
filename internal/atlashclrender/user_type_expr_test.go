package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
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
		db   *schemamodel.Database
		want string
	}{
		{
			name: "a domain's base type",
			db:   typeDatabase(schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d"), Value: &pgtype.DesiredDomain{BaseType: "text"}}),
			want: `  type = sql("text")` + "\n",
		},
		{
			name: "a composite field's type",
			db: typeDatabase(schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "c"),
				Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "f", Type: "text"}}}}),
			want: `    type = sql("text")` + "\n",
		},
		{
			name: "a range's subtype",
			db:   typeDatabase(schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "r"), Value: &pgtype.DesiredRange{Subtype: "int4"}}),
			want: `  subtype = sql("int4")` + "\n",
		},
		{
			// A type carrying a size or a space would be a syntax error bare and
			// a refused string quoted; the call takes it verbatim.
			name: "a sized type survives verbatim",
			db:   typeDatabase(schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d"), Value: &pgtype.DesiredDomain{BaseType: "character varying(100)"}}),
			want: `  type = sql("character varying(100)")` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.Render(test.db, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})

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

	db := typeDatabase(
		schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "public", "d"), Value: &pgtype.DesiredDomain{BaseType: "text"}},
		schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "public", "c"),
			Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "f", Type: "text"}}}},
		schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "public", "r"), Value: &pgtype.DesiredRange{Subtype: "int4"}},
	)
	db.Schemas = []schemamodel.Schema{{Name: "public"}}

	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)

	parsed, err := atlashcl.ParseWithOptions(t.Context(), result.Data, "rendered.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil)
	domain, found, err := parsed.FeatureObjects.Get(pgtype.Ref(pgtype.DomainKind, "public", "d"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(domain.Value.(*pgtype.DesiredDomain).BaseType, qt.Equals, "text")
	composite, found, err := parsed.FeatureObjects.Get(pgtype.Ref(pgtype.CompositeKind, "public", "c"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(composite.Value.(*pgtype.DesiredComposite).Attributes, qt.DeepEquals, []pgtype.Attribute{{Name: "f", Type: "text"}})
	rangeType, found, err := parsed.FeatureObjects.Get(pgtype.Ref(pgtype.RangeKind, "public", "r"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(rangeType.Value.(*pgtype.DesiredRange).Subtype, qt.Equals, "int4")
}

// typeDatabase is a description that declares only the type objects given.
func typeDatabase(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...))}
}
