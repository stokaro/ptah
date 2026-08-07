package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderIndexPartNullsOrderRoundTrip closes the loop issue #1272 opened.
//
// The comparator now treats a key's NULLS ordering as part of the index
// definition, so a rendering that drops it turns a faithful `schema inspect`
// into a permanent rebuild: `schema diff --from <db> --to <its own inspect
// output>` would plan DROP INDEX + CREATE INDEX forever. Measured on
// PostgreSQL 17.10 before the renderer learned these two attributes, that is
// exactly what the fixture below did.
func TestRenderIndexPartNullsOrderRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "a", Type: "text"},
			{StructName: "T", Name: "b", Type: "text"},
		},
		Indexes: []goschema.Index{
			{
				StructName: "T",
				Name:       "i",
				Fields:     []string{"a", "b"},
				Parts: []goschema.IndexPart{
					{Name: "a", Desc: true, NullsOrder: goschema.NullsOrderLast},
					{Name: "b", NullsOrder: goschema.NullsOrderFirst},
				},
			},
		},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, "nulls_last = true")
	c.Assert(hcl, qt.Contains, "nulls_first = true")

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Indexes, qt.HasLen, 1)
	c.Assert(parsed.Indexes[0].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Name: "a", Desc: true, NullsOrder: goschema.NullsOrderLast},
		{Name: "b", NullsOrder: goschema.NullsOrderFirst},
	})
}

// TestRenderIndexPartNullsOrderKeepsOnBlocks guards the other half of the same
// loss. simpleIndexParts decides between the compact `columns = [...]` spelling
// and one `on` block per key; a part carrying only a NULLS ordering used to
// count as simple, so the ordering disappeared even though renderIndex knew how
// to write it.
func TestRenderIndexPartNullsOrderKeepsOnBlocks(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{{StructName: "T", Name: "a", Type: "text"}},
		Indexes: []goschema.Index{
			{
				StructName: "T",
				Name:       "i",
				Fields:     []string{"a"},
				Parts: []goschema.IndexPart{
					{Name: "a", NullsOrder: goschema.NullsOrderFirst},
				},
			},
		},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, "on {")
	c.Assert(hcl, qt.Contains, "nulls_first = true")

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Indexes[0].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Name: "a", NullsOrder: goschema.NullsOrderFirst},
	})
}
