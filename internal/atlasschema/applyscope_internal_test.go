package atlasschema

// White-box testing required: applyReadScope is the step that decides which
// schemas the DATABASE side of a `schema apply` is read at, and it runs between
// two package-private stages -- loading the desired state and reading the
// catalog -- that no exported entry point exposes separately. Driving the
// decision through PrepareApply would need a live multi-schema server for every
// row, and the property under test is which names go to the reader, not what
// the reader answers. The end-to-end property -- inspecting a two-schema
// database and applying its own description back plans nothing -- is covered
// live by TestPostgreSQLSchemaBoundaryGuardIntegration's `two_schemas_plain_url`
// fixture.
//
// The nil rows are the load-bearing ones: nil means the reader keeps the scope
// it had before stokaro/ptah#1264, so a document that names nothing beyond the
// connected schema reads exactly what it read before. A row returning
// []string{"public"} instead would be indistinguishable in the plan and would
// still have widened what every apply asks the catalog for.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestApplyReadScope(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		requested []string
		connected string
		desired   *goschema.Database
		want      []string
	}{
		{
			name:      "explicit schemas outrank the document",
			requested: []string{"only_this"},
			connected: "public",
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}, {Name: "extra"}},
			},
			want: []string{"only_this"},
		},
		{
			name:      "a comma-separated selection is split like the flag",
			requested: []string{"one,two"},
			connected: "public",
			desired:   &goschema.Database{},
			want:      []string{"one", "two"},
		},
		{
			name:      "a document naming only the connected schema keeps the default read",
			connected: "public",
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Tables:  []goschema.Table{{Name: "a", Schema: "public"}},
			},
			want: nil,
		},
		{
			name:      "a document qualifying nothing keeps the default read",
			connected: "public",
			desired: &goschema.Database{
				Tables: []goschema.Table{{Name: "a"}},
			},
			want: nil,
		},
		{
			name:      "a schema block beyond the connected one widens the read",
			connected: "public",
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "extra"}, {Name: "public"}},
				Tables:  []goschema.Table{{Name: "a", Schema: "public"}, {Name: "b", Schema: "extra"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name:      "a qualified table alone widens the read",
			connected: "public",
			desired: &goschema.Database{
				Tables: []goschema.Table{{Name: "b", Schema: "extra"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name:      "declarations other than tables name schemas too",
			connected: "public",
			desired: &goschema.Database{
				Sequences:      []goschema.Sequence{{Name: "s", Schema: "seqs"}},
				Domains:        []goschema.Domain{{Name: "d", Schema: "doms"}},
				CompositeTypes: []goschema.CompositeType{{Name: "c", Schema: "comps"}},
				Ranges:         []goschema.Range{{Name: "r", Schema: "rngs"}},
			},
			want: []string{"comps", "doms", "public", "rngs", "seqs"},
		},
		{
			name:      "blank names are not schemas",
			connected: "public",
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "  "}},
				Tables:  []goschema.Table{{Name: "a", Schema: ""}},
			},
			want: nil,
		},
		{
			name:      "no desired state reads what it read before",
			connected: "public",
			desired:   nil,
			want:      nil,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(applyReadScope(tt.requested, tt.connected, tt.desired), qt.DeepEquals, tt.want)
		})
	}
}
