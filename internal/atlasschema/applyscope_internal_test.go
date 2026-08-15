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
// The load-bearing rows are the ones where base and the document disagree. base
// is what schemascope.ReadNames said this URL covers, and it is the same answer
// `schema diff`, `schema inspect` and `migrate diff` get; the document's own
// schemas are added on top rather than replacing it, because a URL pinned by
// search_path covers less than a document may name and a creation planned for
// an object that exists fails the run. A row that returned only the document's
// schemas would make a whole schema silently unmanaged, which is
// stokaro/ptah#1276 with the sign flipped.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestApplyReadScope(t *testing.T) {
	tests := []struct {
		name      string
		requested []string
		base      []string
		desired   *goschema.Database
		want      []string
	}{
		{
			name:      "explicit schemas outrank both the URL and the document",
			requested: []string{"only_this"},
			base:      []string{"extra", "public"},
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}, {Name: "extra"}},
			},
			want: []string{"only_this"},
		},
		{
			name:      "a comma-separated selection is split like the flag",
			requested: []string{"one,two"},
			base:      []string{"public"},
			desired:   &goschema.Database{},
			want:      []string{"one", "two"},
		},
		{
			name: "the URL's realm scope is read whether or not the document names it",
			base: []string{"extra", "public"},
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Tables:  []goschema.Table{{Name: "a", Schema: "public"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name: "a document qualifying nothing reads exactly the URL's scope",
			base: []string{"public"},
			desired: &goschema.Database{
				Tables: []goschema.Table{{Name: "a"}},
			},
			want: []string{"public"},
		},
		{
			name: "a schema block beyond a pinned URL widens the read",
			base: []string{"public"},
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "extra"}, {Name: "public"}},
				Tables:  []goschema.Table{{Name: "a", Schema: "public"}, {Name: "b", Schema: "extra"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name: "a qualified table alone widens the read",
			base: []string{"public"},
			desired: &goschema.Database{
				Tables: []goschema.Table{{Name: "b", Schema: "extra"}},
			},
			want: []string{"extra", "public"},
		},
		{
			name: "declarations other than tables name schemas too",
			base: []string{"public"},
			desired: &goschema.Database{
				Sequences:      []goschema.Sequence{{Name: "s", Schema: "seqs"}},
				Domains:        []goschema.Domain{{Name: "d", Schema: "doms"}},
				CompositeTypes: []goschema.CompositeType{{Name: "c", Schema: "comps"}},
				Ranges:         []goschema.Range{{Name: "r", Schema: "rngs"}},
			},
			want: []string{"comps", "doms", "public", "rngs", "seqs"},
		},
		{
			name: "blank names are not schemas",
			base: []string{"public"},
			desired: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "  "}},
				Tables:  []goschema.Table{{Name: "a", Schema: ""}},
			},
			want: []string{"public"},
		},
		{
			name:    "a connection naming no schema and a document naming none reads the reader's default",
			base:    nil,
			desired: nil,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(applyReadScope(tt.requested, tt.base, tt.desired), qt.DeepEquals, tt.want)
		})
	}
}
