package objectidentity_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
)

// TestID_JSON pins the identity's JSON: snake_case keys, as every document
// Ptah writes uses, and no key for an empty component, part field or
// signature, so a table reads as its schema and name. Each row decodes back to
// the identity it was encoded from.
func TestID_JSON(t *testing.T) {
	tests := []struct {
		name string
		id   objectidentity.ID
		want string
	}{
		{name: "a table", want: `{"kind":"table","schema":{"source":"app","normalized":"app"},"name":{"source":"events","normalized":"events"}}`,
			id: objectidentity.ID{Kind: objectidentity.KindTable, Schema: objectidentity.Part{Source: "app", Normalized: "app"},
				Name: objectidentity.Part{Source: "events", Normalized: "events"}}},
		{name: "a defaulted schema and a quoted name",
			want: `{"kind":"table","schema":{"source":"public","normalized":"public","defaulted":true},` +
				`"name":{"source":"\"Events\"","normalized":"\"Events\"","quoted":true}}`,
			id: objectidentity.ID{Kind: objectidentity.KindTable, Schema: objectidentity.Part{Source: "public", Normalized: "public", Defaulted: true},
				Name: objectidentity.Part{Source: `"Events"`, Normalized: `"Events"`, Quoted: true}}},
		{name: "a parent, a catalog and a signature",
			want: `{"kind":"function","catalog":{"source":"db","normalized":"db"},"parent":{"source":"t","normalized":"t"},` +
				`"name":{"source":"f","normalized":"f"},"signature":"integer"}`,
			id: objectidentity.ID{Kind: objectidentity.KindFunction, Catalog: objectidentity.Part{Source: "db", Normalized: "db"},
				Parent: objectidentity.Part{Source: "t", Normalized: "t"}, Name: objectidentity.Part{Source: "f", Normalized: "f"},
				Signature: "integer"}},
		{name: "the zero identity", want: `{}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.id)
			c.Assert(err, qt.IsNil)
			var decoded objectidentity.ID
			c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)

			c.Assert(string(encoded), qt.Equals, test.want)
			c.Assert(decoded, qt.DeepEquals, test.id)
		})
	}
}
