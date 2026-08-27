package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// An enum is a type, so its identity is (schema, name). Matching the two sides
// is asymmetric, because a blank schema means a different thing on each: the
// reader blanks the connection's own schema, while a generated enum that names
// none is UNQUALIFIED -- a Go annotation cannot name a schema at all.
//
// Both halves reached a live database before they were pinned here
// (stokaro/ptah#1276), and each row below names the run it came from.
func TestEnumsWithSemantics_SchemaIdentity(t *testing.T) {
	tests := []struct {
		name      string
		desired   []schemamodel.Enum
		database  []catalog.Enum
		semantics identifier.Semantics
		want      difftypes.SchemaDiff
	}{
		{
			// The `schema inspect` round trip. An enum block always writes its
			// schema, the pinned Atlas community binary v1.3.0 having no
			// reading for a block without one, so the document says `public`
			// where the read says nothing. Compared raw this planned a CREATE
			// and a DROP of the same type.
			name:      "an explicit default schema matches the reader's blank",
			desired:   []schemamodel.Enum{{Name: "p_color", Schema: "public", Values: []string{"red"}}},
			database:  []catalog.Enum{{Name: "p_color", Values: []string{"red"}}},
			semantics: identifier.Semantics{DefaultSchema: "public"},
			want:      difftypes.SchemaDiff{},
		},
		{
			// `ptah introspect --schemas <s>` writes annotations that carry no
			// schema, and the read they are compared against names one.
			name:      "a generated enum naming no schema matches a unique read",
			desired:   []schemamodel.Enum{{Name: "status_type", Values: []string{"a"}}},
			database:  []catalog.Enum{{Name: "status_type", Schema: "brownfield", Values: []string{"a"}}},
			semantics: identifier.Semantics{DefaultSchema: "public"},
			want:      difftypes.SchemaDiff{},
		},
		{
			// The control that keeps the row above from becoming "any name
			// matches anything". Two schemas holding one name is exactly where
			// a guess would attribute the type to the wrong schema.
			name:    "an ambiguous bare name matches nothing",
			desired: []schemamodel.Enum{{Name: "mood", Values: []string{"a"}}},
			database: []catalog.Enum{
				{Name: "mood", Schema: "one", Values: []string{"a"}},
				{Name: "mood", Schema: "two", Values: []string{"a"}},
			},
			semantics: identifier.Semantics{DefaultSchema: "public"},
			want: difftypes.SchemaDiff{
				EnumsAdded:   difftypes.EnumChanges{{Name: "mood", Values: []string{"a"}}},
				EnumsRemoved: difftypes.EnumChanges{{Name: "one.mood"}, {Name: "two.mood"}},
			},
		},
		{
			// Two schemas, one name, both sides qualified: the pairing is by
			// schema and the differing values are reported against the enum
			// that actually differs. Keyed on the bare name this reported
			// nothing at all.
			name: "same name in two schemas pairs by schema",
			desired: []schemamodel.Enum{
				{Name: "mood", Schema: "one", Values: []string{"a"}},
				{Name: "mood", Schema: "two", Values: []string{"a", "b"}},
			},
			database: []catalog.Enum{
				{Name: "mood", Schema: "one", Values: []string{"a"}},
				{Name: "mood", Schema: "two", Values: []string{"a"}},
			},
			semantics: identifier.Semantics{DefaultSchema: "public"},
			want: difftypes.SchemaDiff{
				EnumsModified: []difftypes.EnumDiff{
					{EnumName: "two.mood", ValuesAdded: []string{"b"}},
				},
			},
		},
		{
			// A qualified generated enum naming a schema the read does not
			// hold is added there, not silently matched against the enum of
			// the same name somewhere else.
			name:      "a qualified name does not fall back to another schema",
			desired:   []schemamodel.Enum{{Name: "mood", Schema: "wanted", Values: []string{"a"}}},
			database:  []catalog.Enum{{Name: "mood", Schema: "other", Values: []string{"a"}}},
			semantics: identifier.Semantics{DefaultSchema: "public"},
			want: difftypes.SchemaDiff{
				EnumsAdded:   difftypes.EnumChanges{{Name: "wanted.mood"}},
				EnumsRemoved: difftypes.EnumChanges{{Name: "other.mood"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.EnumsWithSemantics(
				&schemamodel.Database{Enums: test.desired},
				&catalog.Database{Enums: test.database},
				diff,
				test.semantics,
			)

			c.Assert(diff.EnumsAdded.Names(), qt.DeepEquals, test.want.EnumsAdded.Names())
			c.Assert(diff.EnumsRemoved.Names(), qt.DeepEquals, test.want.EnumsRemoved.Names())
			c.Assert(diff.EnumsModified, qt.HasLen, len(test.want.EnumsModified))
			for i, wantDiff := range test.want.EnumsModified {
				c.Assert(diff.EnumsModified[i].EnumName, qt.Equals, wantDiff.EnumName)
				c.Assert(diff.EnumsModified[i].ValuesAdded, qt.DeepEquals, wantDiff.ValuesAdded)
				c.Assert(diff.EnumsModified[i].ValuesRemoved, qt.DeepEquals, wantDiff.ValuesRemoved)
			}
		})
	}
}
