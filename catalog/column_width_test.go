package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// TestRawType_CarriesEveryWidthTheCatalogKeepsApart names the families whose
// width PostgreSQL keeps in `character_maximum_length` rather than in the type
// name, and requires each one to reach the caller.
//
// The bit rows are why this test exists. Without them `ptah schema inspect`
// wrote a `bit(4)` column as `bit`, and replaying that document into a fresh
// database produced `bit(1)` -- measured on PostgreSQL 17.11, three bits of
// every value gone. A `bit varying(8)` came back unlimited, and applying the
// document to the SOURCE database removed the declared width from the live
// column (stokaro/ptah#2034).
//
// The list is the assertion: a family added to the reader without a row here
// carries no width, and nothing else would say so.
func TestRawType_CarriesEveryWidthTheCatalogKeepsApart(t *testing.T) {
	width := 8
	tests := []struct {
		name     string
		column   catalog.Column
		wantType string
	}{
		{
			name:     "varchar, as information_schema spells it",
			column:   catalog.Column{DataType: "character varying", CharacterMaxLength: &width},
			wantType: "character varying(8)",
		},
		{
			name:     "char, as information_schema spells it",
			column:   catalog.Column{DataType: "character", CharacterMaxLength: &width},
			wantType: "character(8)",
		},
		{
			name:     "bit",
			column:   catalog.Column{DataType: "bit", CharacterMaxLength: &width},
			wantType: "bit(8)",
		},
		{
			name:     "bit varying",
			column:   catalog.Column{DataType: "bit varying", CharacterMaxLength: &width},
			wantType: "bit varying(8)",
		},
		{
			// The control on the whole rule: a type with no width in that
			// column must not grow one.
			name:     "a type that keeps no width there",
			column:   catalog.Column{DataType: "integer"},
			wantType: "integer",
		},
		{
			// The other control: a width already in the name is not doubled.
			name:     "a name that already carries its width",
			column:   catalog.Column{DataType: "bit", FormattedType: "bit(4)", CharacterMaxLength: &width},
			wantType: "bit(4)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.column.RawType(), qt.Equals, test.wantType)
		})
	}
}
