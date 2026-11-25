package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

// An enum value is data, and PostgreSQL holds ' sad' and 'sad' as two values,
// so an item of a sequence keeps its bytes. The one-line form is a list an
// author typed: the space after a comma is layout, and its parts are trimmed.
func TestParse_EnumValuesKeepTheirBytes(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     []string
	}{
		{name: "a sequence", document: "enums:\n  mood: [happy, ' sad', 'ok ']\n", want: []string{"happy", " sad", "ok "}},
		{name: "a sequence with a comment", document: "enums:\n  mood:\n    values: [happy, ' sad']\n    comment: how it went\n", want: []string{"happy", " sad"}},
		{name: "a block sequence", document: "enums:\n  mood:\n    values:\n      - happy\n      - ' sad'\n", want: []string{"happy", " sad"}},
		{name: "a sequence with an empty item", document: "enums:\n  mood: [happy, '', ' sad']\n", want: []string{"happy", " sad"}},
		{name: "a white-space value", document: "enums:\n  mood: [happy, ' ']\n", want: []string{"happy", " "}},
		{name: "the one-line form", document: "enums:\n  mood: 'happy, sad ,ok'\n", want: []string{"happy", "sad", "ok"}},
		{name: "the one-line form with an empty part", document: "enums:\n  mood:\n    values: 'happy,, sad'\n", want: []string{"happy", "sad"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), noOwners, []byte(tc.document))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Enums, qt.HasLen, 1)
			c.Assert(db.Enums[0].Values, qt.DeepEquals, tc.want)
		})
	}
}

// A column's enum is read by the same rule, and the enum it generates holds
// the values as written.
func TestParse_ColumnEnumValuesKeepTheirBytes(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     []string
	}{
		{name: "a sequence", document: "tables:\n  c:\n    columns:\n      m:\n        enum: [happy, ' sad', 'ok ']\n", want: []string{"happy", " sad", "ok "}},
		{name: "the one-line form", document: "tables:\n  c:\n    columns:\n      m:\n        enum: 'happy, sad'\n", want: []string{"happy", "sad"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(t.Context(), noOwners, []byte(tc.document))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Enums, qt.HasLen, 1)
			c.Assert(db.Enums[0].Name, qt.Equals, "enum_c_m")
			c.Assert(db.Enums[0].Values, qt.DeepEquals, tc.want)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Type, qt.Equals, "enum_c_m")
		})
	}
}
