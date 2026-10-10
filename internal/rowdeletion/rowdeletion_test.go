package rowdeletion_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/rowdeletion"
)

// both is the property set of an owner whose policy reads a unit, as YDB's
// does.
var both = []string{rowdeletion.ColumnProperty, rowdeletion.IntervalProperty, rowdeletion.UnitProperty}

// TestDecode_HappyPath reads a declaration from its properties: the values
// trimmed, the unit left as written for its owner to read.
func TestDecode_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		want       rowdeletion.Declaration
	}{
		{
			name:       "a date column",
			properties: map[string]string{"row_deletion_column": "created_at", "row_deletion_interval": "P30D"},
			want:       rowdeletion.Declaration{Column: "created_at", Interval: "P30D"},
		},
		{
			name:       "an integer column, trimmed",
			properties: map[string]string{"row_deletion_column": " expires ", "row_deletion_interval": " PT1H ", "row_deletion_unit": " seconds "},
			want:       rowdeletion.Declaration{Column: "expires", Interval: "PT1H", Unit: "seconds"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := rowdeletion.Decode(test.properties, both)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDecode_FailurePath refuses a property the owner does not take, naming
// the lower-case spelling where that is one it takes, a blank value, and a
// policy without its column or its interval.
func TestDecode_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		managed    []string
		wantErr    string
	}{
		{
			name:       "a misspelled name",
			properties: map[string]string{"row_deletion_colum": "ts", "row_deletion_interval": "P1D"},
			managed:    both,
			wantErr:    `invalid feature value: unknown row deletion property "row_deletion_colum": the policy takes row_deletion_column, row_deletion_interval, row_deletion_unit`,
		},
		{
			name:       "a name in capitals",
			properties: map[string]string{"ROW_DELETION_COLUMN": "ts", "row_deletion_interval": "P1D"},
			managed:    both,
			wantErr:    `invalid feature value: unknown row deletion property "ROW_DELETION_COLUMN": property names are lower case, as "row_deletion_column"`,
		},
		{
			name:       "a unit an owner does not take",
			properties: map[string]string{"row_deletion_column": "ts", "row_deletion_interval": "30 days", "row_deletion_unit": "SECONDS"},
			managed:    []string{rowdeletion.ColumnProperty, rowdeletion.IntervalProperty},
			wantErr:    `invalid feature value: unknown row deletion property "row_deletion_unit": the policy takes row_deletion_column, row_deletion_interval`,
		},
		{
			name:       "a blank value",
			properties: map[string]string{"row_deletion_column": " ", "row_deletion_interval": "P1D"},
			managed:    both,
			wantErr:    `invalid feature value: row_deletion_column is empty; remove it to leave the policy undeclared`,
		},
		{
			name:       "no column",
			properties: map[string]string{"row_deletion_interval": "P1D"},
			managed:    both,
			wantErr:    `invalid feature value: a row deletion policy needs row_deletion_column, the column its interval is measured from`,
		},
		{
			name:       "no interval",
			properties: map[string]string{"row_deletion_column": "ts", "row_deletion_unit": "SECONDS"},
			managed:    both,
			wantErr:    `invalid feature value: a row deletion policy needs row_deletion_interval, the interval after which a row is deleted`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := rowdeletion.Decode(test.properties, test.managed)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, rowdeletion.Declaration{})
		})
	}
}

// TestEncode writes the column and the interval, and the unit only when one is
// set, so a date column's policy carries no unit property.
func TestEncode(t *testing.T) {
	tests := []struct {
		name        string
		declaration rowdeletion.Declaration
		want        map[string]string
	}{
		{
			name:        "a date column",
			declaration: rowdeletion.Declaration{Column: "created_at", Interval: "30 days"},
			want:        map[string]string{"row_deletion_column": "created_at", "row_deletion_interval": "30 days"},
		},
		{
			name:        "an integer column",
			declaration: rowdeletion.Declaration{Column: "expires", Interval: "PT1H", Unit: "SECONDS"},
			want:        map[string]string{"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "SECONDS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rowdeletion.Encode(test.declaration), qt.DeepEquals, test.want)
		})
	}
}
