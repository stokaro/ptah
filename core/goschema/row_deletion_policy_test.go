package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// rowDeletionSource is an entity whose table directive carries attributes.
func rowDeletionSource(attributes string) string {
	return `package entities

//ptah:schema:table name="events" ` + attributes + `
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="created_at" type="TIMESTAMP"
	CreatedAt string
	//ptah:schema:field name="expires" type="BIGINT UNSIGNED"
	Expires uint64
}
`
}

// TestParseSource_RowDeletionPolicy_HappyPath reads a table's row deletion
// policy from its table directive: the column, the interval as written, and
// an integer column's unit in capitals.
func TestParseSource_RowDeletionPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       *ast.RowDeletionPolicySpec
	}{
		{name: "none", want: nil},
		{
			name:       "a date column",
			attributes: `row_deletion_column="created_at" row_deletion_interval="P30D"`,
			want:       &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P30D"},
		},
		{
			name:       "an integer column",
			attributes: `row_deletion_column="expires" row_deletion_interval="PT1H" row_deletion_unit="seconds"`,
			want:       &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT1H", Unit: "SECONDS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("events.go", rowDeletionSource(test.attributes))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].RowDeletionPolicy, qt.DeepEquals, test.want)
		})
	}
}

// TestParseSource_RowDeletionPolicy_FailurePath refuses, where it was written,
// a policy missing its column or its interval and a unit YDB does not take.
func TestParseSource_RowDeletionPolicy_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantErr    string
	}{
		{name: "no column", attributes: `row_deletion_interval="P30D"`,
			wantErr: `.*table "events" declares row_deletion_interval without row_deletion_column: .*`},
		{name: "no interval", attributes: `row_deletion_column="created_at"`,
			wantErr: `.*table "events" declares row_deletion_column without row_deletion_interval: .*`},
		{name: "a unit YDB does not take", attributes: `row_deletion_column="expires" row_deletion_interval="PT1H" row_deletion_unit="ms"`,
			wantErr: `.*table "events" declares row_deletion_unit: unit "ms" is not one YDB takes: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("events.go", rowDeletionSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
