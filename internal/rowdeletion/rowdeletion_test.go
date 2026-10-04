package rowdeletion_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/rowdeletion"
)

func TestParseDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   *ast.RowDeletionPolicySpec
	}{
		{name: "no policy", values: map[string]string{"name": "events"}},
		{
			name:   "a date column",
			values: map[string]string{"row_deletion_column": " created_at ", "row_deletion_interval": "P30D"},
			want:   &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P30D"},
		},
		{
			name:   "Spanner's spelling, which the target judges",
			values: map[string]string{"row_deletion_column": "created_at", "row_deletion_interval": "30 days"},
			want:   &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30 days"},
		},
		{
			name: "an integer column, its unit in capitals",
			values: map[string]string{
				"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "milliseconds",
			},
			want: &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT1H", Unit: "MILLISECONDS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := rowdeletion.ParseDeclaration("events", test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParseDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{
			name:    "an interval without a column",
			values:  map[string]string{"row_deletion_interval": "P30D"},
			wantErr: `table "events" declares row_deletion_interval without row_deletion_column: .*`,
		},
		{
			name:    "a column without an interval",
			values:  map[string]string{"row_deletion_column": "created_at"},
			wantErr: `table "events" declares row_deletion_column without row_deletion_interval: .*`,
		},
		{
			name:    "a unit alone",
			values:  map[string]string{"row_deletion_unit": "SECONDS"},
			wantErr: `table "events" declares row_deletion_unit without row_deletion_column: .*`,
		},
		{
			name: "a unit YDB does not take",
			values: map[string]string{
				"row_deletion_column": "expires", "row_deletion_interval": "PT1H", "row_deletion_unit": "minutes",
			},
			wantErr: `table "events" declares row_deletion_unit: unit "minutes" is not one YDB takes: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := rowdeletion.ParseDeclaration("events", test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// Equal reads each interval in its engine's spelling: YDB's as the seconds it
// keeps, Spanner's as the days the server rewrites it into.
func TestEqual(t *testing.T) {
	exact := func(name string) string { return name }
	tests := []struct {
		name string
		a, b *ast.RowDeletionPolicySpec
		want bool
	}{
		{
			name: "YDB: hours and days",
			a:    &ast.RowDeletionPolicySpec{Column: "ts", Interval: "PT720H"},
			b:    &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
			want: true,
		},
		{
			name: "Spanner: days and weeks",
			a:    &ast.RowDeletionPolicySpec{Column: "ts", Interval: "30 days"},
			b:    &ast.RowDeletionPolicySpec{Column: "ts", Interval: "4 WEEKS 2 DAYS"},
			want: true,
		},
		{
			name: "YDB: another unit",
			a:    &ast.RowDeletionPolicySpec{Column: "e", Interval: "PT1H", Unit: "SECONDS"},
			b:    &ast.RowDeletionPolicySpec{Column: "e", Interval: "PT1H"},
		},
		{
			name: "Spanner: another interval",
			a:    &ast.RowDeletionPolicySpec{Column: "ts", Interval: "30 days"},
			b:    &ast.RowDeletionPolicySpec{Column: "ts", Interval: "31 days"},
		},
		{name: "no policy", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rowdeletion.Equal(test.a, test.b, exact), qt.Equals, test.want)
		})
	}
}
