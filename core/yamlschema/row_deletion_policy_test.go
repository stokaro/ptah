package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/yamlschema"
)

// rowDeletionDocument is a table whose own keys are table.
func rowDeletionDocument(table string) string {
	return `
tables:
  events:
` + table + `
    columns:
      id:
        type: bigint
        primary: true
      created_at:
        type: timestamp
      expires:
        type: bigint unsigned
`
}

// TestParse_RowDeletionPolicy_HappyPath reads a table's row deletion policy
// under the keys the annotation reads it from.
func TestParse_RowDeletionPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  *ast.RowDeletionPolicySpec
	}{
		{name: "none", table: "    comment: no policy"},
		{
			name:  "a date column",
			table: "    row_deletion_column: created_at\n    row_deletion_interval: P30D",
			want:  &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P30D"},
		},
		{
			name:  "an integer column",
			table: "    row_deletion_column: expires\n    row_deletion_interval: PT1H\n    row_deletion_unit: nanoseconds",
			want:  &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT1H", Unit: "NANOSECONDS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(rowDeletionDocument(test.table)))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].RowDeletionPolicy, qt.DeepEquals, test.want)
		})
	}
}

func TestParse_RowDeletionPolicy_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		wantErr string
	}{
		{name: "no interval", table: "    row_deletion_column: created_at",
			wantErr: `table "events" declares row_deletion_column without row_deletion_interval: .*`},
		{name: "a unit YDB does not take",
			table:   "    row_deletion_column: expires\n    row_deletion_interval: PT1H\n    row_deletion_unit: days",
			wantErr: `table "events" declares row_deletion_unit: unit "days" is not one YDB takes: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(rowDeletionDocument(test.table)))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
