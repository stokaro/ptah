package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_RowDeletionPolicy writes a table's row deletion policy as the
// attributes the annotation parser reads it back from, so a YDB table read
// from a database and written as Go keeps its TTL.
func TestRender_RowDeletionPolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy *ast.RowDeletionPolicySpec
		want   string
	}{
		{
			name:   "a date column",
			policy: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P30D"},
			want:   `row_deletion_column="created_at" row_deletion_interval="P30D"`,
		},
		{
			name:   "an integer column",
			policy: &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT1H", Unit: "SECONDS"},
			want:   `row_deletion_column="expires" row_deletion_interval="PT1H" row_deletion_unit="SECONDS"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Event", Name: "events", PrimaryKey: []string{"id"}, RowDeletionPolicy: test.policy}},
				Fields: []schemamodel.Field{
					{StructName: "Event", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
					{StructName: "Event", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP"},
					{StructName: "Event", FieldName: "Expires", Name: "expires", Type: "BIGINT UNSIGNED"},
				},
			}

			files, err := goschematogo.Render(db, goschematogo.Options{SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, test.want)
			c.Assert(reparsed.Tables, qt.HasLen, 1)
			c.Assert(reparsed.Tables[0].RowDeletionPolicy, qt.DeepEquals, test.policy)
		})
	}
}
