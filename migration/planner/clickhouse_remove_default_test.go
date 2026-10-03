package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
)

// A ClickHouse column whose default goes away is planned with REMOVE DEFAULT.
// The planner records the default the column had and the renderer reads it,
// so the rows go through the public path that joins the two.
//
// A MODIFY COLUMN naming only a type keeps the default the column had, so a
// removal planned that way changed nothing while the migration reported
// success (stokaro/ptah#4030).
func TestGenerateSchemaDiffSQLStatements_ClickHouseRemovedDefault(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Field
		current schemamodel.Field
		want    []string
	}{
		{
			name:    "a literal default removed",
			desired: schemamodel.Field{Type: "INTEGER"},
			current: schemamodel.Field{Type: "INTEGER", Default: "0", DefaultSet: true},
			want:    []string{"ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT", "ALTER TABLE asn MODIFY COLUMN n Int32"},
		},
		{
			name:    "an expression default removed",
			desired: schemamodel.Field{Type: "INTEGER"},
			current: schemamodel.Field{Type: "INTEGER", DefaultExpr: "toInt32(40 + 2)"},
			want:    []string{"ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT", "ALTER TABLE asn MODIFY COLUMN n Int32"},
		},
		{
			name:    "a default removed as the type changes",
			desired: schemamodel.Field{Type: "BIGINT"},
			current: schemamodel.Field{Type: "INTEGER", Default: "5", DefaultSet: true},
			want:    []string{"ALTER TABLE asn MODIFY COLUMN n REMOVE DEFAULT", "ALTER TABLE asn MODIFY COLUMN n Int64"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				asnDiff(test.desired, test.current), platform.ClickHouse, planner.Options{Capabilities: capability.ClickHouse2411()},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
