package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// TestRender_RowDeletionPolicyOnAnIntegerColumn_FailurePath refuses a policy
// that names the unit an integer column counts in. Spanner's clause has no
// unit, so written without it the clause would read the column as a timestamp,
// which is not what was declared.
func TestRender_RowDeletionPolicyOnAnIntegerColumn_FailurePath(t *testing.T) {
	policy := &ast.RowDeletionPolicySpec{Column: "expires", Interval: "30 days", Unit: "SECONDS"}
	tests := []struct {
		name string
		node ast.Node
	}{
		{
			name: "at CREATE TABLE",
			node: &ast.CreateTableNode{
				Name: "sessions",
				Columns: []*ast.ColumnNode{
					{Name: "id", Type: "BIGINT", Primary: true},
					{Name: "expires", Type: "BIGINT", Nullable: true},
				},
				RowDeletionPolicy: policy,
			},
		},
		{
			name: "on a table that exists",
			node: &ast.AlterTableNode{Name: "sessions", Operations: []ast.AlterOperation{
				&ast.SetRowDeletionPolicyOperation{Column: policy.Column, Interval: policy.Interval, Unit: policy.Unit},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
			sql, err := renderer.Render(test.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `unsupported feature: spanner: table "sessions" declares a row deletion policy on an `+
				`integer column counting SECONDS, which requires target capability row_deletion_policy_epoch_column; .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
