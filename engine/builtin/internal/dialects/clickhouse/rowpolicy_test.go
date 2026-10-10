package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
)

// TestClickHouseRenderer_RefusesSharedRowSecurityNodes refuses the shared
// row-level security nodes: a ClickHouse row policy is the ClickHouse owner's
// object, rendered by its own operation, and ClickHouse has no table switch,
// so a shared node reaching this renderer was built by hand.
func TestClickHouseRenderer_RefusesSharedRowSecurityNodes(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{"create policy", ast.NewCreatePolicy("p", "t")},
		{"drop policy", ast.NewDropPolicy("p", "t")},
		{"enable rls", &ast.AlterTableEnableRLSNode{Table: "t"}},
		{"disable rls", &ast.AlterTableDisableRLSNode{Table: "t"}},
		{"force rls", &ast.AlterTableForceRLSNode{Table: "t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := renderErr(test.node)
			c.Assert(err, qt.ErrorMatches, `unsupported feature: clickhouse: \*ast\.\w+ has no handler in this renderer`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}
