package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestDefaultPoolSettingsPreservesUnspecifiedLimits(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec ydbworkload.PoolSpec
		want []string
	}{
		{"empty", ydbworkload.PoolSpec{}, nil},
		{"zero", ydbworkload.PoolSpec{ResourceWeight: new(0.0)}, []string{"ALTER RESOURCE POOL `default` SET (RESOURCE_WEIGHT = 0);"}},
		{"fractional", ydbworkload.PoolSpec{QueryMemoryLimitPercentPerNode: new(12.5), QueryCPULimitPercentPerNode: new(20.0), TotalCPULimitPercentPerNode: new(30.25)},
			[]string{"ALTER RESOURCE POOL `default` SET (QUERY_MEMORY_LIMIT_PERCENT_PER_NODE = '12.5', QUERY_CPU_LIMIT_PERCENT_PER_NODE = 20, TOTAL_CPU_LIMIT_PERCENT_PER_NODE = '30.25');"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := renderer.NewExtensions(ydbrender.DefaultPoolSettingsHandler())
			c.Assert(err, qt.IsNil)
			ctx := renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262().With(capability.ResourcePools, true)}
			result, err := registry.Render(ctx, ast.StatementExtension, &ydbast.DefaultPoolSettings{Spec: tc.spec})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, tc.want)
			ctx.Capabilities = nil
			result, err = registry.Render(ctx, ast.StatementExtension, &ydbast.DefaultPoolSettings{Spec: tc.spec})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result, qt.IsNil)
			ctx.Target = "postgres"
			result, err = registry.Render(ctx, ast.StatementExtension, &ydbast.DefaultPoolSettings{Spec: tc.spec})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(result, qt.IsNil)
		})
	}
}
