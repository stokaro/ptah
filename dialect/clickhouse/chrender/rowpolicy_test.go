package chrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/clickhouse/chschema"
)

func renderRowPolicy(c *qt.C, target string, caps capability.Capabilities, op *chast.RowPolicy) ([]string, error) {
	c.Helper()
	return must.Must(chrender.Registry()).Render(renderer.ExtensionContext{Target: target, Capabilities: caps}, ast.StatementExtension, op)
}

func rowPolicyCaps() capability.Capabilities {
	return capability.Capabilities{}.With(capability.RowLevelSecurity, true)
}

// Each transition is one statement that states every part: the composition,
// ClickHouse's default included, the filter in parentheses, and on ALTER the
// role clause and USING NONE for a policy without a filter.
func TestRenderRowPolicy(t *testing.T) {
	observed := &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	for _, test := range []struct {
		name string
		op   *chast.RowPolicy
		want string
	}{
		{"a creation with every part", &chast.RowPolicy{Database: "app", Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(nil,
			&chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{Names: []string{"alice", "my role"}}})},
			"CREATE ROW POLICY `tenant` ON `app`.`orders` USING (tenant = 1) AS RESTRICTIVE TO `alice`, `my role`;"},
		{"a creation that applies to nobody", &chast.RowPolicy{Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{})},
			"CREATE ROW POLICY `tenant` ON `orders` AS PERMISSIVE;"},
		{"a change to every user but one", &chast.RowPolicy{Database: "app", Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(observed,
			&chschema.DesiredRowPolicy{Filter: new("tenant = 2"), Roles: chschema.RoleSelection{All: true, Except: []string{"b`t"}}})},
			"ALTER ROW POLICY `tenant` ON `app`.`orders` USING (tenant = 2) AS PERMISSIVE TO ALL EXCEPT `b``t`;"},
		{"a change that removes the filter and the users", &chast.RowPolicy{Database: "app", Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(observed,
			&chschema.DesiredRowPolicy{Composition: chschema.Restrictive})},
			"ALTER ROW POLICY `tenant` ON `app`.`orders` USING NONE AS RESTRICTIVE TO NONE;"},
		{"a drop", &chast.RowPolicy{Database: "app", Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(observed, nil)},
			"DROP ROW POLICY `tenant` ON `app`.`orders`;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderRowPolicy(c, "clickhouse", rowPolicyCaps(), test.op)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// A target without row policies, another dialect and an invalid operation
// are refused before any statement is written.
func TestRenderRowPolicy_FailurePath(t *testing.T) {
	valid := &chast.RowPolicy{Table: "orders", Name: "tenant", Change: *chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{})}
	for _, test := range []struct {
		name   string
		target string
		caps   capability.Capabilities
		op     *chast.RowPolicy
		want   error
	}{
		{"a target without row policies", "clickhouse", capability.Capabilities{}, valid, ptaherr.ErrUnsupportedFeature},
		{"another dialect", "postgres", rowPolicyCaps(), valid, ptaherr.ErrUnsupportedDialect},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderRowPolicy(c, test.target, test.caps, test.op)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}
