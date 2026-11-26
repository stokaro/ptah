package chrender_test

import (
	"regexp"
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

// A filter that names a column called not quoted, in any case or quote, is
// refused before any statement is written, on a creation, a change and a
// command read from SQL: 24.10 and 26.10 refuse the statement with code 62
// after the statements before it in the plan have run.
func TestRenderRowPolicy_AFilterThatNamesNotIsRefused(t *testing.T) {
	observed := &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive}
	for _, test := range []struct {
		name    string
		op      ast.ExtensionPayload
		written string
	}{
		{name: "a creation", written: "`not`", op: &chast.RowPolicy{Table: "orders", Name: "tenant",
			Change: *chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{Filter: new("`not` > 0")})}},
		{name: "a change", written: `"NOT"`, op: &chast.RowPolicy{Table: "orders", Name: "tenant",
			Change: *chdiff.NewRowPolicy(observed, &chschema.DesiredRowPolicy{Filter: new(`"NOT" = 1`)})}},
		{name: "a command", written: "`Not`", op: &chast.RowPolicyCommand{Action: "CREATE", Table: "orders", Name: "tenant",
			Values: &chschema.DesiredRowPolicy{Filter: new("abs(`Not`) > 0")}, FilterSet: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := must.Must(chrender.Registry()).Render(renderer.ExtensionContext{Target: "clickhouse", Capabilities: rowPolicyCaps()}, ast.StatementExtension, test.op)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`unsupported feature: ClickHouse: the filter of row policy "tenant" on table "orders" names `+
				test.written+`; releases before 26.11 store that name as the bare word not, the NOT operator`)+`.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A filter that names not as the operator, as a string or as a function is
// written as declared.
func TestRenderRowPolicy_AFilterWithoutAQuotedNotRenders(t *testing.T) {
	c := qt.New(t)
	op := &chast.RowPolicy{Table: "orders", Name: "tenant",
		Change: *chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{Filter: new("NOT (id < 0) AND s != 'not' AND `not`(0) = 1")})}

	statements, err := renderRowPolicy(c, "clickhouse", rowPolicyCaps(), op)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"CREATE ROW POLICY `tenant` ON `orders` USING (NOT (id < 0) AND s != 'not' AND `not`(0) = 1) AS PERMISSIVE;"})
}
