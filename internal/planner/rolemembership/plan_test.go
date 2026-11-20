package rolemembership_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// planMemberships compares a declaration with a live role graph, plans the
// difference through the production path and renders it for the target, so the
// assertions below read the statements a server would receive.
func planMemberships(
	t *testing.T,
	dialect string,
	caps capability.Capabilities,
	roles []schemamodel.Role,
	live []catalog.RoleMembership,
) ([]string, error) {
	t.Helper()
	sql, err := renderMembershipPlan(t, dialect, caps, roles, live)
	if err != nil {
		return nil, err
	}
	return splitStatements(sql), nil
}

// renderMembershipPlan is the plan as rendered text, comments included.
func renderMembershipPlan(
	t *testing.T,
	dialect string,
	caps capability.Capabilities,
	roles []schemamodel.Role,
	live []catalog.RoleMembership,
) (string, error) {
	t.Helper()
	runtime := must.Must(builtin.New())
	liveRoles := make([]catalog.Role, 0, len(roles))
	for _, role := range roles {
		liveRoles = append(liveRoles, catalog.Role{Name: role.Name, Inherit: role.Inherit, PasswordState: catalog.RolePasswordAbsent})
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), &schemamodel.Database{Roles: roles},
		&catalog.Database{Roles: liveRoles, RoleMemberships: live},
		catalog.ServerInfo{Dialect: dialect, Schema: "public", Capabilities: caps},
		nil, runtime,
	)
	if err != nil {
		return "", err
	}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(t.Context(), runtime, diff, dialect, planext.Options{Capabilities: caps})
	if err != nil {
		return "", err
	}
	return builtin.RenderSQLWithCapabilities(dialect, caps, nodes...)
}

func splitStatements(sql string) []string {
	return slices.DeleteFunc(strings.Split(sql, "\n"), func(line string) bool {
		return line == "" || strings.HasPrefix(line, "-")
	})
}

func plainRoles() []schemamodel.Role {
	return []schemamodel.Role{
		{Name: "readers", Inherit: true},
		{Name: "app", Inherit: true, MemberOf: []string{"readers"}},
	}
}

func rolesWith(options schemamodel.RoleMembershipOptions) []schemamodel.Role {
	roles := plainRoles()
	roles[1].MemberOptions = map[string]schemamodel.RoleMembershipOptions{"readers": options}
	return roles
}

func edge(admin bool, inherit, set *bool) []catalog.RoleMembership {
	return []catalog.RoleMembership{{Role: "readers", Member: "app", AdminOption: admin, InheritOption: inherit, SetOption: set}}
}

func TestPlan_PostgreSQL16_HappyPath(t *testing.T) {
	t.Run("a declared membership is granted with no clause", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), plainRoles(), nil)
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`GRANT "readers" TO "app";`})
	})
	t.Run("every stated option is written", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(false), Set: new(false)})
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, nil)
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`GRANT "readers" TO "app" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE;`})
	})
	t.Run("a membership holding the defaults matches a declaration that states none", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), plainRoles(), edge(false, new(true), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
	t.Run("a membership holding the defaults matches a declaration that states them", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)})
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, edge(false, new(true), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
	t.Run("the member's own inherit attribute is the default of the inherit option", func(t *testing.T) {
		c := qt.New(t)
		roles := plainRoles()
		roles[1].Inherit = false
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, edge(false, new(false), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
	t.Run("a membership holding the admin option is brought back to the default in place", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), plainRoles(), edge(true, new(true), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`GRANT "readers" TO "app" WITH ADMIN FALSE;`})
	})
	t.Run("only the differing options are written", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(true), Set: new(false)})
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, edge(true, new(false), new(false)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`GRANT "readers" TO "app" WITH INHERIT TRUE;`})
	})
	t.Run("a membership between declared roles that the member's stated list leaves out is revoked", func(t *testing.T) {
		c := qt.New(t)
		roles := []schemamodel.Role{{Name: "readers", Inherit: true}, {Name: "app", Inherit: true, MemberOfStated: true}}
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, edge(false, new(true), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`REVOKE "readers" FROM "app";`})
	})
	t.Run("a membership of a member that never wrote member_of is left alone", func(t *testing.T) {
		c := qt.New(t)
		roles := []schemamodel.Role{{Name: "readers", Inherit: true}, {Name: "app", Inherit: true}}
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, edge(false, new(true), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
	t.Run("a membership in a role the declaration does not name is left alone", func(t *testing.T) {
		c := qt.New(t)
		roles := []schemamodel.Role{{Name: "app", Inherit: true, MemberOfStated: true}}
		got, err := planMemberships(t, platform.Postgres, capability.Postgres16(), roles, edge(false, new(true), new(true)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
}

func TestPlan_PostgreSQL14_HappyPath(t *testing.T) {
	t.Run("the admin option is granted in place", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(true)})
		got, err := planMemberships(t, platform.Postgres, capability.Postgres14(), roles, edge(false, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`GRANT "readers" TO "app" WITH ADMIN OPTION;`})
	})
	t.Run("the admin option is withdrawn without losing the membership", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.Postgres, capability.Postgres14(), plainRoles(), edge(true, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{`REVOKE ADMIN OPTION FOR "readers" FROM "app";`})
	})
	t.Run("a membership with no option declared and none held plans nothing", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.Postgres, capability.Postgres14(), plainRoles(), edge(false, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
}

func TestPlan_PostgreSQL14_FailurePath(t *testing.T) {
	t.Run("an inherit option is refused rather than dropped", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Inherit: new(false)})
		got, err := planMemberships(t, platform.Postgres, capability.Postgres14(), roles, nil)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `the schema states the inherit or set option of its membership in a role: member "app", which requires target capability role_membership_inherit_set, .*`)
		c.Assert(got, qt.IsNil)
	})
}

func TestPlan_MySQL84_HappyPath(t *testing.T) {
	t.Run("the admin option is granted with the membership", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(true)})
		got, err := planMemberships(t, platform.MySQL, capability.MySQL84(), roles, nil)
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"GRANT `readers` TO `app` WITH ADMIN OPTION;"})
	})
	t.Run("the admin option is withdrawn by revoking and granting again", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.MySQL, capability.MySQL84(), plainRoles(), edge(true, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"REVOKE `readers` FROM `app`;", "GRANT `readers` TO `app`;"})
		raw, err := renderMembershipPlan(t, platform.MySQL, capability.MySQL84(), plainRoles(), edge(true, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(raw, qt.Matches, "(?s).*-- WARNING: app holds no readers between the next two statements, and keeps none "+
			"if the second one fails, because the server commits each statement as it runs\n"+
			"REVOKE `readers` FROM `app`;\nGRANT `readers` TO `app`;\n.*")
	})
	t.Run("a membership holding the declared option plans nothing", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(true)})
		got, err := planMemberships(t, platform.MySQL, capability.MySQL84(), roles, edge(true, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.HasLen, 0)
	})
}

func TestPlan_MySQL84_FailurePath(t *testing.T) {
	t.Run("a set option is refused", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Set: new(false)})
		got, err := planMemberships(t, platform.MySQL, capability.MySQL84(), roles, nil)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(got, qt.IsNil)
	})
}

func TestPlan_ClickHouse_HappyPath(t *testing.T) {
	t.Run("the admin option is granted with the membership", func(t *testing.T) {
		c := qt.New(t)
		roles := rolesWith(schemamodel.RoleMembershipOptions{Admin: new(true)})
		got, err := planMemberships(t, platform.ClickHouse, capability.ClickHouse24(), roles, nil)
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"GRANT `readers` TO `app` WITH ADMIN OPTION;"})
	})
	t.Run("the admin option is withdrawn in place", func(t *testing.T) {
		c := qt.New(t)
		got, err := planMemberships(t, platform.ClickHouse, capability.ClickHouse24(), plainRoles(), edge(true, nil, nil))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []string{"REVOKE ADMIN OPTION FOR `readers` FROM `app`;"})
	})
}

func TestPlan_AMembershipIsRefusedWhereTheTargetPlansNone(t *testing.T) {
	c := qt.New(t)
	got, err := planMemberships(t, platform.MariaDB, capability.MariaDB1011(), plainRoles(), nil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(got, qt.IsNil)
}

// TestPlan_MySQL84_TheWindowWarningSurvivesTheExactStyle keeps the warning in
// a plan that leaves out descriptive comments: it states a risk, not a label.
func TestPlan_MySQL84_TheWindowWarningSurvivesTheExactStyle(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	caps := capability.MySQL84()
	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), &schemamodel.Database{Roles: plainRoles()},
		&catalog.Database{Roles: []catalog.Role{{Name: "readers", Inherit: true}, {Name: "app", Inherit: true}}, RoleMemberships: edge(true, nil, nil)},
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "public", Capabilities: caps}, nil, runtime,
	)
	c.Assert(err, qt.IsNil)

	nodes, err := planner.GenerateSchemaDiffASTWithOptions(t.Context(), runtime, diff, platform.MySQL,
		planext.Options{Capabilities: caps, Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.MySQL, caps, nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "-- WARNING: app holds no readers")
}
