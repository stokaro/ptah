package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A replacement that drops a routine also drops its access list. Both
// directions must restore the grants on that exact overload, including a
// grant to a role the declaration does not manage and its grant option.
func TestPlanBidirectionalSchemaDiff_ChangedRoutineKeepsGrants(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
		arguments  string
		returns    string
	}{
		{name: "input type", parameters: "a bigint", arguments: "bigint", returns: "text"},
		{name: "parameter name", parameters: "renamed integer", arguments: "integer", returns: "text"},
		{name: "return type", parameters: "a integer", arguments: "integer", returns: "varchar"},
		{name: "parameter default", parameters: "a bigint DEFAULT 7", arguments: "bigint", returns: "text"},
		{name: "no arguments", returns: "text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := catalog.Function{Schema: "public", Name: "g", Parameters: "a integer", IdentityArguments: new("a integer"),
				Returns: "text", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'"}
			desired := &schemamodel.Database{Functions: []schemamodel.Function{{
				Name: "g", Parameters: test.parameters, Returns: test.returns,
				Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'",
			}}}
			current := &catalog.Database{Functions: []catalog.Function{held}, Grants: []catalog.Grant{
				{Role: "pg_monitor", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "a integer"},
				{Role: "delegator", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "a integer", WithOption: true},
				{Role: "other_overload", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "text"},
			}}

			up, down := changedRoutineGrantPlan(c, desired, current)

			for _, direction := range []struct{ sql, arguments string }{{up, test.arguments}, {down, "integer"}} {
				c.Assert(direction.sql, qt.Contains, `GRANT EXECUTE ON FUNCTION "g"(`+direction.arguments+`) TO "pg_monitor";`)
				c.Assert(direction.sql, qt.Contains, `GRANT EXECUTE ON FUNCTION "g"(`+direction.arguments+`) TO "delegator" WITH GRANT OPTION;`)
				c.Assert(direction.sql, qt.Contains, `REVOKE EXECUTE ON FUNCTION "g"(`+direction.arguments+`) FROM PUBLIC;`)
				c.Assert(direction.sql, qt.Not(qt.Contains), "other_overload")
			}
		})
	}
}

// A grant declared on the new signature is applied there, and no grant or
// revoke names the old signature after its DROP. The rollback gives the
// original signature its original privileges.
func TestPlanBidirectionalSchemaDiff_ChangedRoutineManagedGrant(t *testing.T) {
	c := qt.New(t)
	held := catalog.Function{Schema: "public", Name: "g", Parameters: "a integer", IdentityArguments: new("a integer"),
		Returns: "text", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'"}
	desired := &schemamodel.Database{
		Functions: []schemamodel.Function{{Name: "g", Parameters: "a bigint", Returns: "text", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'"}},
		Roles:     []schemamodel.Role{{Name: "app"}},
		Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"EXECUTE"}, OnRoutine: "g",
			RoutineKind: "FUNCTION", RoutineArguments: "bigint"}},
	}
	current := &catalog.Database{Functions: []catalog.Function{held}, Roles: []catalog.Role{{Name: "app"}}, Grants: []catalog.Grant{
		{Role: "app", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "a integer", WithOption: true},
		{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "a integer", Implicit: true},
	}}

	up, down := changedRoutineGrantPlan(c, desired, current)

	c.Assert(up, qt.Contains, `GRANT EXECUTE ON FUNCTION "g"(bigint) TO "app";`)
	c.Assert(up, qt.Not(qt.Contains), "REVOKE")
	c.Assert(up, qt.Not(qt.Contains), "WITH GRANT OPTION")
	c.Assert(down, qt.Contains, `GRANT EXECUTE ON FUNCTION "public"."g"(a integer) TO "app" WITH GRANT OPTION;`)
	c.Assert(down, qt.Not(qt.Contains), "REVOKE")
}

// CREATE OR REPLACE keeps an access list in place; a body change neither
// re-grants it nor revokes the default PUBLIC privilege.
func TestPlanBidirectionalSchemaDiff_ChangedBodyLeavesGrantsInPlace(t *testing.T) {
	c := qt.New(t)
	held := catalog.Function{Schema: "public", Name: "g", Parameters: "a integer", IdentityArguments: new("a integer"),
		Returns: "text", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'"}
	desired := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "g", Parameters: "a integer", Returns: "text",
		Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'changed'"}}}
	current := &catalog.Database{Functions: []catalog.Function{held}, Grants: []catalog.Grant{
		{Role: "pg_monitor", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "a integer"},
	}}

	up, down := changedRoutineGrantPlan(c, desired, current)

	for _, sql := range []string{up, down} {
		c.Assert(sql, qt.Not(qt.Contains), "GRANT")
		c.Assert(sql, qt.Not(qt.Contains), "REVOKE")
	}
}

// An owner-only read proves PUBLIC was revoked. A read that excluded grants
// does not, and must not invent a revoke in either direction.
func TestPlanBidirectionalSchemaDiff_ChangedRoutineGrantSnapshot(t *testing.T) {
	for _, test := range []struct {
		name    string
		grants  []catalog.Grant
		want    string
		notWant string
	}{
		{name: "owner only", grants: []catalog.Grant{{Role: "owner", Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "g", Arguments: "a integer", Implicit: true}}, want: "FROM PUBLIC", notWant: "TO PUBLIC"},
		{name: "grants excluded", want: "CREATE OR REPLACE", notWant: "REVOKE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := catalog.Function{Schema: "public", Name: "g", Parameters: "a integer", IdentityArguments: new("a integer"),
				Returns: "text", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'"}
			desired := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "g", Parameters: "a bigint", Returns: "text",
				Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "SELECT 'held'"}}}
			current := &catalog.Database{Functions: []catalog.Function{held}, Grants: test.grants}

			up, down := changedRoutineGrantPlan(c, desired, current)

			for _, sql := range []string{up, down} {
				c.Assert(sql, qt.Contains, test.want)
				c.Assert(sql, qt.Not(qt.Contains), test.notWant)
			}
		})
	}
}

func changedRoutineGrantPlan(c *qt.C, desired *schemamodel.Database, current *catalog.Database) (up, down string) {
	c.Helper()
	runtime := must.Must(builtin.New())
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "public"
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", IdentifierSemantics: semantics}, nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: platform.Postgres, Capabilities: capability.ForDialect(platform.Postgres),
	})
	c.Assert(err, qt.IsNil)
	up, err = builtin.RenderSQL(platform.Postgres, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	down, err = builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return up, down
}
