package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_RemovedRoutineComesBackWithItsProperties
// pins the rollback of a routine the forward plan removes: it creates the
// routine again as the database held it, with its pinned settings, STRICT,
// LEAKPROOF, PARALLEL and SECURITY, and with a SQL-standard body as SQL. A
// SECURITY DEFINER function that came back without its search_path would run
// with the caller's.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineComesBackWithItsProperties(t *testing.T) {
	tests := []struct {
		name    string
		routine catalog.Function
		want    string
	}{
		{
			name: "a function with settings and properties",
			routine: catalog.Function{Name: "f", Schema: "public", Parameters: "x integer", Returns: "integer",
				Language: "sql", Security: "DEFINER", Volatility: "IMMUTABLE", Body: "SELECT x",
				Settings: []string{"search_path=pg_catalog"}, IdentityArguments: new("x integer"),
				Facets: must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, pgroutine.Properties{Strict: true, Leakproof: true, Parallel: "SAFE"}))},
			want: "CREATE FUNCTION \"public\".\"f\"(x integer) RETURNS integer AS $$\nSELECT x\n$$\n" +
				"LANGUAGE sql SECURITY DEFINER IMMUTABLE LEAKPROOF STRICT PARALLEL SAFE SET search_path = pg_catalog;",
		},
		{
			name: "a function with a SQL-standard BEGIN ATOMIC body",
			routine: catalog.Function{Name: "c", Schema: "public", Returns: "bigint", Language: "sql", Security: "INVOKER",
				Volatility: "STABLE", Body: "BEGIN ATOMIC\n SELECT count(t.a) AS count\n    FROM t;\nEND", IdentityArguments: new("")},
			want: "CREATE FUNCTION \"public\".\"c\"() RETURNS bigint LANGUAGE sql SECURITY INVOKER STABLE BEGIN ATOMIC\n" +
				" SELECT count(t.a) AS count\n    FROM t;\nEND;",
		},
		{
			name: "a function with a SQL-standard RETURN body",
			routine: catalog.Function{Name: "n", Schema: "public", Returns: "bigint", Language: "sql", Security: "INVOKER",
				Volatility: "STABLE", Body: "RETURN (SELECT count(*) AS count FROM t)", IdentityArguments: new("")},
			want: `CREATE FUNCTION "public"."n"() RETURNS bigint LANGUAGE sql SECURITY INVOKER STABLE RETURN (SELECT count(*) AS count FROM t);`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{}
			current := &catalog.Database{Functions: []catalog.Function{test.routine}}
			diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
				catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
				Dialect: platform.Postgres, Capabilities: capability.Postgres18(),
			})
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// securedFunction is a SECURITY DEFINER function the database holds, in the
// schema the connection defaults to.
var securedFunction = catalog.Function{Name: "f", Schema: "public", Parameters: "x integer", Returns: "integer",
	Language: "sql", Security: "DEFINER", Volatility: "IMMUTABLE", Body: "SELECT x", IdentityArguments: new("x integer")}

// executeGrant is a row the PostgreSQL reader reports for f.
func executeGrant(role, grantor string, withOption, implicit bool) catalog.Grant {
	return catalog.Grant{Role: role, Privilege: "EXECUTE", ObjectType: "FUNCTION", Schema: "public", ObjectName: "f",
		Arguments: "x integer", GrantedBy: grantor, WithOption: withOption, Implicit: implicit}
}

// reverseOfRemoving plans the removal of every routine current holds and
// renders the rollback.
func reverseOfRemoving(c *qt.C, current *catalog.Database) (string, error) {
	c.Helper()
	runtime := must.Must(builtin.New())
	desired := &schemamodel.Database{}
	diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"}, nil, runtime))
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: platform.Postgres, Capabilities: capability.Postgres18(),
	})
	if err != nil {
		return "", err
	}
	return builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
}

// TestPlanBidirectionalSchemaDiff_RemovedRoutineComesBackWithItsPrivileges
// pins the privileges the rollback of a removed routine gives back. A new
// routine is executable by PUBLIC, so one whose read reports no PUBLIC
// EXECUTE has it revoked, and the privileges of every other role, grant option
// included, are granted again. The old owner gets EXECUTE because the
// migrator may be a different role. A read that excluded grants gives no ACL
// evidence, so the rollback must not infer a PUBLIC revoke from it.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineComesBackWithItsPrivileges(t *testing.T) {
	tests := []struct {
		name    string
		grants  []catalog.Grant
		want    []string
		notWant []string
	}{
		{
			name: "a function revoked from PUBLIC and granted to a role",
			grants: []catalog.Grant{
				executeGrant("app_owner", "", false, true),
				executeGrant("reporting", "app_owner", false, false),
				executeGrant("auditor", "app_owner", true, false),
			},
			want: []string{
				`REVOKE EXECUTE ON FUNCTION "public"."f"(x integer) FROM PUBLIC;`,
				`GRANT EXECUTE ON FUNCTION "public"."f"(x integer) TO "app_owner";`,
				`GRANT EXECUTE ON FUNCTION "public"."f"(x integer) TO "reporting";`,
				`GRANT EXECUTE ON FUNCTION "public"."f"(x integer) TO "auditor" WITH GRANT OPTION;`,
			},
		},
		{
			name:    "a function that holds its default ACL",
			grants:  []catalog.Grant{executeGrant("PUBLIC", "", false, true)},
			notWant: []string{"REVOKE", "GRANT"},
		},
		{
			name: "an owner who revoked its own EXECUTE",
			grants: []catalog.Grant{{Role: "app_owner", ObjectType: "FUNCTION", Schema: "public", ObjectName: "f",
				Arguments: "x integer", Implicit: true}},
			want:    []string{`REVOKE EXECUTE ON FUNCTION "public"."f"(x integer) FROM PUBLIC;`},
			notWant: []string{`TO "app_owner"`},
		},
		{
			name:    "a read that excluded routine grants",
			notWant: []string{"REVOKE", "GRANT"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := reverseOfRemoving(c, &catalog.Database{Functions: []catalog.Function{securedFunction}, Grants: test.grants})

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(sql, qt.Contains, want)
			}
			for _, notWant := range test.notWant {
				c.Assert(sql, qt.Not(qt.Contains), notWant)
			}
		})
	}
}

// TestPlanBidirectionalSchemaDiff_RemovedRoutineOutsidePostgres keeps the
// rollback of a routine usable on engines whose routine privileges Ptah does
// not model. A PostgreSQL PUBLIC revoke on their down file is refused before
// the file can be written.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineOutsidePostgres(t *testing.T) {
	tests := []struct {
		dialect string
		schema  string
		caps    capability.Capabilities
	}{
		{dialect: platform.MySQL, schema: "app", caps: capability.MySQL84()},
		{dialect: platform.MariaDB, schema: "app", caps: capability.MariaDB1011()},
		{dialect: platform.SQLServer, schema: "dbo", caps: capability.SQLServer2022()},
		{dialect: platform.Oracle, schema: "APP", caps: capability.Oracle23()},
		{dialect: platform.ClickHouse, schema: "default", caps: capability.ClickHouse24()},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current := &catalog.Database{Functions: []catalog.Function{{
				Name: "f", Schema: test.schema, Returns: "int", Language: "SQL", Body: "RETURN 1",
			}}}
			desired := &schemamodel.Database{}
			diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
				catalog.ServerInfo{Dialect: test.dialect, Schema: test.schema}, nil, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
				Dialect: test.dialect, Capabilities: test.caps,
			})
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL(test.dialect, plan.Reverse.Nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Not(qt.Contains), "REVOKE EXECUTE")
		})
	}
}

// TestPlanBidirectionalSchemaDiff_RefusesARoutineTheReadDoesNotHold_FailurePath
// pins the refusal of a rollback that would create a removed routine without
// the declaration the database held: a diff that removes a routine the read
// does not report has nothing to restore it from.
func TestPlanBidirectionalSchemaDiff_RefusesARoutineTheReadDoesNotHold_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{FunctionsRemoved: difftypes.FunctionChanges{
		{Function: schemamodel.Function{Name: "public.gone"}, Signature: new("")},
	}}

	_, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: &schemamodel.Database{}, CurrentSchema: &catalog.Database{},
		Dialect: platform.Postgres, Capabilities: capability.Postgres18(),
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*cannot plan the rollback of removing public\.gone\(\): the database read holds no such routine to restore`)
}
