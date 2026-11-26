package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// changedRoutineRollback is the down migration of the plan that gives the
// database current describes, the read of schema on dialect, the routines
// desired declares, rendered as one script.
func changedRoutineRollback(c *qt.C, dialect, schema string, desired *schemamodel.Database, current *catalog.Database) string {
	c.Helper()
	runtime := must.Must(builtin.New())
	// A live read names the schema it describes as the default one, so a
	// name the declaration leaves unqualified resolves to it.
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = schema
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
		catalog.ServerInfo{Dialect: dialect, Schema: schema, IdentifierSemantics: semantics}, nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: dialect, Capabilities: capability.ForDialect(dialect),
	})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(dialect, capability.ForDialect(dialect), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// The rollback of a change to a routine's body creates the routine again as
// the database held it, with its old body, security and volatility, under the
// name the declaration gives it. The declaration leaves the schema out, and
// the read names every routine in the schema it is in: on MySQL and MariaDB
// the database, on SQL Server dbo, on Oracle the connected user. On the MySQL
// family the down migration also gives back what each account held on the
// routine, which the DROP takes.
func TestPlanBidirectionalSchemaDiff_ChangedRoutineComesBackWithItsBody(t *testing.T) {
	procedure := schemamodel.FunctionKindProcedure
	shopGrants := catalog.RoutinePrivileges{Grants: []catalog.RoutineGrant{
		{User: "svc", Host: "%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}, ReaderMayGrant: true},
		{User: "svc", Host: "%", Schema: "shop", Name: "rf", Type: "FUNCTION", Privileges: []string{"EXECUTE"}, ReaderMayGrant: true},
	}}
	tests := []struct {
		name    string
		dialect string
		schema  string
		desired schemamodel.Function
		current catalog.Function
		grants  catalog.RoutinePrivileges
		want    string
	}{
		{
			name:    "a MySQL procedure",
			dialect: platform.MySQL,
			schema:  "shop",
			desired: schemamodel.Function{Name: "rt", Kind: procedure, Language: "sql", Security: "DEFINER", Volatility: "VOLATILE", Body: "SELECT 2"},
			current: catalog.Function{Schema: "shop", Name: "rt", Kind: procedure, Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT 1"},
			grants:  shopGrants,
			want: "-- Replace procedure rt: body, security, volatility\nDROP PROCEDURE IF EXISTS `rt`;\n" +
				"-- Modify procedure rt: body, security, volatility\n" +
				"CREATE PROCEDURE `rt`() NOT DETERMINISTIC NO SQL SQL SECURITY INVOKER SELECT 1;\n" +
				"GRANT EXECUTE ON PROCEDURE `rt` TO `svc`@`%`;\n",
		},
		{
			name:    "a MariaDB procedure",
			dialect: platform.MariaDB,
			schema:  "shop",
			desired: schemamodel.Function{Name: "rt", Kind: procedure, Language: "sql", Security: "DEFINER", Volatility: "VOLATILE", Body: "SELECT 2"},
			current: catalog.Function{Schema: "shop", Name: "rt", Kind: procedure, Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT 1"},
			grants:  shopGrants,
			want: "-- Replace procedure rt: body, security, volatility\nDROP PROCEDURE IF EXISTS `rt`;\n" +
				"-- Modify procedure rt: body, security, volatility\n" +
				"CREATE PROCEDURE `rt`() NOT DETERMINISTIC NO SQL SQL SECURITY INVOKER SELECT 1;\n" +
				"GRANT EXECUTE ON PROCEDURE `rt` TO `svc`@`%`;\n",
		},
		{
			name:    "a MySQL function",
			dialect: platform.MySQL,
			schema:  "shop",
			desired: schemamodel.Function{Name: "rf", Parameters: "a int", Returns: "int", Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: "RETURN a + 2"},
			current: catalog.Function{Schema: "shop", Name: "rf", Parameters: "a int", Returns: "int", Language: "sql", Security: "DEFINER", Volatility: "IMMUTABLE", Body: "RETURN a + 1"},
			grants:  shopGrants,
			want: "-- Replace function rf: body, security, volatility\nDROP FUNCTION IF EXISTS `rf`;\n" +
				"-- Modify function rf: body, security, volatility\n" +
				"CREATE FUNCTION `rf`(a int) RETURNS int DETERMINISTIC SQL SECURITY DEFINER RETURN a + 1;\n" +
				"GRANT EXECUTE ON FUNCTION `rf` TO `svc`@`%`;\n",
		},
		{
			name:    "a SQL Server procedure",
			dialect: platform.SQLServer,
			schema:  "dbo",
			desired: schemamodel.Function{Name: "rt", Kind: procedure, Language: "sql", Body: "BEGIN SELECT 2 END"},
			current: catalog.Function{Schema: "dbo", Name: "rt", Kind: procedure, Language: "sql", Body: "BEGIN SELECT 1 END"},
			want: "-- Replace procedure rt: body, volatility\nDROP PROCEDURE IF EXISTS [rt];\n" +
				"-- Modify procedure rt: body, volatility\nCREATE OR ALTER PROCEDURE [rt] \nAS\nBEGIN SELECT 1 END;\n",
		},
		{
			name:    "an Oracle procedure",
			dialect: platform.Oracle,
			schema:  "APP",
			desired: schemamodel.Function{Name: "rt", Kind: procedure, Language: "plsql", Body: "BEGIN DBMS_OUTPUT.PUT_LINE(2); END;"},
			current: catalog.Function{Schema: "APP", Name: "rt", Kind: procedure, Language: "plsql", Body: "BEGIN NULL; END;"},
			want: "-- Replace procedure rt: body, volatility\nDROP PROCEDURE IF EXISTS rt;\n" +
				"-- Modify procedure rt: body, volatility\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN NULL; END;\n",
		},
		{
			name:    "a PostgreSQL function",
			dialect: platform.Postgres,
			schema:  "public",
			desired: schemamodel.Function{Name: "f", Parameters: "a integer", Returns: "integer", Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT a + 2"},
			current: catalog.Function{Schema: "public", Name: "f", Parameters: "a integer", IdentityArguments: new("a integer"), Returns: "integer", Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT a + 1"},
			want: "-- Modify function f: body\n" +
				"CREATE OR REPLACE FUNCTION \"f\"(a integer) RETURNS integer AS $$\nSELECT a + 1\n$$\nLANGUAGE sql SECURITY INVOKER STABLE;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Functions: []schemamodel.Function{test.desired}}
			current := &catalog.Database{Functions: []catalog.Function{test.current}, RoutinePrivileges: test.grants}

			rollback := changedRoutineRollback(c, test.dialect, test.schema, desired, current)

			c.Assert(rollback, qt.Equals, test.want)
		})
	}
}

// A MySQL database may hold a function and a procedure of the same name, and
// neither carries an argument list that tells them apart. The rollback of a
// change to the procedure creates the procedure again with the body it had,
// not the function's, and leaves the function alone.
func TestPlanBidirectionalSchemaDiff_ChangedProcedureComesBackAsItself(t *testing.T) {
	c := qt.New(t)
	function := schemamodel.Function{Name: "rt", Returns: "int", Language: "sql", Security: "INVOKER", Volatility: "IMMUTABLE", Body: "RETURN 7"}
	desired := &schemamodel.Database{Functions: []schemamodel.Function{
		function,
		{Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT 2"},
	}}
	current := &catalog.Database{Functions: []catalog.Function{
		{Schema: "shop", Name: "rt", Returns: "int", Language: "sql", Security: "INVOKER", Volatility: "IMMUTABLE", Body: "RETURN 7"},
		{Schema: "shop", Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT 1"},
	}}

	rollback := changedRoutineRollback(c, platform.MySQL, "shop", desired, current)

	c.Assert(rollback, qt.Equals, "-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS `rt`;\n"+
		"-- Modify procedure rt: body\nCREATE PROCEDURE `rt`() NOT DETERMINISTIC NO SQL SQL SECURITY INVOKER SELECT 1;\n")
}

// A MySQL database may hold a function and a procedure of the same name. When
// the declaration keeps only the function, the rollback of removing the
// procedure creates the procedure again as the database held it. Created from
// the function the database also holds, the down migration would create a
// function the database still has, and the server would refuse it with error
// 1304. Whether the procedure is named with the database the read describes
// is stokaro/ptah#6710, so the pattern takes either name.
func TestPlanBidirectionalSchemaDiff_RemovedProcedureComesBackAsItself(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "rt", Returns: "int", Language: "sql", Security: "INVOKER", Volatility: "IMMUTABLE", Body: "RETURN 7"},
	}}
	current := &catalog.Database{Functions: []catalog.Function{
		{Schema: "shop", Name: "rt", Returns: "int", Language: "sql", Security: "INVOKER", Volatility: "IMMUTABLE", Body: "RETURN 7"},
		{Schema: "shop", Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Security: "INVOKER", Volatility: "STABLE", Body: "SELECT 1"},
	}}

	rollback := changedRoutineRollback(c, platform.MySQL, "shop", desired, current)

	c.Assert(rollback, qt.Matches, "CREATE PROCEDURE (`shop`\\.)?`rt`\\(\\) NOT DETERMINISTIC NO SQL SQL SECURITY INVOKER SELECT 1;\n")
}

// The rollback of a change to a routine the read does not hold has nothing to
// create again, so the plan is refused. Planned, its down migration would keep
// the body the up migration wrote. The read may hold the name as the other
// kind of routine, which is not the one that changed, and a diff that records
// no name the read gave the routine looks it up under the declaration's.
func TestPlanBidirectionalSchemaDiff_RefusesAChangedRoutineTheReadDoesNotHold_FailurePath(t *testing.T) {
	procedure := schemamodel.Function{Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "SELECT 2"}
	held := catalog.Function{Schema: "shop", Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "SELECT 1"}
	tests := []struct {
		name        string
		currentName string
		current     []catalog.Function
	}{
		{name: "no routine of that name", currentName: "shop.rt"},
		{
			name:        "a function of the procedure's name",
			currentName: "shop.rt",
			current:     []catalog.Function{{Schema: "shop", Name: "rt", Returns: "int", Language: "sql", Body: "RETURN 1"}},
		},
		{name: "the procedure under the read's name", current: []catalog.Function{held}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FunctionsModified: []difftypes.FunctionDiff{{
				FunctionName: "rt", CurrentName: test.currentName, Desired: procedure,
				Changes: map[string]string{"body": "SELECT 1 -> SELECT 2"},
			}}}

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: must.Must(builtin.New()), Diff: diff,
				DesiredSchema: &schemamodel.Database{Functions: []schemamodel.Function{procedure}},
				CurrentSchema: &catalog.Database{Functions: test.current},
				Dialect:       platform.MySQL, Capabilities: capability.ForDialect(platform.MySQL),
			})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*cannot plan the rollback of changing rt\(\): the database read holds no such routine to restore`)
			c.Assert(plan, qt.IsNil)
		})
	}
}
