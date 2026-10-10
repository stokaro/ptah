//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestSQLServerLiveRLSRoundTrip declares a security policy the way an author
// does, as a row-level security annotation scoped to SQL Server, applies it,
// reads it back, and compares again: the second comparison must plan nothing.
//
// The failure it guards against is an apply loop, which no offline assertion
// sees:
//
//   - the target of a predicate has to be a two-part name, so a statement
//     that writes `ON [t]` is refused with `Cannot schema bind security
//     policy`;
//   - sys.security_predicates hands every argument back rewritten --
//     `[tenant]` for `tenant`, and `CONVERT([int],[tenant])+(0)` for
//     `CAST(tenant AS int) + 0` -- so a comparison that takes the catalog's
//     spelling at face value plans the policy again on every run.
//
// The cast is the half only the server can settle. Offline the comparison is
// undecided and refuses to plan; connected, the owner's probe asks the server
// how it stores the declaration, and the policy agrees.
func TestSQLServerLiveRLSRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rls_%d", time.Now().UnixNano())
	functions := schemaName + "_fn"
	quoted, quotedFunctions := quoteSQLServerIdentifier(schemaName), quoteSQLServerIdentifier(functions)
	for _, schema := range []string{quoted, quotedFunctions} {
		_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+schema+"')")
		c.Assert(err, qt.IsNil)
	}
	defer func() {
		_, _ = conn.ExecContext(ctx,
			"DROP SECURITY POLICY IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("tenant_isolation"))
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("documents"))
		_, _ = conn.ExecContext(ctx, "DROP FUNCTION IF EXISTS "+quotedFunctions+"."+quoteSQLServerIdentifier("fn_tenant"))
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quotedFunctions)
	}()

	// The predicate function is created outside Ptah, in a schema of its own
	// that the read leaves out, which is the design this target forces rather
	// than an omission in the test. T-SQL has no inline predicate expression,
	// so the policy references a function its author owns; a read of its
	// schema would report the function, and a declaration that does not
	// manage it would drop it.
	_, err = conn.ExecContext(ctx, "EXEC('CREATE FUNCTION "+quotedFunctions+".fn_tenant(@tenant int) "+
		"RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS allowed WHERE @tenant = 1')")
	c.Assert(err, qt.IsNil)

	description, err := goschema.ParseSource(builtintest.Annotations(), "documents.go", fmt.Sprintf(`package documents

//ptah:schema:table name="documents" schema="%[1]s"
//ptah:schema:rls:policy name="tenant_isolation" table="%[1]s.documents" for="INSERT" using="%[2]s.fn_tenant(tenant)" with_check="%[2]s.fn_tenant(CAST(tenant AS int) + 0)" dialects="sqlserver"
type Document struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="tenant" type="INT" not_null="true"
	Tenant int
}
`, schemaName, functions))
	c.Assert(err, qt.IsNil)

	// 1. The statements a render writes are the ones the server is given, so
	// one this engine refuses fails here rather than being corrected by hand.
	statements, err := builtin.GetOrderedCreateStatements(&description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE SECURITY POLICY")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it holds, in its own spelling.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	function := mssqlschema.ObjectName{Schema: functions, Name: "fn_tenant"}
	table := mssqlschema.ObjectName{Schema: schemaName, Name: "documents"}
	held, found, err := live.FeatureObjects.Get(mssqlschema.SecurityPolicyRef(schemaName, "tenant_isolation"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(held.Value, qt.DeepEquals, &mssqlschema.ObservedSecurityPolicy{Enabled: true, SchemaBinding: true,
		Predicates: []mssqlschema.Predicate{
			{Type: mssqlschema.Filter, Function: function, Arguments: []string{"[tenant]"}, Table: table},
			{Type: mssqlschema.Block, Function: function, Arguments: []string{"CONVERT([int],[tenant])+(0)"}, Table: table,
				Operation: mssqlschema.AfterInsert},
		}})

	// 3. Offline the cast is undecided, and the comparison refuses to plan
	// rather than drop and create a security control nobody changed.
	_, err = schemadiff.CompareWithDialect(ctx, &description, live, platform.SQLServer, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorMatches, `(?s).*the server rewrites argument expressions, so only it can tell.*`)

	// 4. The convergence assertion: connected, the probe spells the
	// declaration as the server stores it, and the plan is empty.
	settled, err := schemadiff.CompareWithDatabase(ctx, conn, &description, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(settled.FeatureChanges, qt.HasLen, 0)
	c.Assert(settled.HasChanges(), qt.IsFalse)
}

// TestSQLServerLiveRLSRefusesWhatTheRendererDeclines pins that the
// declarations a SQL Server-scoped source refuses (see
// mssqlpolicysource.Attributes.Policy) are ones the engine really refuses.
//
// Without this the refusals are just Ptah's opinion, and an opinion that
// turned out to be wrong would be a capability withheld for no reason.
func TestSQLServerLiveRLSRefusesWhatTheRendererDeclines(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rlsref_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("documents"))
		_, _ = conn.ExecContext(ctx, "DROP FUNCTION IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("fn_tenant"))
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()
	_, err = conn.ExecContext(ctx, "EXEC('CREATE TABLE "+quoted+".documents (id int NOT NULL, tenant int NOT NULL)')")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE FUNCTION "+quoted+".fn_tenant(@tenant int) "+
		"RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS allowed WHERE @tenant = 1')")
	c.Assert(err, qt.IsNil)

	target := quoted + ".documents"
	predicate := quoted + ".fn_tenant(tenant)"

	rows := []struct {
		name      string
		statement string
		refusal   string
	}{{
		name:      "an inline expression is not a predicate",
		statement: "CREATE SECURITY POLICY " + quoted + ".p ADD FILTER PREDICATE (tenant = 1) ON " + target + " WITH (STATE = ON)",
		refusal:   "Incorrect syntax",
	}, {
		name:      "a one-part predicate name cannot be schema bound",
		statement: "CREATE SECURITY POLICY " + quoted + ".p ADD FILTER PREDICATE fn_tenant(tenant) ON " + target + " WITH (STATE = ON)",
		refusal:   "invalid for schema binding",
	}, {
		name:      "a one-part target cannot be schema bound either",
		statement: "CREATE SECURITY POLICY " + quoted + ".p ADD FILTER PREDICATE " + predicate + " ON documents WITH (STATE = ON)",
		refusal:   "invalid for schema binding",
	}, {
		name:      "IF NOT EXISTS is not a clause here",
		statement: "CREATE SECURITY POLICY IF NOT EXISTS " + quoted + ".p ADD FILTER PREDICATE " + predicate + " ON " + target + " WITH (STATE = ON)",
		// The parser names whichever token it stopped on, and that is not
		// stable across statement shapes -- the same clause draws
		// `near the keyword 'IF'` on one and `near 'FILTER'` on another. What
		// is stable is that it never parses.
		refusal: "Incorrect syntax",
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, execErr := conn.ExecContext(ctx, row.statement)
			c.Assert(execErr, qt.IsNotNil, qt.Commentf("statement:\n%s", row.statement))
			c.Assert(execErr.Error(), qt.Contains, row.refusal)
		})
	}

	// The control: the form the renderer does emit is accepted against the same
	// schema, so the four refusals above are about those spellings and not about
	// a fixture the engine dislikes for some other reason.
	t.Run("the rendered form is accepted", func(t *testing.T) {
		c := qt.New(t)

		_, execErr := conn.ExecContext(ctx,
			"CREATE SECURITY POLICY "+quoted+".p_ok ADD FILTER PREDICATE "+predicate+" ON "+target+" WITH (STATE = ON)")
		c.Assert(execErr, qt.IsNil)
		_, _ = conn.ExecContext(ctx, "DROP SECURITY POLICY IF EXISTS "+quoted+".p_ok")
	})
}
