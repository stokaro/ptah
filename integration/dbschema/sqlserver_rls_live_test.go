//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerLiveRLSRoundTrip is the test the RowLevelSecurity capability
// could not have been flipped without.
//
// The key promises render, read back and plan together, and the failure when
// one is missing is not a compile error but an apply loop that plans the same
// policy forever. This target has two ways to fall into that, and both are
// live-only:
//
//   - the target of a predicate has to be a two-part name, so a renderer that
//     emits `ON [t]` produces a statement the engine refuses with
//     `Cannot schema bind security policy`, which no offline assertion sees;
//   - sys.security_predicates hands the predicate back fully bracketed --
//     `([dbo].[fn_tenant]([tenant]))` for a policy created from
//     `dbo.fn_tenant(tenant)` -- so a reader that returns the catalog spelling
//     verbatim reports using_expression as changed on every run.
//
// Step 3 is the assertion that decides the key.
func TestSQLServerLiveRLSRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rls_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx,
			"DROP SECURITY POLICY IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("tenant_isolation"))
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("documents"))
		_, _ = conn.ExecContext(ctx, "DROP FUNCTION IF EXISTS "+quoted+"."+quoteSQLServerIdentifier("fn_tenant"))
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	// The predicate function is created outside Ptah, which is the design this
	// target forces rather than an omission in the test. T-SQL has no inline
	// predicate expression, and Ptah does not manage SQL Server routines
	// (capability.Functions is false here), so the policy references a function
	// its author owns.
	_, err = conn.ExecContext(ctx, "EXEC('CREATE FUNCTION "+quoted+".fn_tenant(@tenant int) "+
		"RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS allowed WHERE @tenant = 1')")
	c.Assert(err, qt.IsNil)

	description := sqlServerRLSSchema(schemaName)

	// 1. The renderer's statements are the ones the server is given, so a
	// statement this engine refuses fails here rather than being corrected by
	// hand.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE SECURITY POLICY")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it holds. The predicate has to come back in
	// the declaration's spelling, not the catalog's.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.RLSPolicies, qt.HasLen, 1)
	c.Assert(live.RLSPolicies[0].Table, qt.Equals, "documents")
	c.Assert(live.RLSPolicies[0].UsingExpression, qt.Equals, schemaName+".fn_tenant(tenant)")

	// The derived table flag: SQL Server has no per-table RLS attribute, so an
	// enabled policy naming the table is what makes it true.
	c.Assert(sqlServerTableNamed(live.Tables, "documents").RLSEnabled, qt.IsTrue)

	// 3. The convergence assertion. Comparing the same description against what
	// the server now holds must produce nothing to do.
	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(settled.RLSPoliciesAdded, qt.HasLen, 0)
	c.Assert(settled.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(settled.RLSPoliciesModified, qt.HasLen, 0)
}

// TestSQLServerLiveRLSRefusesWhatTheRendererDeclines pins that the two
// declarations the renderer answers with a sentence are ones the engine really
// refuses.
//
// Without this the refusals are just this renderer's opinion, and an opinion
// that turned out to be wrong would be a capability withheld for no reason.
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

// sqlServerRLSSchema is a table under one policy, with the predicate naming a
// function the schema does not declare.
func sqlServerRLSSchema(schemaName string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Document", Name: "documents", Schema: schemaName},
		},
		Fields: []goschema.Field{
			{StructName: "Document", Name: "id", Type: "INT", Nullable: false},
			{StructName: "Document", Name: "tenant", Type: "INT", Nullable: false},
		},
		RLSPolicies: []goschema.RLSPolicy{{
			StructName:      "Document",
			Name:            "tenant_isolation",
			Table:           "documents",
			UsingExpression: schemaName + ".fn_tenant(tenant)",
		}},
	}
}

// sqlServerTableNamed returns the table a catalog read reports under a name.
func sqlServerTableNamed(tables []dbschematypes.DBTable, name string) dbschematypes.DBTable {
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	return dbschematypes.DBTable{}
}
