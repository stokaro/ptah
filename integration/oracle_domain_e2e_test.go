//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestOracleDomainPlansAndConvergesE2E is the assertion the domain_types
// capability key could not be flipped without.
//
// The key promises that a declared domain is planned, rendered, introspected
// and compared, and the failure when one of the four is missing is not a
// compile error: it is a plan that reports the same pending change forever,
// because the reader never sees what the renderer made
// (stokaro/ptah#1920).
//
// Two facts about Oracle's own catalog decide whether it converges, and both
// are asserted here rather than assumed.
//
// A domain declared NOT NULL grows a CHECK of its own, named by the server and
// numbered per database -- measured, SYS_DOMAIN_C0043 with the condition
// `"EMAIL_D" IS NOT NULL`. Reported as a declared CHECK it would compare
// against a declaration that has none, and the plan would carry the same
// change every run with a different name each time.
//
// And the column that carries the base type is NOT always called VALUE: it is
// named after the domain when the declaration never mentions VALUE, which is
// why the reader recognizes that constraint by the column name the catalog
// gives rather than by a literal.
func TestOracleDomainPlansAndConvergesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	dropOracleDomains(context.WithoutCancel(ctx), conn)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
		dropOracleDomains(context.WithoutCancel(ctx), conn)
	}()

	// Both Oracle lines are asserted rather than one skipped: 21 answers
	// ORA-00901 to CREATE DOMAIN and has no ALL_DOMAINS, so the declaration
	// has to be REFUSED there rather than planned, and a run that skipped
	// would report that refusal as covered without checking it.
	assertOracleDomainBehavior(ctx, c, conn)
}

// assertOracleDomainBehavior dispatches on what the target's preset says, so
// the branch lives here rather than in the test body.
func assertOracleDomainBehavior(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	if conn.Info().Capabilities.Has(capability.DomainTypes) {
		assertOracleDomainsConverge(ctx, c, conn)
		return
	}
	assertOracleDomainsAreRefused(ctx, c, conn)
}

// assertOracleDomainsAreRefused is the Oracle 21 half.
//
// The refusal is the Oracle type owner's: a column declared with a type the
// target cannot create would be left naming something the server has no
// definition of. It fires when the plan creates the domain, since a target
// without domains holds none and the comparison asks for each declared one.
//
// The read is asserted empty as well, and that is the other half of the same
// preset: ALL_DOMAINS does not exist on this line -- measured, ORA-00942 --
// so the reader must not ask, and a read that asked anyway would fail the
// statement and abort the transaction around it.
func assertOracleDomainsAreRefused(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDomainSummary(c, live), qt.HasLen, 0)

	declared := oracleDomainDeclaration(c)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	_, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "CREATE DOMAIN")
}

// assertOracleDomainsConverge is the Oracle 23 half: the whole loop.
func assertOracleDomainsConverge(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	declared := oracleDomainDeclaration(c)

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, before, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)

	// Non-vacuity: the plan really carries both domains, and it carries them
	// BEFORE the table whose column is typed by one. Oracle resolves that name
	// through the catalog, so the other order answers ORA-00902.
	c.Assert(oracleStatementsNamingDomain(statements), qt.HasLen, 2)
	c.Assert(oracleFirstIndexOf(c, statements, "CREATE DOMAIN") <
		oracleFirstIndexOf(c, statements, "CREATE TABLE"), qt.IsTrue)

	for _, statement := range statements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// What the catalog gives back, spelled out: the NOT NULL is a column fact
	// rather than a CHECK, and the declared CHECK survives as itself.
	c.Assert(oracleDomainSummary(c, after), qt.DeepEquals, []string{
		"DOM_EMAIL VARCHAR2(255) NOT NULL",
		"DOM_SCORE NUMBER(5,2) CHECK(VALUE BETWEEN 0 AND 100)",
	})

	settled, err := schemadiff.CompareWithDatabase(ctx, conn, declared, after, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(settled.FeatureChanges, qt.HasLen, 0)

	// And the removal direction, which has an ordering constraint of its own:
	// measured, dropping a domain a table still uses answers ORA-11502.
	teardown, err := schemadiff.CompareWithDatabase(ctx, conn, oracleTypesDeclared(c), after, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	teardownStatements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		teardown, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleFirstIndexOf(c, teardownStatements, "DROP TABLE") <
		oracleFirstIndexOf(c, teardownStatements, "DROP DOMAIN"), qt.IsTrue)
	for _, statement := range teardownStatements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	empty, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDomainSummary(c, empty), qt.HasLen, 0)
}

// oracleDomainDeclaration declares the two domains and the table one types.
func oracleDomainDeclaration(c *qt.C) *schemamodel.Database {
	declared := oracleTypesDeclared(c,
		oracleTypeObject(c, oratype.DomainKind, "dom_email", &oratype.DesiredDomain{BaseType: "VARCHAR2(255)", NotNull: true}),
		oracleTypeObject(c, oratype.DomainKind, "dom_score", &oratype.DesiredDomain{BaseType: "NUMBER(5,2)", Check: "VALUE BETWEEN 0 AND 100"}),
	)
	declared.Tables = []schemamodel.Table{{StructName: "T", Name: "dom_users"}}
	declared.Fields = []schemamodel.Field{
		{StructName: "T", Name: "id", Type: "INT", Primary: true},
		{StructName: "T", Name: "addr", Type: "dom_email"},
	}
	return declared
}

// oracleTypesDeclared is a desired schema holding objects, which describes
// every Oracle domain and object type it wants, as a schema source does: a
// type it leaves out is one to drop.
func oracleTypesDeclared(c *qt.C, objects ...schemaext.Object) *schemamodel.Database {
	c.Helper()
	known, err := oratype.CompleteCoverage(schemaext.Desired)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...)), FeatureCoverage: known}
}

// oracleTypeObject declares one Oracle domain or object type.
func oracleTypeObject(c *qt.C, kind schemaext.Kind, name string, value schemaext.Value) schemaext.Object {
	c.Helper()
	object, err := oratype.DesiredObject(oratype.Ref(kind, "", name), value)
	c.Assert(err, qt.IsNil)
	return object
}

// dropOracleDomains removes what this test creates, tolerating what is absent.
//
// Oracle has no DROP DOMAIN IF EXISTS worth relying on here -- the guard exists
// on 23 and not on 21 -- so the tolerance is in Go rather than in the SQL.
func dropOracleDomains(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, name := range []string{"dom_email", "dom_score"} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP DOMAIN "+name)
	}
}

// oracleDomainSummary renders each read domain in one line, by name.
func oracleDomainSummary(c *qt.C, read *catalog.Database) []string {
	c.Helper()
	objects, err := read.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	var summary []string
	for _, object := range objects {
		domain, ok := object.Value.(*oratype.ObservedDomain)
		if !ok {
			continue
		}
		line := object.Ref.Name.Source + " " + domain.BaseType
		if domain.NotNull {
			line += " NOT NULL"
		}
		if domain.Check != "" {
			line += " CHECK(" + domain.Check + ")"
		}
		summary = append(summary, line)
	}
	return summary
}

func oracleStatementsNamingDomain(statements []string) []string {
	var found []string
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), "CREATE DOMAIN") {
			found = append(found, statement)
		}
	}
	return found
}

func oracleFirstIndexOf(c *qt.C, statements []string, keyword string) int {
	c.Helper()
	for i, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), keyword) {
			return i
		}
	}
	c.Fatalf("no planned statement carries %q", keyword)
	return -1
}
