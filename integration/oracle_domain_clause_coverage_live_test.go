//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestOracleDomainClausesTheModelCannotHoldAreNotDescribedLive pins what the
// read does with a domain that carries a DISPLAY or ORDER expression or
// annotations: it records the domain as one it cannot describe, and a
// declaration of that name is undecided.
//
// The domain model holds a base type, NOT NULL, a default and a CHECK, so such
// a domain was described without the clause. A comparison then called a
// declaration without it the same domain, and `schema inspect` wrote a
// statement that replays without it, measured on 23.26.3.0.0.
func TestOracleDomainClausesTheModelCannotHoldAreNotDescribedLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleDomainClauseFixture(context.WithoutCancel(ctx), conn)
	defer dropOracleDomainClauseFixture(context.WithoutCancel(ctx), conn)
	for _, statement := range []string{
		"CREATE DOMAIN dcl_display AS VARCHAR2(10) DISPLAY UPPER(dcl_display)",
		"CREATE DOMAIN dcl_order AS VARCHAR2(10) ORDER LOWER(dcl_order)",
		"CREATE DOMAIN dcl_noted AS NUMBER ANNOTATIONS (Sensitive, Owner 'billing')",
		"CREATE DOMAIN dcl_plain AS NUMBER",
	} {
		execOracle(ctx, c, conn, statement)
	}

	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(unrepresentableDomainClauses(live.FeatureCoverage), qt.DeepEquals, []string{
		"DCL_DISPLAY: the domain has a DISPLAY expression, which the domain model cannot describe",
		"DCL_NOTED: the domain has annotations, which the domain model cannot describe",
		"DCL_ORDER: the domain has an ORDER expression, which the domain model cannot describe",
	})
	// The control: a domain with none of the clauses is still described.
	c.Assert(oracleDomainSummary(c, live), qt.Contains, "DCL_PLAIN NUMBER")

	declared := oracleTypesDeclared(c,
		oracleTypeObject(c, oratype.DomainKind, "dcl_display", &oratype.DesiredDomain{BaseType: "VARCHAR2(10)"}))
	_, err = schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	c.Assert(err, qt.ErrorMatches, `(?s).*the domain exists and the read cannot describe it: the domain has a DISPLAY expression.*`)
}

// unrepresentableDomainClauses lists the fixture's domains the read found and
// could not describe, as "<name>: <reason>".
func unrepresentableDomainClauses(coverage schemaext.Coverage) []string {
	var listed []string
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == oratype.DomainKind && record.Knowledge.State == schemaext.Unrepresentable &&
			strings.HasPrefix(record.Subject.Name.Source, "DCL_") {
			listed = append(listed, record.Subject.Name.Source+": "+record.Knowledge.Reason)
		}
	}
	return listed
}

// dropOracleDomainClauseFixture removes the fixture, tolerating what is absent.
func dropOracleDomainClauseFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, name := range []string{"dcl_display", "dcl_order", "dcl_noted", "dcl_plain"} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP DOMAIN "+name)
	}
}
