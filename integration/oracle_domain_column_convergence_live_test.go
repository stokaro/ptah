//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestOracleDomainColumnsConvergeLive pins that a table whose columns a domain
// types plans nothing once it is applied, and that a read of it agrees with
// itself.
//
// The column catalog reports such a column by its base type, NOT NULL where
// the domain is, with the domain's default, and with a copy of each domain
// constraint on the table. Compared as they stand, every plan after the first
// carried `MODIFY (addr dom_email NULL)`, `MODIFY (score dom_score)` and the
// drop of the copied CHECK, measured on 23.26.3.0.0.
func TestOracleDomainColumnsConvergeLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleDomainColumnFixture(context.WithoutCancel(ctx), conn)
	defer dropOracleDomainColumnFixture(context.WithoutCancel(ctx), conn)

	declared := oracleDomainColumnDeclaration(c)
	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	created := planOracleDomainColumns(ctx, c, conn, declared, before)
	c.Assert(created, qt.Not(qt.HasLen), 0)
	for _, statement := range created {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	// Non-vacuity: the columns really are typed by their domains on the
	// server, so the empty plan below compares domain columns.
	c.Assert(oracleColumnDomains(after, "DCC_ACCOUNTS"), qt.DeepEquals, map[string]string{
		"ADDR": "DCC_EMAIL", "LOGIN": "DCC_EMAIL", "QTY": "DCC_QTY", "SCORE": "DCC_SCORE",
	})
	c.Assert(planOracleDomainColumns(ctx, c, conn, declared, after), qt.HasLen, 0)

	// The read agrees with itself, which is what `schema inspect` followed by
	// `schema apply` relies on.
	described, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, after, platform.Oracle, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(planOracleDomainColumns(ctx, c, conn, described, after), qt.HasLen, 0)
}

// oracleDomainColumnDeclaration declares a NOT NULL domain, a CHECK domain and
// a domain with a default, and a table whose columns they type. LOGIN states
// its own NOT NULL as well, which Oracle keeps as a constraint of the column.
func oracleDomainColumnDeclaration(c *qt.C) *schemamodel.Database {
	declared := oracleTypesDeclared(c,
		oracleTypeObject(c, oratype.DomainKind, "dcc_email", &oratype.DesiredDomain{BaseType: "VARCHAR2(255)", NotNull: true}),
		oracleTypeObject(c, oratype.DomainKind, "dcc_score", &oratype.DesiredDomain{BaseType: "NUMBER(5,2)", Check: "VALUE BETWEEN 0 AND 100"}),
		oracleTypeObject(c, oratype.DomainKind, "dcc_qty", &oratype.DesiredDomain{BaseType: "NUMBER(10)", Default: "5"}),
	)
	declared.Tables = []schemamodel.Table{{StructName: "T", Name: "dcc_accounts"}}
	declared.Fields = []schemamodel.Field{
		{StructName: "T", Name: "id", Type: "NUMBER(10)", Primary: true},
		{StructName: "T", Name: "addr", Type: "dcc_email", Nullable: true},
		{StructName: "T", Name: "login", Type: "dcc_email"},
		{StructName: "T", Name: "score", Type: "dcc_score", Nullable: true},
		{StructName: "T", Name: "qty", Type: "dcc_qty", Nullable: true},
	}
	return declared
}

// planOracleDomainColumns plans declared against read and returns the
// statements that name the fixture's objects. The schema is shared, and an
// object another test left behind is not this test's to plan or to drop.
func planOracleDomainColumns(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database,
	read *catalog.Database,
) []string {
	c.Helper()
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, read, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	var fixture []string
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), "DCC_") {
			fixture = append(fixture, statement)
		}
	}
	return fixture
}

// oracleColumnDomains maps each column of table a domain types to the domain.
func oracleColumnDomains(read *catalog.Database, table string) map[string]string {
	domains := make(map[string]string)
	for _, candidate := range read.Tables {
		if candidate.Name != table {
			continue
		}
		for _, column := range candidate.Columns {
			if column.DomainName != "" {
				domains[column.Name] = column.DomainName
			}
		}
	}
	return domains
}

// dropOracleDomainColumnFixture removes what the test creates, the table
// first, tolerating what is absent.
func dropOracleDomainColumnFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{
		"DROP TABLE dcc_accounts PURGE", "DROP DOMAIN dcc_email", "DROP DOMAIN dcc_score", "DROP DOMAIN dcc_qty",
	} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}
