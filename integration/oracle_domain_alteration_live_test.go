//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"ptah.run/core/planext"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestOracleDomainAlterationIsRefusedLive pins what a plan does with a domain
// whose base type or NOT NULL changed: it refuses, naming the domain.
//
// Oracle has no statement for either change. Measured on 23.26.3.0.0, ALTER
// DOMAIN changes DISPLAY, ORDER and annotations, and answers ORA-00904 or
// ORA-03050 to every spelling of a new base type, NOT NULL, DEFAULT or
// CHECK. DROP DOMAIN answers ORA-11502 while a column uses the domain. The
// plan wrote nothing for the change, so the declaration read as applied while
// the domain kept its old definition.
func TestOracleDomainAlterationIsRefusedLive(t *testing.T) {
	tests := []struct {
		name   string
		domain *oratype.DesiredDomain
		want   string
	}{
		{name: "a base type", domain: &oratype.DesiredDomain{BaseType: "VARCHAR2(255)"},
			want: `(?s).*oracle: domain dal_email: Oracle cannot change its base type in place, .*`},
		{name: "NOT NULL", domain: &oratype.DesiredDomain{BaseType: "VARCHAR2(100)", NotNull: true},
			want: `(?s).*oracle: domain dal_email: Oracle cannot change its NOT NULL in place, .*`},
	}

	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleDomainAlterationFixture(context.WithoutCancel(ctx), conn)
	defer dropOracleDomainAlterationFixture(context.WithoutCancel(ctx), conn)
	execOracle(ctx, c, conn, "CREATE DOMAIN dal_email AS VARCHAR2(100)")
	execOracle(ctx, c, conn, "CREATE TABLE dal_users (id NUMBER(10) PRIMARY KEY, addr dal_email)")
	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := oracleTypesDeclared(c, oracleTypeObject(c, oratype.DomainKind, "dal_email", test.domain))
			declared.Tables = []schemamodel.Table{{StructName: "T", Name: "dal_users"}}
			declared.Fields = []schemamodel.Field{
				{StructName: "T", Name: "id", Type: "NUMBER(10)", Primary: true},
				{StructName: "T", Name: "addr", Type: "dal_email", Nullable: true},
			}

			diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// TestOracleDomainBaseTypeSpellingsConvergeLive pins that a domain declared
// in a spelling Oracle does not keep plans nothing once it is created, so the
// refusal above fires only on a real change.
//
// ALL_DOMAIN_COLS reports a base type in its own spelling: INTEGER as
// NUMBER(*,0), TIMESTAMP as TIMESTAMP(6), FLOAT as FLOAT(126), and BOOLEAN as
// UNDEFINED. VARCHAR2(10 CHAR) reports a DATA_LENGTH of 40. Compared as
// written, nine of these spellings planned a base type change on every run,
// measured on 23.26.3.0.0.
func TestOracleDomainBaseTypeSpellingsConvergeLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	spellings := oracleDomainSpellings()
	dropOracleDomainSpellings(context.WithoutCancel(ctx), conn, spellings)
	defer dropOracleDomainSpellings(context.WithoutCancel(ctx), conn, spellings)
	declared := oracleDomainSpellingDeclaration(c, spellings)

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	created := planOracleDomainSpellings(ctx, c, conn, declared, before)
	c.Assert(created, qt.HasLen, len(spellings))
	for _, statement := range created {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(planOracleDomainSpellings(ctx, c, conn, declared, after), qt.HasLen, 0)
}

// oracleDomainSpellings maps each fixture domain to the base type it is
// declared with.
func oracleDomainSpellings() map[string]string {
	return map[string]string{
		"dsp_integer": "INTEGER", "dsp_smallint": "SMALLINT", "dsp_numeric": "NUMERIC(5,2)",
		"dsp_decimal": "DECIMAL(10,2)", "dsp_number": "NUMBER(10,0)", "dsp_float": "FLOAT", "dsp_real": "REAL",
		"dsp_double": "DOUBLE PRECISION", "dsp_ts": "TIMESTAMP", "dsp_tstz": "TIMESTAMP WITH TIME ZONE",
		"dsp_tsltz": "TIMESTAMP WITH LOCAL TIME ZONE", "dsp_ids": "INTERVAL DAY TO SECOND",
		"dsp_iym": "INTERVAL YEAR TO MONTH", "dsp_varchar": "VARCHAR(10)", "dsp_bytes": "VARCHAR2(10 BYTE)",
		"dsp_chars": "VARCHAR2(10 CHAR)", "dsp_char": "CHAR", "dsp_fixed": "CHAR(3 CHAR)", "dsp_bool": "BOOLEAN",
		"dsp_date": "DATE", "dsp_clob": "CLOB", "dsp_raw": "RAW(16)", "dsp_binary": "BINARY_DOUBLE",
		"dsp_national": "NVARCHAR2(10)",
	}
}

// oracleDomainSpellingDeclaration declares every fixture domain.
func oracleDomainSpellingDeclaration(c *qt.C, spellings map[string]string) *schemamodel.Database {
	c.Helper()
	objects := make([]schemaext.Object, 0, len(spellings))
	for name, baseType := range spellings {
		objects = append(objects, oracleTypeObject(c, oratype.DomainKind, name, &oratype.DesiredDomain{BaseType: baseType}))
	}
	return oracleTypesDeclared(c, objects...)
}

// planOracleDomainSpellings plans declared against read and returns the
// statements that name a fixture domain. The schema is shared, and an object
// another test left behind is not this test's to plan.
func planOracleDomainSpellings(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database,
	read *catalog.Database,
) []string {
	c.Helper()
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, read, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	var fixture []string
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), "DSP_") {
			fixture = append(fixture, statement)
		}
	}
	return fixture
}

// dropOracleDomainSpellings removes the fixture domains, tolerating what is
// absent.
func dropOracleDomainSpellings(ctx context.Context, conn *dbschema.DatabaseConnection, spellings map[string]string) {
	for name := range spellings {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP DOMAIN "+name)
	}
}

// dropOracleDomainAlterationFixture removes what the test creates, the table
// first, tolerating what is absent.
func dropOracleDomainAlterationFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{"DROP TABLE dal_users PURGE", "DROP DOMAIN dal_email"} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}
