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
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestOracleObjectTypeAttributeSpellingsConvergeLive pins that an object type
// declared with attributes in spellings Oracle does not keep plans nothing
// once it is created, while a table uses it.
//
// ALL_TYPE_ATTRS keeps some aliases and replaces others: NUMERIC(5,2) comes
// back as DECIMAL, VARCHAR(10) as VARCHAR2, NUMBER(10,0) as NUMBER(10), and the
// time zone forms as TIMESTAMP WITH TZ. TIMESTAMP(3) and VARCHAR2(10 CHAR)
// lost their precision and their unit in the read. Compared as written, the
// second plan carried CREATE OR REPLACE TYPE, which Oracle refuses with
// ORA-02303 while the table uses the type, measured on 23.26.3.0.0.
func TestOracleObjectTypeAttributeSpellingsConvergeLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleAttributeSpellingFixture(context.WithoutCancel(ctx), conn)
	defer dropOracleAttributeSpellingFixture(context.WithoutCancel(ctx), conn)
	declared := oracleAttributeSpellingDeclaration(c)

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	created := planOracleAttributeSpellings(ctx, c, conn, declared, before)
	c.Assert(created, qt.HasLen, 2)
	for _, statement := range created {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(planOracleAttributeSpellings(ctx, c, conn, declared, after), qt.HasLen, 0)
}

// oracleAttributeSpellingDeclaration declares an object type with one
// attribute per spelling, and a table with a column of the type.
func oracleAttributeSpellingDeclaration(c *qt.C) *schemamodel.Database {
	c.Helper()
	spellings := []string{
		"INTEGER", "INT", "SMALLINT", "NUMERIC", "NUMERIC(5,2)", "DECIMAL(10,2)", "DECIMAL(5)", "NUMBER(10,0)",
		"NUMBER", "FLOAT", "REAL", "DOUBLE PRECISION", "FLOAT(10)", "TIMESTAMP", "TIMESTAMP(3)",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", "INTERVAL DAY TO SECOND",
		"INTERVAL DAY(3) TO SECOND(2)", "INTERVAL YEAR TO MONTH", "VARCHAR(10)", "VARCHAR2(10 BYTE)",
		"VARCHAR2(10 CHAR)", "CHAR", "CHAR(3 CHAR)", "NCHAR(3)", "NVARCHAR2(10)", "BOOLEAN", "DATE", "CLOB",
		"RAW(16)", "BINARY_DOUBLE",
	}
	attributes := make([]oratype.Attribute, 0, len(spellings))
	for i, spelling := range spellings {
		attributes = append(attributes, oratype.Attribute{Name: "a" + string(rune('a'+i/26)) + string(rune('a'+i%26)), Type: spelling})
	}
	declared := oracleTypesDeclared(c,
		oracleTypeObject(c, oratype.ObjectTypeKind, "oas_record", &oratype.DesiredObjectType{Attributes: attributes}))
	declared.Tables = []schemamodel.Table{{StructName: "T", Name: "oas_holders"}}
	declared.Fields = []schemamodel.Field{
		{StructName: "T", Name: "id", Type: "NUMBER(10)", Primary: true},
		{StructName: "T", Name: "rec", Type: "oas_record", Nullable: true},
	}
	return declared
}

// planOracleAttributeSpellings plans declared against read and returns the
// statements that name the fixture. The schema is shared, and an object
// another test left behind is not this test's to plan.
func planOracleAttributeSpellings(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database,
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
		if strings.Contains(strings.ToUpper(statement), "OAS_") {
			fixture = append(fixture, statement)
		}
	}
	return fixture
}

// dropOracleAttributeSpellingFixture removes what the test creates, the table
// first, tolerating what is absent.
func dropOracleAttributeSpellingFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{"DROP TABLE oas_holders PURGE", "DROP TYPE oas_record"} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}
