//go:build integration

package integration_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"ptah.run/core/planext"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

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

// TestOracleCompositeTypesPlanAndConvergeE2E is the assertion the
// composite_types capability key could not be flipped without.
//
// The key promises that a declared composite is planned, rendered, introspected
// and compared, and for this object the failure without it is silent rather
// than loud. PostgreSQL's spelling is ACCEPTED by Oracle: measured on
// 23.26.2.0.0 through go-ora, `CREATE TYPE t AS (a NUMBER, b VARCHAR2(10))`
// returns no error and leaves USER_TYPES reporting ATTRIBUTES 0 with
// INCOMPLETE YES and USER_OBJECTS reporting INVALID. Only
// `CREATE TYPE t AS OBJECT (...)` creates the type, which is why this asserts
// the CATALOG rather than the absence of an error (stokaro/ptah#1920).
func TestOracleCompositeTypesPlanAndConvergeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleComposites(context.WithoutCancel(ctx), conn)
	defer dropOracleComposites(context.WithoutCancel(ctx), conn)

	declared := oracleCompositeDeclaration(c)

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, before, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)

	creates := oracleStatementsNaming(statements, "CREATE OR REPLACE TYPE")
	c.Assert(creates, qt.HasLen, 2)
	for _, statement := range creates {
		// The spelling is the assertion. PostgreSQL's `AS (` reaches the server
		// without an error and creates a shell.
		c.Assert(statement, qt.Contains, "AS OBJECT (")
	}
	for _, statement := range statements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleCompositeSummary(c, after), qt.DeepEquals, []string{
		"ORA_ADDR(STREET VARCHAR2(100), ZIP VARCHAR2(10))",
		"ORA_POINT(X NUMBER(10,2), Y NUMBER(10,2))",
	})

	settled, err := schemadiff.CompareWithDatabase(ctx, conn, declared, after, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(settled.FeatureChanges, qt.HasLen, 0)

	// And the removal direction, whose ordering matters: dropping a type a
	// column still uses answers ORA-02303.
	teardown, err := schemadiff.CompareWithDatabase(ctx, conn, oracleTypesDeclared(c), after, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	teardownStatements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		teardown, platform.Oracle, planext.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleFirstIndexOf(c, teardownStatements, "DROP TABLE") <
		oracleFirstIndexOf(c, teardownStatements, "DROP TYPE"), qt.IsTrue)
	for _, statement := range teardownStatements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	empty, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleCompositeSummary(c, empty), qt.HasLen, 0)
}

// TestOracleCompositeReaderDeclinesWhatTheModelCannotCarryE2E pins the shapes
// the read declines, against types the server really holds.
//
// Each one is a shape the model would describe wrongly rather than not at all:
// an object type with a method, the NOT FINAL root of a hierarchy and its
// subtype, a collection, and the incomplete shell PostgreSQL's own spelling
// leaves behind. Describing any of them by an attribute list would say a
// replay produces the same type.
func TestOracleCompositeReaderDeclinesWhatTheModelCannotCarryE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleDeclinedTypes(context.WithoutCancel(ctx), conn)
	defer dropOracleDeclinedTypes(context.WithoutCancel(ctx), conn)

	for _, statement := range []string{
		"CREATE OR REPLACE TYPE ora_kept AS OBJECT (a NUMBER)",
		"CREATE OR REPLACE TYPE ora_method AS OBJECT (a NUMBER, MEMBER FUNCTION doubled RETURN NUMBER)",
		"CREATE OR REPLACE TYPE ora_parent AS OBJECT (a NUMBER) NOT FINAL",
		"CREATE OR REPLACE TYPE ora_child UNDER ora_parent (b NUMBER)",
		"CREATE OR REPLACE TYPE ora_list AS VARRAY(4) OF NUMBER",
		// The PostgreSQL spelling, which the server accepts and leaves broken.
		"CREATE TYPE ora_shell AS (a NUMBER)",
	} {
		execOracle(ctx, c, conn, statement)
	}

	schema, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// ORA_PARENT has no methods and no supertype, and is declined for being
	// NOT FINAL: a replay of its attributes creates a FINAL type, under which
	// ORA_CHILD could not be created again.
	c.Assert(oracleCompositeNames(c, schema), qt.DeepEquals, []string{"ORA_KEPT"})
}

// oracleCompositeDeclaration declares two object types and a table typed by
// one.
func oracleCompositeDeclaration(c *qt.C) *schemamodel.Database {
	declared := oracleTypesDeclared(c,
		oracleTypeObject(c, oratype.ObjectTypeKind, "ora_point", &oratype.DesiredObjectType{Attributes: []oratype.Attribute{
			{Name: "x", Type: "NUMBER(10,2)"},
			{Name: "y", Type: "NUMBER(10,2)"},
		}}),
		oracleTypeObject(c, oratype.ObjectTypeKind, "ora_addr", &oratype.DesiredObjectType{Attributes: []oratype.Attribute{
			{Name: "street", Type: "VARCHAR2(100)"},
			{Name: "zip", Type: "VARCHAR2(10)"},
		}}),
	)
	declared.Tables = []schemamodel.Table{{StructName: "T", Name: "ora_places"}}
	declared.Fields = []schemamodel.Field{
		{StructName: "T", Name: "id", Type: "INT", Primary: true},
		{StructName: "T", Name: "at", Type: "ora_point"},
	}
	return declared
}

func dropOracleComposites(ctx context.Context, conn *dbschema.DatabaseConnection) {
	_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP TABLE ora_places PURGE")
	for _, name := range []string{"ora_point", "ora_addr"} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP TYPE "+name)
	}
}

func dropOracleDeclinedTypes(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, name := range []string{
		"ora_child", "ora_parent", "ora_method", "ora_list", "ora_shell", "ora_kept",
	} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP TYPE "+name)
	}
}

// oracleCompositeSummary renders each read object type in one line, sorted.
func oracleCompositeSummary(c *qt.C, read *catalog.Database) []string {
	c.Helper()
	var summary []string
	for name, attributes := range oracleObjectTypes(c, read) {
		fields := make([]string, 0, len(attributes))
		for _, attribute := range attributes {
			fields = append(fields, attribute.Name+" "+attribute.Type)
		}
		summary = append(summary, name+"("+strings.Join(fields, ", ")+")")
	}
	sort.Strings(summary)
	return summary
}

func oracleCompositeNames(c *qt.C, read *catalog.Database) []string {
	c.Helper()
	var names []string
	for name := range oracleObjectTypes(c, read) {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// oracleObjectTypes is the attributes of each object type a read holds, by
// name.
func oracleObjectTypes(c *qt.C, read *catalog.Database) map[string][]oratype.Attribute {
	c.Helper()
	objects, err := read.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	types := make(map[string][]oratype.Attribute)
	for _, object := range objects {
		if objectType, ok := object.Value.(*oratype.ObservedObjectType); ok {
			types[object.Ref.Name.Source] = objectType.Attributes
		}
	}
	return types
}
