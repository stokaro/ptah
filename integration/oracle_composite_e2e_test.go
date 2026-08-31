//go:build integration

package integration_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
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

	declared := oracleCompositeDeclaration()

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, before, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
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
	c.Assert(oracleCompositeSummary(after), qt.DeepEquals, []string{
		"ORA_ADDR(STREET VARCHAR2(100), ZIP VARCHAR2(10))",
		"ORA_POINT(X NUMBER(10,2), Y NUMBER(10,2))",
	})

	settled, err := schemadiff.CompareWithDatabase(ctx, conn, declared, after, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(settled.CompositeTypesAdded, qt.HasLen, 0)
	c.Assert(settled.CompositeTypesRemoved, qt.HasLen, 0)
	c.Assert(settled.CompositeTypesModified, qt.HasLen, 0)

	// And the removal direction, whose ordering matters: dropping a type a
	// column still uses answers ORA-02303.
	teardown, err := schemadiff.CompareWithDatabase(ctx, conn, &schemamodel.Database{}, after, nil)
	c.Assert(err, qt.IsNil)
	teardownStatements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(teardown, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)
	c.Assert(oracleFirstIndexOf(c, teardownStatements, "DROP TABLE") <
		oracleFirstIndexOf(c, teardownStatements, "DROP TYPE"), qt.IsTrue)
	for _, statement := range teardownStatements {
		execOracle(ctx, c, conn, strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	}

	empty, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleCompositeSummary(empty), qt.HasLen, 0)
}

// TestOracleCompositeReaderDeclinesWhatTheModelCannotCarryE2E pins the four
// predicates the read applies, against types the server really holds.
//
// Each one is a shape the model would describe wrongly rather than not at all:
// an object type with a method, a subtype, a collection, and the incomplete
// shell PostgreSQL's own spelling leaves behind. Describing any of them by an
// attribute list would say a replay produces the same type.
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

	// ORA_PARENT survives on its own merits -- it has no methods and no
	// supertype -- which is what makes the ORA_CHILD row a statement about
	// subtypes rather than about that pair.
	c.Assert(oracleCompositeNames(schema), qt.DeepEquals, []string{"ORA_KEPT", "ORA_PARENT"})
}

// oracleCompositeDeclaration declares two composites and a table typed by one.
func oracleCompositeDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		CompositeTypes: []schemamodel.CompositeType{
			{
				StructName: "P", Name: "ora_point",
				Fields: []schemamodel.CompositeField{
					{Name: "x", Type: "NUMBER(10,2)"},
					{Name: "y", Type: "NUMBER(10,2)"},
				},
			},
			{
				StructName: "A", Name: "ora_addr",
				Fields: []schemamodel.CompositeField{
					{Name: "street", Type: "VARCHAR2(100)"},
					{Name: "zip", Type: "VARCHAR2(10)"},
				},
			},
		},
		Tables: []schemamodel.Table{{StructName: "T", Name: "ora_places"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "at", Type: "ora_point"},
		},
	}
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

// oracleCompositeSummary renders each read composite in one line, sorted.
func oracleCompositeSummary(read *catalog.Database) []string {
	summary := make([]string, 0, len(read.Composites))
	for _, composite := range read.Composites {
		fields := make([]string, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			fields = append(fields, field.Name+" "+field.Type)
		}
		summary = append(summary, composite.Name+"("+strings.Join(fields, ", ")+")")
	}
	sort.Strings(summary)
	return summary
}

func oracleCompositeNames(read *catalog.Database) []string {
	names := make([]string, 0, len(read.Composites))
	for _, composite := range read.Composites {
		names = append(names, composite.Name)
	}
	sort.Strings(names)
	return names
}
