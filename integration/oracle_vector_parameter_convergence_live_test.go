//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestOracleVectorDeclarationConvergesLive is the first half of the rule the
// comparator's type fold has to keep: a declaration carrying a parameterized
// type agrees with the column it built.
//
// The declaration says VECTOR(512, INT8) and the server answers
// VECTOR(512,INT8,DENSE) -- it fills in the storage form the author left out
// and drops the space. Neither completion is something an offline test can
// know: the dimension, the element format and the storage form come back in
// ALL_TAB_COLS.VECTOR_INFO, and what Oracle writes there is the measurement.
// So this is the row that says the completed spelling the declared side folds
// to is still the spelling this server reports.
func TestOracleVectorDeclarationConvergesLive(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.Oracle)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	// A leftover table from an earlier run reads back as a table the
	// declaration does not declare, and the convergence assertion below would
	// fail against a defect that is not there.
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()

	declared := oracleVectorDeclaration("VECTOR(512, INT8)")
	applyOracleVectorDeclaration(ctx, c, conn, declared)

	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	embedding := oracleColumnByName(c,
		oracleTableByName(c, read.Tables, "ORA_VECTORS").Columns, "EMBEDDING")
	c.Assert(embedding.DataType, qt.Equals, "VECTOR(512,INT8,DENSE)")

	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, read, config.DefaultCompareOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDiffSummary(diff), qt.DeepEquals, []string(nil))
}

// TestOracleVectorParameterChangeIsReportedLive is the destructive half, and
// the half convergence alone cannot measure.
//
// A fold that reads the whole spelling rather than the type's name finds a
// token inside the parameter list that names a type elsewhere -- INT8 is one --
// and answers with that token's own type. Both sides of a comparison then
// collapse onto "integer": the declared VECTOR(512,INT8,DENSE) and the catalog's
// VECTOR(768,INT8,DENSE) become the same type, and a dimension change is a
// change the comparator never reports. The convergence test above stays green
// through exactly that defect, because two sides that collapse onto one token
// also agree with each other.
//
// The third row is the same error seen from the other end, and it is what makes
// the miss destructive rather than cosmetic: a NUMBER(19) column that has to
// become a vector already normalizes to "integer", so a declaration folded onto
// its own INT8 parameter equals it, and the column stays a number through every
// plan with nothing in the output to read.
//
// Each row states the applied type and the desired one, so the want is the
// comparator's own report rather than a count.
func TestOracleVectorParameterChangeIsReportedLive(t *testing.T) {
	tests := []struct {
		name    string
		applied string
		desired string
		want    string
	}{
		{
			name:    "the dimension changes, both sides carrying int8 elements",
			applied: "VECTOR(512, INT8)",
			desired: "VECTOR(768, INT8)",
			want: "column modified: ora_vectors.embedding " +
				"map[type:vector(512,int8,dense) -> vector(768,int8,dense)]",
		},
		{
			name:    "the element format changes away from int8",
			applied: "VECTOR(512, INT8)",
			desired: "VECTOR(512, FLOAT32)",
			want: "column modified: ora_vectors.embedding " +
				"map[type:vector(512,int8,dense) -> vector(512,float32,dense)]",
		},
		{
			name:    "a number column has to become a vector",
			applied: "BIGINT",
			desired: "VECTOR(512, INT8)",
			want: "column modified: ora_vectors.embedding " +
				"map[type:number(19) -> vector(512,int8,dense)]",
		},
	}

	parent := qt.New(t)
	dbURL := dbtarget.URL(parent, dbtarget.Oracle)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	parent.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	defer func() {
		parent.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
			applyOracleVectorDeclaration(ctx, c, conn, oracleVectorDeclaration(tt.applied))

			read, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			// The one edit, on the declaration only.
			desired := oracleVectorDeclaration(tt.desired)
			diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, read, config.DefaultCompareOptions())
			c.Assert(err, qt.IsNil)
			c.Assert(oracleDiffSummary(diff), qt.DeepEquals, []string{tt.want})
		})
	}
}

// oracleVectorDeclaration is a one-table schema whose only non-key column
// carries the type under test.
//
// It is deliberately smaller than the convergence declaration beside it: every
// other column would contribute its own row to the reported summary, and a
// summary carrying more than the column under test says nothing clearer about
// the type fold.
func oracleVectorDeclaration(columnType string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Embedding", Name: "ora_vectors"}},
		Fields: []schemamodel.Field{
			{StructName: "Embedding", Name: "id", Type: "INT", Primary: true},
			{StructName: "Embedding", Name: "embedding", Type: columnType, Nullable: true},
		},
	}
}

// applyOracleVectorDeclaration renders the declaration through the Oracle
// renderer and executes it, one statement at a time.
//
// The renderer is asked rather than a hand-written CREATE TABLE because the
// declared type has to reach the server through the same mapping the comparison
// folds with; a hand-written vector column would prove the reader and the
// comparator agree while saying nothing about what Ptah writes.
func applyOracleVectorDeclaration(
	ctx context.Context,
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
) {
	c.Helper()

	statements, err := renderer.RenderSQLWithCapabilities(
		platform.Oracle,
		capability.ForServerVersion(platform.Oracle, conn.Info().Version),
		oracleConvergenceNodes(declared)...,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.Equals), "")

	for _, statement := range splitOracleStatements(statements) {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
			qt.Commentf("statement: %s", statement))
	}
}
