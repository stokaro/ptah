//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestTimescaleHypertableRoundTripE2E is the acceptance stokaro/ptah#1026's
// second criterion asks for: a hypertable declared, applied, introspected and
// diffed back to zero.
//
// It is live because no offline test can see the failure. Three surfaces have
// to agree — the renderer emits create_hypertable, the reader finds the row it
// made in timescaledb_information, and the comparator finds nothing left to do
// — and when one is missing the result is not a compile error but an apply loop
// that emits the same call forever, or a description that says a partitioned
// table is ordinary.
//
// The capability is asserted rather than assumed. TimescaleDB puts no token in
// version(), so nothing the version resolver reads can see it; what decides the
// key is pg_extension on the connection, and a run against the ordinary
// PostgreSQL service would silently skip every statement below.
func TestTimescaleHypertableRoundTripE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	c.Assert(conn.Info().Capabilities.Has(capability.Hypertables), qt.IsTrue,
		qt.Commentf("the extension is installed and the key must follow the connection, not the banner"))

	// A schema of its own, because the comparison is about a whole schema: any
	// other table in `public` is an object the declaration does not name, and a
	// declaration that does not name a hypertable is a refusal. A shared server
	// makes that a test that fails for somebody else's fixture.
	schemaName := fmt.Sprintf("ht_round_%d", time.Now().UnixNano())
	table := "readings"
	execTimescale(ctx, c, conn, "CREATE SCHEMA "+schemaName)
	defer dropTimescaleSchema(context.WithoutCancel(ctx), conn, schemaName)

	declared := hypertableSchema(schemaName, table, "time", "1 day")

	// 1. Apply what the planner produces. Nothing is hand-written, so a
	//    statement the server refuses fails here rather than being corrected.
	statements := planTimescale(c, conn, declared, schemaName)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		execTimescale(ctx, c, conn, statement)
	}

	// 2. The extension's own catalog is asked what it holds. Nothing else can
	//    answer: pg_class reports relkind 'r' for a hypertable.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(describedHypertableNames(live), qt.Contains, table)
	c.Assert(readHypertable(c, live, table), qt.DeepEquals, catalog.Hypertable{
		Schema: schemaName, Name: table,
		PrimaryDimension: "time", PrimaryDimensionType: "timestamp with time zone",
		ChunkInterval: "1 day", Dimensions: 1,
	})

	// 3. The same declaration now plans nothing, which is the property an apply
	//    loop depends on.
	c.Assert(planTimescale(c, conn, declared, schemaName), qt.HasLen, 0)
}

// TestTimescaleHypertableRefusesWhatTheServerCannotUndoE2E pins the two
// divergences TimescaleDB has no statement for.
//
// Measured on 2.29.2: `drop_hypertable` answers `function drop_hypertable
// (unknown) does not exist`, and there is no call that repartitions an existing
// hypertable either. Planning nothing for those would be worse than refusing —
// the table stays partitioned, the description says otherwise, and an operator
// reading "no changes" believes the two agree.
func TestTimescaleHypertableRefusesWhatTheServerCannotUndoE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ht_refuse_%d", time.Now().UnixNano())
	table := "readings"
	execTimescale(ctx, c, conn, "CREATE SCHEMA "+schemaName)
	defer dropTimescaleSchema(context.WithoutCancel(ctx), conn, schemaName)

	declared := hypertableSchema(schemaName, table, "time", "")
	for _, statement := range planTimescale(c, conn, declared, schemaName) {
		execTimescale(ctx, c, conn, statement)
	}

	tests := []struct {
		name    string
		declare func() *schemamodel.Database
		want    string
	}{
		{
			name: "the declaration stops naming it",
			declare: func() *schemamodel.Database {
				schema := hypertableSchema(schemaName, table, "time", "")
				schema.Hypertables = nil
				return schema
			},
			want: "TimescaleDB has no statement that turns a hypertable back into an ordinary table",
		},
		{
			name: "the declaration moves the dimension",
			declare: func() *schemamodel.Database {
				return hypertableSchema(schemaName, table, "device", "")
			},
			want: "TimescaleDB has no statement that repartitions an existing hypertable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := planTimescaleReportingError(c, conn, test.declare(), schemaName)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
		})
	}
}

// hypertableSchema declares one table and asks for it to be partitioned.
func hypertableSchema(schemaName, table, column, interval string) *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: schemaName}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: table, Schema: schemaName}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "time", Type: "TIMESTAMPTZ"},
			{StructName: "T", Name: "device", Type: "INTEGER"},
			{StructName: "T", Name: "value", Type: "INTEGER"},
		},
		Extensions: []schemamodel.Extension{{Name: "timescaledb", IfNotExists: true}},
		Hypertables: []schemamodel.Hypertable{{
			StructName: "T", Table: schemaName + "." + table, Column: column,
			ChunkInterval: interval, IfNotExists: true,
		}},
	}
}

// readHypertable picks the one row this test is about, so the assertion is a
// comparison rather than a loop with a filter in it.
func readHypertable(
	c *qt.C,
	schema *catalog.Database,
	table string,
) catalog.Hypertable {
	c.Helper()
	for _, hypertable := range schema.Hypertables {
		if hypertable.Name == table {
			return hypertable
		}
	}
	c.Fatalf("the read carries no hypertable named %s", table)
	return catalog.Hypertable{}
}

// dropTimescaleSchema removes the schema a test worked in, and everything it
// put there.
func dropTimescaleSchema(ctx context.Context, conn *dbschema.DatabaseConnection, schemaName string) {
	_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
}

// planTimescale plans the declaration against the live database and fails the
// test on a planning error.
func planTimescale(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
	schemaName string,
) []string {
	c.Helper()
	statements, err := planTimescaleReportingError(c, conn, declared, schemaName)
	c.Assert(err, qt.IsNil)
	return statements
}

// planTimescaleReportingError is the same, for the refusals that are the point.
func planTimescaleReportingError(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
	schemaName string,
) ([]string, error) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil)
	c.Assert(err, qt.IsNil)

	return planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, conn.Info().Dialect, planner.Options{Capabilities: conn.Info().Capabilities})
}
