//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/timescale"
)

// TestTimescaleUndescribedObjectsE2E is the live acceptance for the note that
// tells an operator what a TimescaleDB description leaves out
// (stokaro/ptah#1026).
//
// The failure it closes is silent by construction. A hypertable is an ordinary
// PostgreSQL table everywhere Ptah looks: pg_class reports relkind 'r',
// pg_depend reports no extension ownership, and the rendered CREATE TABLE is
// exactly what the columns say. Replayed, it produces a table that is not
// partitioned, and a diff between the two reports no difference -- so the
// description is wrong in a way only the extension's own catalog can name.
//
// Both objects are created here rather than one, because only ONE of them is
// still incomplete: the hypertable has a second dimension no declaration
// carries, and the continuous aggregate beside it is described in full. The
// aggregate is what keeps the note honest -- a note that named everything
// TimescaleDB owns would name it too.
func TestTimescaleUndescribedObjectsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := "ts_conditions_" + suffix
	aggregate := "ts_hourly_" + suffix
	dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, aggregate)
	defer dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, aggregate)

	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE TABLE %s ("time" TIMESTAMPTZ NOT NULL, device TEXT NOT NULL, temperature DOUBLE PRECISION)`,
		table))
	execTimescale(ctx, c, conn, fmt.Sprintf(
		`SELECT create_hypertable('%s', by_range('time'))`, table))
	// A SECOND dimension, because that is what the note is about now. One range
	// dimension is what a declaration carries, so a description of it is
	// complete and there is nothing to report; `add_dimension` adds what no
	// declaration can say (stokaro/ptah#1026).
	execTimescale(ctx, c, conn, fmt.Sprintf(
		`SELECT add_dimension('%s', by_hash('device', 4))`, table))
	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous) AS `+
			`SELECT time_bucket('1 hour', "time") AS bucket, device, avg(temperature) AS avg_temp `+
			`FROM %s GROUP BY bucket, device WITH NO DATA`,
		aggregate, table))

	schema, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The hypertable is in the description, and so is its primary dimension.
	// The note is about the SECOND one, and asserting the first here is what
	// keeps this test honest: a note about an incomplete description would be
	// pointless if the description were empty.
	c.Assert(describedTableNames(schema), qt.Contains, table)
	c.Assert(describedHypertableNames(schema), qt.Contains, table)

	// The continuous aggregate is in NEITHER the views nor the materialized
	// views, and is reported on its own list. A description that carried it as
	// a view would render CREATE VIEW over a body naming a relation in a schema
	// the extension owns.
	c.Assert(describedViewNames(schema), qt.Not(qt.Contains), aggregate)
	c.Assert(describedMatViewNames(schema), qt.Not(qt.Contains), aggregate)
	c.Assert(describedAggregateNames(schema), qt.Contains, aggregate)

	var out bytes.Buffer
	timescale.ReportUndescribed(&out, schema)

	c.Assert(out.String(), qt.Contains, "described with the first partitioning dimension only")
	c.Assert(out.String(), qt.Contains, table+" (on time and 1 more dimension)")
	// The aggregate earns no note. It has a declaration of its own now, so the
	// description carries it and there is nothing to warn about -- a note here
	// would send an operator looking for what is missing from a document that
	// has it (stokaro/ptah#1026).
	c.Assert(out.String(), qt.Not(qt.Contains), aggregate)
}

// TestTimescaleReportFollowsInspectSelectionE2E pins WHERE the note is emitted,
// which is a different question from what it says.
//
// `schema inspect` applies its selection after the read, so a note emitted from
// the unscoped schema names objects the document does not contain: `--exclude`
// on the hypertable removes its table from the rendering while the note still
// says the description carries it incompletely, sending the reader to look for
// a statement that is not there. The SQLite virtual-table note was moved after
// the projection for exactly this reason (stokaro/ptah#1028), and this one is
// emitted beside it.
//
// It runs through the command rather than the reader, because the call ORDER is
// what is under test and only the command has one.
func TestTimescaleReportFollowsInspectSelectionE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := "ts_selected_" + suffix
	dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, "")
	defer dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, "")

	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE TABLE %s ("time" TIMESTAMPTZ NOT NULL, v INTEGER NOT NULL)`, table))
	execTimescale(ctx, c, conn, fmt.Sprintf(`SELECT create_hypertable('%s', by_range('time'))`, table))
	execTimescale(ctx, c, conn, fmt.Sprintf(`SELECT add_dimension('%s', by_hash('v', 4))`, table))

	named, err := runAtlasCompat("schema", "inspect", "--url", dbURL)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", named))
	c.Assert(named, qt.Contains, table)

	excluded, err := runAtlasCompat("schema", "inspect", "--url", dbURL, "--exclude", table)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", excluded))
	// The table is gone from the document AND from the note. Asserting only the
	// document would pass with the note still naming it, which is the defect.
	c.Assert(excluded, qt.Not(qt.Contains), table)
}

// TestTimescaleReportIsSilentOnOrdinaryPostgresE2E is the control the note
// needs, and it runs against the OTHER service on purpose.
//
// A note that fired on every PostgreSQL server would pass every assertion above
// and be noise on every inspection anyone runs. The ordinary PostgreSQL service
// has no TimescaleDB, so the read asks its catalog nothing and the note has
// nothing to say.
func TestTimescaleReportIsSilentOnOrdinaryPostgresE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schema, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	var out bytes.Buffer
	timescale.ReportUndescribed(&out, schema)

	c.Assert(out.String(), qt.Equals, "")
	c.Assert(schema.Hypertables, qt.HasLen, 0)
}

func execTimescale(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, statement string) {
	c.Helper()
	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
		qt.Commentf("statement: %s", statement))
}

// dropTimescaleFixture removes what this test creates. The aggregate goes
// first: a hypertable with a continuous aggregate on it cannot be dropped
// without CASCADE, and CASCADE on a shared server is not something a test
// should reach for.
func dropTimescaleFixture(ctx context.Context, conn *dbschema.DatabaseConnection, table, aggregate string) {
	if aggregate != "" {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP MATERIALIZED VIEW IF EXISTS "+aggregate)
	}
	_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+table)
}

func describedTableNames(schema *catalog.Database) []string {
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, strings.ToLower(table.Name))
	}
	return names
}

func describedHypertableNames(schema *catalog.Database) []string {
	names := make([]string, 0, len(schema.Hypertables))
	for _, hypertable := range schema.Hypertables {
		names = append(names, strings.ToLower(hypertable.Name))
	}
	return names
}

func describedViewNames(schema *catalog.Database) []string {
	names := make([]string, 0, len(schema.Views))
	for _, view := range schema.Views {
		names = append(names, strings.ToLower(view.Name))
	}
	return names
}

func describedMatViewNames(schema *catalog.Database) []string {
	names := make([]string, 0, len(schema.MatViews))
	for _, view := range schema.MatViews {
		names = append(names, strings.ToLower(view.Name))
	}
	return names
}

func describedAggregateNames(schema *catalog.Database) []string {
	names := make([]string, 0, len(schema.ContinuousAggregates))
	for _, aggregate := range schema.ContinuousAggregates {
		names = append(names, strings.ToLower(aggregate.Name))
	}
	return names
}

// describedAggregateDefinition returns the stored SELECT of one aggregate, and
// fails the test when the read did not describe it.
//
// It returns the definition rather than a found flag so a caller asserts on the
// value: a helper that answered "absent" quietly would let a comparison test
// pass over an aggregate nobody read.
func describedAggregateDefinition(schema *catalog.Database, name string) string {
	for _, aggregate := range schema.ContinuousAggregates {
		if strings.EqualFold(aggregate.Name, name) {
			return aggregate.Definition
		}
	}
	return ""
}
