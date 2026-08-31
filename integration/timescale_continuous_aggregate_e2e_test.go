//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbexprprobe"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestTimescaleContinuousAggregateConvergesE2E is the live acceptance for
// comparing a declared continuous aggregate against the one the server holds
// (stokaro/ptah#1026).
//
// The declaration and the catalog never agree textually. TimescaleDB rewrites
// the definition before storing it -- `time_bucket('1 hour', time)` comes back
// as `time_bucket('01:00:00'::interval, "time")`, and the GROUP BY key written
// by its output name comes back as the whole expression that name stood for --
// so a string comparison reports a change for an aggregate nobody touched, and
// the plan it produces DROPs the aggregate and its materialization on every
// run.
//
// Only a server can settle it, which is why this test is live: the declaration
// is put through the same rewrite in a rolled-back transaction and the two
// normalized forms are compared. A unit test cannot hold the rewrite, because
// the rewrite is the thing under test.
func TestTimescaleContinuousAggregateConvergesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := "ts_cagg_readings_" + suffix
	aggregate := "ts_cagg_hourly_" + suffix
	dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, aggregate)
	defer dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, aggregate)

	// The body as an author writes it. Every part of it is rewritten on the way
	// into the catalog, and this exact string is what the declaration below
	// carries.
	body := fmt.Sprintf(
		`SELECT time_bucket('1 hour', "time") AS bucket, device, avg(temperature) AS avg_temp `+
			`FROM %s GROUP BY bucket, device`, table)

	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE TABLE %s ("time" TIMESTAMPTZ NOT NULL, device TEXT NOT NULL, temperature DOUBLE PRECISION)`,
		table))
	execTimescale(ctx, c, conn, fmt.Sprintf(`SELECT create_hypertable('%s', by_range('time'))`, table))
	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous) AS %s WITH NO DATA`, aggregate, body))

	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The catalog and the declaration disagree textually. Asserting that first
	// is what makes the convergence below mean something: without it, a
	// comparison that never looked at the body would pass this test.
	c.Assert(describedAggregateDefinition(live, aggregate), qt.Not(qt.Equals), body)
	c.Assert(describedAggregateDefinition(live, aggregate), qt.Contains, "01:00:00")

	declared := timescaleDeclaration(table, aggregate, body)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil)

	c.Assert(err, qt.IsNil)
	// Scoped to the aggregate this test made. The server is shared, and a diff
	// read whole would report every other object on it -- which is a fact about
	// the server rather than about the comparison under test.
	c.Assert(aggregateChanges(diff, aggregate), qt.HasLen, 0,
		qt.Commentf("declared:\n%s\n\ncatalog:\n%s", body, describedAggregateDefinition(live, aggregate)))

	// The control on the other side: a body that really did change is reported,
	// so the normalization above is not a comparison that always agrees.
	changed := fmt.Sprintf(
		`SELECT time_bucket('1 day', "time") AS bucket, device, avg(temperature) AS avg_temp `+
			`FROM %s GROUP BY bucket, device`, table)
	changedDiff, err := schemadiff.CompareWithDatabase(
		ctx, conn, timescaleDeclaration(table, aggregate, changed), live, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(aggregateChanges(changedDiff, aggregate), qt.DeepEquals, []string{"modified:" + aggregate})
}

// aggregateChanges names the changes a diff carries for ONE aggregate, in a
// form an assertion can read.
func aggregateChanges(diff *difftypes.SchemaDiff, name string) []string {
	changes := make([]string, 0, 3)
	changes = append(changes, prefixedMatches("added:", diff.ContinuousAggregatesAdded.Names(), name)...)
	changes = append(changes, prefixedMatches("removed:", diff.ContinuousAggregatesRemoved.Names(), name)...)
	for _, change := range diff.ContinuousAggregatesModified {
		changes = append(changes, prefixedMatches("modified:", []string{change.Name}, name)...)
	}
	return changes
}

// prefixedMatches labels the entries naming one object and drops the rest.
func prefixedMatches(label string, entries []string, name string) []string {
	matched := make([]string, 0, len(entries))
	for _, entry := range entries {
		matched = append(matched, labelIfNames(label, entry, name)...)
	}
	return matched
}

func labelIfNames(label, entry, name string) []string {
	if !strings.Contains(strings.ToLower(entry), strings.ToLower(name)) {
		return nil
	}
	return []string{label + name}
}

// TestTimescaleContinuousAggregateProbeLeavesNothingBehindE2E pins that asking
// the server to normalize a declaration creates nothing.
//
// The probe creates a real continuous aggregate to read its stored definition
// back, and a continuous aggregate is not a cheap object: it carries a
// materialization hypertable of its own in a schema the extension owns. If the
// rollback did not reach those, every comparison would leave one behind on the
// operator's database.
func TestTimescaleContinuousAggregateProbeLeavesNothingBehindE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	table := "ts_probe_readings_" + suffix
	aggregate := "ts_probe_hourly_" + suffix
	dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, aggregate)
	defer dropTimescaleFixture(context.WithoutCancel(ctx), conn, table, aggregate)

	body := fmt.Sprintf(
		`SELECT time_bucket('1 hour', "time") AS bucket, count(*) AS n FROM %s GROUP BY bucket`, table)

	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE TABLE %s ("time" TIMESTAMPTZ NOT NULL)`, table))
	execTimescale(ctx, c, conn, fmt.Sprintf(`SELECT create_hypertable('%s', by_range('time'))`, table))
	execTimescale(ctx, c, conn, fmt.Sprintf(
		`CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous) AS %s WITH NO DATA`, aggregate, body))

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	resolved, err := dbexprprobe.ResolveContinuousAggregateBodies(ctx, conn, []dbexprprobe.ContinuousAggregateProbe{{
		Key: aggregate, Body: body,
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(resolved[aggregate].Resolved, qt.IsTrue)
	c.Assert(resolved[aggregate].Body, qt.Equals, describedAggregateDefinition(before, aggregate))

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(describedAggregateNames(after), qt.DeepEquals, describedAggregateNames(before))
}

// timescaleDeclaration is the desired schema both tests compare with: the table
// as a Go-annotated schema would describe it, the hypertable declaration, and
// the aggregate carrying the body it was written with.
func timescaleDeclaration(table, aggregate, body string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "R", Name: table}},
		Fields: []schemamodel.Field{
			{StructName: "R", Name: "time", Type: "TIMESTAMPTZ"},
			{StructName: "R", Name: "device", Type: "TEXT"},
			{StructName: "R", Name: "temperature", Type: "DOUBLE PRECISION", Nullable: true},
		},
		Hypertables: []schemamodel.Hypertable{{StructName: "R", Table: table, Column: "time"}},
		ContinuousAggregates: []schemamodel.ContinuousAggregate{{
			StructName: "A", Name: aggregate, Body: body,
		}},
	}
}
