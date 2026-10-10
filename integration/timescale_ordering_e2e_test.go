//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/dbtarget"
)

// timescaleSchemaFixture opens the TimescaleDB server and a schema of the
// test's own, dropped again when the test ends.
func timescaleSchemaFixture(c *qt.C, prefix string) (context.Context, *dbschema.DatabaseConnection, string) {
	c.Helper()
	dbURL := dbtarget.URL(c, dbtarget.TimescaleDB)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	c.Cleanup(cancel)
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schemaName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	execTimescale(ctx, c, conn, "CREATE SCHEMA "+schemaName)
	c.Cleanup(func() { dropTimescaleSchema(context.Background(), conn, schemaName) })
	return ctx, conn, schemaName
}

// applyTimescale plans the declaration against the live schema and applies
// every statement the plan holds, failing on the first one the server
// refuses.
func applyTimescale(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, schemaName string) []string {
	c.Helper()
	statements := planTimescale(c, conn, declared, schemaName)
	for _, statement := range statements {
		execTimescale(ctx, c, conn, statement)
	}
	return statements
}

// readingsWithAggregateAndView declares a hypertable in schemaName, a
// continuous aggregate over it, and a view that reads the aggregate.
func readingsWithAggregateAndView(schemaName string) *schemamodel.Database {
	declared := hypertableSchema(schemaName, "readings", "time", "")
	declared.FeatureObjects = must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject(schemaName, "hourly",
		tsschema.DesiredContinuousAggregate{Body: fmt.Sprintf(
			`SELECT time_bucket('1 hour', "time") AS bucket, avg(value) AS avg_value FROM %s.readings GROUP BY 1`, schemaName)}),
	))
	declared.Views = []schemamodel.View{{StructName: "Recent", Name: schemaName + ".recent", Body: "SELECT bucket FROM " + schemaName + ".hourly"}}
	return declared
}

// TestTimescaleAggregateAndTheViewThatReadsItE2E pins the order between a
// continuous aggregate and a view that reads it, in both directions, against
// a server that refuses the wrong one: CREATE VIEW over an aggregate that does
// not exist yet answers `relation ... does not exist`, and DROP MATERIALIZED
// VIEW of an aggregate a view still reads answers that other objects depend
// on it.
func TestTimescaleAggregateAndTheViewThatReadsItE2E(t *testing.T) {
	c := qt.New(t)
	ctx, conn, schemaName := timescaleSchemaFixture(c, "ts_order")

	created := applyTimescale(ctx, c, conn, readingsWithAggregateAndView(schemaName), schemaName)
	createdScript := strings.Join(created, "\n")
	c.Assert(strings.Index(createdScript, `CREATE MATERIALIZED VIEW "`+schemaName+`"."hourly"`) < strings.Index(createdScript, `CREATE VIEW "`+schemaName+`"."recent"`),
		qt.IsTrue, qt.Commentf("plan:\n%s", createdScript))

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(describedAggregateNames(live), qt.DeepEquals, []string{"hourly"})
	c.Assert(describedViewNames(live), qt.DeepEquals, []string{"recent"})

	dropped := applyTimescale(ctx, c, conn, hypertableSchema(schemaName, "readings", "time", ""), schemaName)
	droppedScript := strings.Join(dropped, "\n")
	c.Assert(strings.Index(droppedScript, "DROP VIEW") < strings.Index(droppedScript, "DROP MATERIALIZED VIEW"),
		qt.IsTrue, qt.Commentf("plan:\n%s", droppedScript))

	live, err = dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(describedAggregateNames(live), qt.HasLen, 0)
	c.Assert(describedViewNames(live), qt.HasLen, 0)
}

// TestTimescaleMixedCaseHypertableE2E pins that create_hypertable finds a
// table whose name only a quoted spelling keeps. The CREATE TABLE quotes the
// name, and the REGCLASS literal the call takes is parsed as a name, so an
// unquoted literal would look for the folded name and answer
// `relation ... does not exist`.
func TestTimescaleMixedCaseHypertableE2E(t *testing.T) {
	c := qt.New(t)
	ctx, conn, schemaName := timescaleSchemaFixture(c, "ts_case")
	declared := hypertableSchema(schemaName, "Readings", "time", "1 day")

	statements := applyTimescale(ctx, c, conn, declared, schemaName)

	c.Assert(strings.Join(statements, "\n"), qt.Contains, `SELECT create_hypertable('"`+schemaName+`"."Readings"'`)
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(readHypertable(c, live, "Readings"), qt.DeepEquals, &tsschema.ObservedHypertable{
		Column: "time", ColumnType: "timestamp with time zone", ChunkInterval: "1 day", Dimensions: 1,
	})
	c.Assert(planTimescale(c, conn, declared, schemaName), qt.HasLen, 0)
}

// TestTimescaleQualifiedAggregateAnnotationE2E pins that a Go annotation naming
// an aggregate `schema.name` creates name in schema, and that the comparison
// afterwards finds nothing to do: a relation literally named "schema.name" in
// the default schema would be created instead, and the real one planned for a
// drop.
func TestTimescaleQualifiedAggregateAnnotationE2E(t *testing.T) {
	c := qt.New(t)
	ctx, conn, schemaName := timescaleSchemaFixture(c, "ts_named")
	source := fmt.Sprintf("package models\n\n"+
		"//ptah:schema:table name=\"readings\" schema=%q\ntype Reading struct {\n"+
		"\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\" not_null=\"true\"\n\tTime string\n"+
		"\t//ptah:schema:field name=\"value\" type=\"INTEGER\" not_null=\"true\"\n\tValue int\n}\n\n"+
		"//ptah:schema:hypertable table=\"%s.readings\" column=\"time\"\ntype ReadingsHypertable struct{}\n\n"+
		"//ptah:schema:continuousaggregate name=\"%s.hourly\" body=\"SELECT time_bucket('1 hour', time) AS bucket, avg(value) AS v FROM %s.readings GROUP BY 1\"\n"+
		"type Hourly struct{}\n", schemaName, schemaName, schemaName, schemaName)
	parsed, err := goschema.ParseSource("readings.go", source)
	c.Assert(err, qt.IsNil)
	declared := &parsed
	declared.Schemas = []schemamodel.Schema{{Name: schemaName}}
	declared.Extensions = []schemamodel.Extension{{Name: "timescaledb", IfNotExists: true}}

	applyTimescale(ctx, c, conn, declared, schemaName)

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	var names []string
	for _, ref := range live.FeatureObjects.Refs() {
		names = append(names, tsschema.QualifiedName(ref))
	}
	c.Assert(names, qt.DeepEquals, []string{schemaName + ".hourly"})
	c.Assert(planTimescale(c, conn, declared, schemaName), qt.HasLen, 0)
}
