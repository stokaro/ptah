package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
)

// TestRender_ADeclaredExtensionIsTheOfflineEvidence pins that a render produces
// a coherent script.
//
// A render has no connection to ask which extensions the target has, and the
// document in front of it declares them. Without that rule, a schema declaring
// the timescaledb extension and a hypertable rendered the CREATE EXTENSION and
// then skipped the call that needs it — a script that installs an extension and
// refuses to use it (stokaro/ptah#1026).
func TestRender_ADeclaredExtensionIsTheOfflineEvidence(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(hypertableDocumentDeclaringTheExtension(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	script := strings.Join(statements, "\n")
	c.Assert(script, qt.Contains, `CREATE EXTENSION "timescaledb"`)
	c.Assert(script, qt.Contains, `SELECT create_hypertable('"public"."readings"'`)
	c.Assert(script, qt.Not(qt.Contains), "hypertable public.readings is not supported")
}

// TestRender_WithoutTheExtensionTheCallIsSkipped is the control the rule needs.
//
// A schema that declares a hypertable and NOT the extension is asking for a
// call the target may not have, and an offline render has nothing that says it
// does. Skipping with a comment is the answer; emitting it would produce a
// script that fails on `function create_hypertable(unknown, unknown) does not
// exist`.
func TestRender_WithoutTheExtensionTheCallIsSkipped(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatements(hypertableDocument(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	script := strings.Join(statements, "\n")
	c.Assert(script, qt.Contains, "hypertable public.readings is not supported by this target; skipped.")
	c.Assert(script, qt.Not(qt.Contains), "create_hypertable")
}

// hypertableDocumentDeclaringTheExtension is the same document plus the
// extension that makes the call available.
func hypertableDocumentDeclaringTheExtension() *schemamodel.Database {
	database := hypertableDocument()
	database.Extensions = []schemamodel.Extension{{Name: "timescaledb"}}
	return database
}

// hypertableDocument declares one partitioned table and nothing else.
func hypertableDocument() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables: []schemamodel.Table{{StructName: "T", Name: "readings", Schema: "public",
			Facets: must.Must(schemaext.NewFacets(&tsschema.DesiredHypertable{Column: "time"}))}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true},
		},
	}
}

// TestRender_AnAggregateComesBetweenItsTableAndTheViewsThatReadIt pins the
// whole-schema order: the aggregate after the table and the call that
// partitions it, and before a view that reads it. A script that created the
// view first would answer `relation "hourly" does not exist`.
func TestRender_AnAggregateComesBetweenItsTableAndTheViewsThatReadIt(t *testing.T) {
	c := qt.New(t)
	database := hypertableDocumentDeclaringTheExtension()
	database.FeatureObjects = must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject("public", "hourly",
		tsschema.DesiredContinuousAggregate{Body: "SELECT time_bucket('1 hour', time) AS bucket FROM public.readings GROUP BY bucket"})))
	database.FeatureCoverage = must.Must(tsschema.CompleteCoverage(schemaext.Desired))
	database.Views = []schemamodel.View{{StructName: "Recent", Name: "public.recent", Body: "SELECT bucket FROM public.hourly"}}

	statements, err := builtin.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(err, qt.IsNil)
	script := strings.Join(statements, "\n")
	table := strings.Index(script, `CREATE TABLE "public"."readings"`)
	hypertable := strings.Index(script, "SELECT create_hypertable(")
	aggregate := strings.Index(script, `CREATE MATERIALIZED VIEW "public"."hourly"`)
	view := strings.Index(script, `CREATE VIEW "public"."recent"`)
	c.Assert([]bool{table >= 0, hypertable > table, aggregate > hypertable, view > aggregate}, qt.DeepEquals, []bool{true, true, true, true},
		qt.Commentf("script:\n%s", script))
}
