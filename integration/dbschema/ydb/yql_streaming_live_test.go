//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestYDBDesiredYQL_StreamingQueries(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn, clusterFlag{yaml: "enable_streaming_queries", page: "EnableStreamingQueries", on: true})
	conn := openYDB(c, line)
	const directory = "ptah_yql_streaming"
	schemas := []string{directory}
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(context.Context, string) error
	})
	c.Assert(ok, qt.IsTrue)
	c.Cleanup(func() { c.Check(dropper.DropDirectory(context.Background(), directory), qt.IsNil) })
	path := filepath.Join(c.TempDir(), "schema.sql")
	const topics = "CREATE TOPIC `ptah_yql_streaming/source`; CREATE TOPIC `ptah_yql_streaming/sink`;"
	const body = " AS DO BEGIN INSERT INTO `ptah_yql_streaming/sink` SELECT * FROM `ptah_yql_streaming/source`"
	first := loadStreamingYQL(c, path, topics+"CREATE STREAMING QUERY `ptah_yql_streaming/copy` WITH (RUN=FALSE)"+body+"; END DO;")
	apply(c, conn, planAgainst(c, conn, first, schemas))
	c.Assert(planAgainst(c, conn, first, schemas), qt.HasLen, 0)
	refused := loadStreamingYQL(c, path, topics+"CREATE STREAMING QUERY `ptah_yql_streaming/copy` WITH (RUN=FALSE)"+body+" WHERE TRUE; END DO;")
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), refused, readScoped(c, conn, schemas), conn.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, "ydb", planner.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	c.Assert(statements, qt.HasLen, 0)
	for _, source := range []string{
		topics + "CREATE STREAMING QUERY `ptah_yql_streaming/copy` WITH (RUN=TRUE)" + body + "; END DO;",
		topics + "CREATE OR REPLACE STREAMING QUERY `ptah_yql_streaming/copy` WITH (RUN=FALSE)" + body + " WHERE TRUE; END DO;",
		topics,
		"",
	} {
		desired := loadStreamingYQL(c, path, source)
		plan := planAgainst(c, conn, desired, schemas)
		c.Assert(plan, qt.Not(qt.HasLen), 0)
		apply(c, conn, plan)
		c.Assert(planAgainst(c, conn, desired, schemas), qt.HasLen, 0)
	}
}

func loadStreamingYQL(c *qt.C, path, source string) *schemamodel.Database {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
	desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	return desired
}
