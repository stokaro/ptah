//go:build integration

package postgres_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// The extensions a server puts in every database survive both paths that
// could remove them: a comparison of an empty declaration plans no DROP
// EXTENSION, and the realm cleanup leaves each one installed. Both read the one
// list in internal/serverobjects, so an extension a later line adds to its
// template fails both halves of this test until the list names it, rather than
// one of them.
func TestServerExtensions_LiveSurviveTheComparisonAndTheCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	tests := []postgresWriterFamilyLiveCase{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, test.engine))
			defer liveDatabase.cleanup()
			installed := postgresWriterLiveExtensionNames(c, ctx, liveDatabase.db)
			conn, err := dbschema.ConnectToDatabase(ctx, engineURLFor(c, test.engine, liveDatabase.name))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(&schemamodel.Database{}, live, conn.Info(), nil)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0, qt.Commentf("installed: %v", installed))

			c.Assert(postgres.NewPostgreSQLWriter(liveDatabase.db, "public").DropDatabaseRealm(ctx), qt.IsNil)
			c.Assert(postgresWriterLiveExtensionNames(c, ctx, liveDatabase.db), qt.DeepEquals, installed)
		})
	}
}

// engineURLFor is the address Ptah connects to database with on the engine's
// target, keeping the engine's own scheme.
func engineURLFor(c *qt.C, engine dbtarget.Engine, database string) string {
	c.Helper()
	parsed, err := url.Parse(dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	return parsed.String()
}
