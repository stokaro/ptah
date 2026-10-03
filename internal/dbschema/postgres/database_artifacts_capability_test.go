package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// answerArtifacts answers a server that reports itself as PostgreSQL 14.1, as
// Cloud Spanner's PostgreSQL interface does, and holds one event trigger. It
// records whether the artifact query was sent.
func answerArtifacts(asked *bool) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "version()") {
			return dbtest.QueryResult{Columns: []string{"version"}, Rows: [][]driver.Value{{"PostgreSQL 14.1"}}}, nil
		}
		if strings.Contains(query, "database_scoped_artifacts") {
			*asked = true
			return dbtest.QueryResult{
				Columns: []string{"object_kind", "object_name"},
				Rows:    [][]driver.Value{{"event trigger", "watch_ddl"}},
			}, nil
		}
		return dbtest.QueryResult{}, nil
	}
}

// TestDatabaseScopedArtifactsReadsAServerWithTheCatalogsItJoins lists the
// event trigger on a server whose capability set has pg_depend.
func TestDatabaseScopedArtifactsReadsAServerWithTheCatalogsItJoins(t *testing.T) {
	c := qt.New(t)
	var asked bool
	db := dbtest.Open(c, answerArtifacts(&asked))
	writer := postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public",
		capability.Capabilities{capability.CatalogDependencies: true})

	artifacts, err := writer.DatabaseScopedArtifacts(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(asked, qt.IsTrue)
	c.Assert(artifacts, qt.DeepEquals, []dbreset.Object{{Kind: "event trigger", Name: "watch_ddl"}})
}

// TestDatabaseScopedArtifactsAsksNothingOfSpanner is the server the version
// string misnames: Cloud Spanner's PostgreSQL interface reports PostgreSQL
// 14.1 and has neither pg_publication nor pg_depend, so the query is not sent
// and the answer is none. Sent, it failed every dev-database claim there with
// `relation "pg_publication" does not exist`.
func TestDatabaseScopedArtifactsAsksNothingOfSpanner(t *testing.T) {
	c := qt.New(t)
	var asked bool
	db := dbtest.Open(c, answerArtifacts(&asked))
	writer := postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public",
		capability.Capabilities{capability.CatalogDependencies: false})

	artifacts, err := writer.DatabaseScopedArtifacts(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(asked, qt.IsFalse)
	c.Assert(artifacts, qt.HasLen, 0)
}
