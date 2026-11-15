package postgres_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// addedSchemasQuery answers for a dev database whose URL pins public and
// that holds auth and kept beside it, with nothing in either for the cleanup
// to drop first.
func addedSchemasQuery() *postgresRealmQuery {
	query := newPostgresRealmMetadataQuery()
	query.initialSchemas = [][]driver.Value{{"auth"}, {"kept"}, {"public"}}
	query.publicObjects = nil
	query.defaultACLs = nil
	return query
}

// TestWriterDropSchemasAddedSince_HappyPath drops the schemas beside the pinned
// one that the claim did not find, and only those: the pinned schema is the
// other half of the cleanup, and a schema the claim found is left as it is,
// with what it holds.
func TestWriterDropSchemasAddedSince_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		kept []string
		want []string
	}{
		{name: "a schema the run created", kept: []string{"kept"}, want: []string{`DROP SCHEMA IF EXISTS "auth" RESTRICT`}},
		{name: "every schema found at the claim", kept: []string{"auth", "kept"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var executed []string
			db := dbtest.OpenWithExec(t, addedSchemasQuery().query, func(query string, _ []driver.NamedValue) (driver.Result, error) {
				executed = append(executed, query)
				return driver.RowsAffected(0), nil
			})

			err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropSchemasAddedSince(c.Context(), dbreset.Kept{Schemas: test.kept})

			c.Assert(err, qt.IsNil)
			c.Assert(executed, qt.DeepEquals, test.want)
			c.Assert(db.CommitCount(), qt.Equals, 1)
		})
	}
}

// TestWriterDropSchemasAddedSince_FailurePath is a server that refuses the
// drop: the cleanup reports it and rolls back.
func TestWriterDropSchemasAddedSince_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New(`cannot drop schema auth because other objects depend on it`)
	db := dbtest.OpenWithExec(t, addedSchemasQuery().query, func(string, []driver.NamedValue) (driver.Result, error) {
		return nil, refused
	})

	err := postgres.NewPostgreSQLWriter(db.SQL, "public").DropSchemasAddedSince(c.Context(), dbreset.Kept{Schemas: []string{"kept"}})

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}
