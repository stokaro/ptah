//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
)

// withDevSearchPath returns dbURL carrying a search_path query parameter.
func withDevSearchPath(c *qt.C, dbURL, schema string) string {
	c.Helper()
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// countDevSchema returns how many namespaces of the given name exist.
func countDevSchema(c *qt.C, ctx context.Context, db *sql.DB, name string) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_namespace WHERE nspname = $1", name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// TestPostgresConnectionResolvesTheSearchPathSchemaE2E pins the schema a
// PostgreSQL connection reports, because a writer decides from it which schemas
// belong to the caller and which are strangers it may drop.
//
// It used to be the constant "public": the code assigned "public", then branched
// on the URL path to assign "public" again. A dev URL naming another schema was
// therefore not merely ignored — the realm cleanup treated that schema as a
// stranger and DROPPED it, and the replay that followed ran under a search_path
// resolving to nothing, failing with "no schema has been selected to create in"
// (stokaro/ptah#1198). The pinned community binary leaves the schema standing.
//
// The rows are a pair plus the fallback: only the search_path moves.
func TestPostgresConnectionResolvesTheSearchPathSchemaE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_dev_schema_scope_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	scopedURL := replaceDatabaseName(c, dbURL, testDBName)
	setupDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()
	_, err = setupDB.ExecContext(ctx, "CREATE SCHEMA app")
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name       string
		searchPath string
		want       string
	}{{
		name:       "a search_path naming another schema resolves to it",
		searchPath: "app",
		want:       "app",
	}, {
		name:       "a search_path naming public resolves to public",
		searchPath: "public",
		want:       "public",
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(ctx, withDevSearchPath(c, scopedURL, test.searchPath))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			c.Assert(conn.Info().Schema, qt.Equals, test.want)
		})
	}

	// A search_path naming a schema that does not exist is REFUSED, naming the
	// schema. Folding it back to "public" was the first shape of this change and
	// is wrong for the same reason the bug was: a caller who named a schema and
	// silently got a different one is exactly what this fixes, and "public" would
	// resume dropping the schemas it does not cover.
	//
	// The message has to name the schema. Without it the run reaches the replay
	// and dies on a CREATE TABLE with "no schema has been selected to create in",
	// which sends the operator to their migration instead of their URL.
	t.Run("a search_path naming no existing schema is refused", func(t *testing.T) {
		c := qt.New(t)
		_, err := dbschema.ConnectToDatabase(ctx, withDevSearchPath(c, scopedURL, "nosuchschema"))

		c.Assert(err, qt.ErrorMatches,
			`.*database URL selects schema "nosuchschema", which does not exist in this database.*`)
	})
}

// TestPostgresRealmCleanupKeepsTheSelectedSchemaE2E is the consequence the
// resolution above exists for: cleaning the database realm through a dev URL
// that selects `app` must leave `app` standing, and must still clean the
// realm's other user schemas.
//
// Restoring the hardcoded "public" reddens the `app` assertion, which is the
// half this change is about.
//
// The "public" assertion is the other half, and it is a defect this change
// introduced before it was caught: once the root follows the URL, "public" is
// just another user schema, so it was dropped and never put back, and the next
// migration naming `public.users` failed with `schema "public" does not exist`.
// Emptying it is what a realm cleanup is for; removing it is not.
//
// The `bystander` schema is NOT an independent assertion, and saying so is the
// point of this comment. Sparing every schema but the root — the inverse mutant
// that would have to prove it — is caught one line earlier, by the production
// code's own verification:
//
//	e`PostgreSQL database realm cleanup left residual user schema "bystander"`
//
// returned from DropDatabaseRealm, which the test already asserts is nil. So
// the bystander is here to give the cleanup something to do and to make that
// verification reachable; the count below restates the outcome for a reader
// rather than adding coverage the error check does not already have.
func TestPostgresRealmCleanupKeepsTheSelectedSchemaE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_dev_realm_keep_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	scopedURL := replaceDatabaseName(c, dbURL, testDBName)
	setupDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()
	for _, statement := range []string{"CREATE SCHEMA app", "CREATE SCHEMA bystander"} {
		_, err = setupDB.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil)
	}

	conn, err := dbschema.ConnectToDatabase(ctx, withDevSearchPath(c, scopedURL, "app"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	writer, ok := conn.SchemaWriter().(interface{ DropDatabaseRealm(context.Context) error })
	c.Assert(ok, qt.IsTrue)
	c.Assert(writer.DropDatabaseRealm(ctx), qt.IsNil)

	c.Assert(countDevSchema(c, ctx, setupDB, "app"), qt.Equals, 1)
	c.Assert(countDevSchema(c, ctx, setupDB, "bystander"), qt.Equals, 0)
	c.Assert(countDevSchema(c, ctx, setupDB, "public"), qt.Equals, 1)
}
