//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/shadow"
)

// The candidate creates two tables, the second referencing the first. Each
// DROP TABLE in either down body below is valid SQL on its own; only the live
// constraint graph decides that one order runs and the other does not.
const roundTripCandidateUpSQL = `CREATE TABLE "orders" (
	"id" SERIAL PRIMARY KEY,
	"user_id" INTEGER NOT NULL,
	CONSTRAINT "fk_orders_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id")
);
CREATE TABLE "order_items" (
	"id" SERIAL PRIMARY KEY,
	"order_id" INTEGER NOT NULL,
	CONSTRAINT "fk_order_items_order" FOREIGN KEY ("order_id") REFERENCES "orders" ("id")
);
`

const roundTripDependencyOrderedDownSQL = `DROP TABLE "order_items";
DROP TABLE "orders";
`

const roundTripReversedDownSQL = `DROP TABLE "orders";
DROP TABLE "order_items";
`

// TestVerifyMigrationRoundTrip_HappyPath drives the whole of
// [shadow.VerifyMigration] on PostgreSQL: the prior history and the candidate
// replay, the resulting catalog matches the desired schema, and the candidate
// is then rolled back and reapplied. The read-back afterwards is what says the
// round trip ran rather than that nothing objected -- both candidate tables are
// present again, which only the round-trip-up replay can produce.
func TestVerifyMigrationRoundTrip_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL, admin := openShadowTestPostgres(c)
	defer dbschema.CloseAndWarn(admin)
	shadowURL, shadowDatabase := createShadowTestPostgres(c, admin, dbURL)
	defer dropShadowTestPostgres(c, admin, shadowDatabase)

	err := shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConnection:  admin,
		MigrationsFS:      roundTripPriorHistory(),
		Dialect:           platform.Postgres,
		Candidates: []shadow.Candidate{{
			Version: 2,
			Name:    "create_orders",
			UpSQL:   roundTripCandidateUpSQL,
			DownSQL: roundTripDependencyOrderedDownSQL,
		}},
		Generated: roundTripDesiredSchema(c),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(readRoundTripShadowTables(c, shadowURL), qt.DeepEquals, []string{"order_items", "orders", "users"})
}

// TestVerifyMigrationRoundTrip_FailurePath varies one axis against the happy
// path: the two DROP TABLE statements in the candidate's down body swap places.
// The up body, the prior history and the desired schema are the same, so the
// replay and the schema comparison both still pass, and the only thing that can
// report the defect is the down body actually running on a server.
func TestVerifyMigrationRoundTrip_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL, admin := openShadowTestPostgres(c)
	defer dbschema.CloseAndWarn(admin)
	shadowURL, shadowDatabase := createShadowTestPostgres(c, admin, dbURL)
	defer dropShadowTestPostgres(c, admin, shadowDatabase)

	err := shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConnection:  admin,
		MigrationsFS:      roundTripPriorHistory(),
		Dialect:           platform.Postgres,
		Candidates: []shadow.Candidate{{
			Version: 2,
			Name:    "create_orders",
			UpSQL:   roundTripCandidateUpSQL,
			DownSQL: roundTripReversedDownSQL,
		}},
		Generated: roundTripDesiredSchema(c),
	})

	c.Assert(err, qt.ErrorMatches, `(?s)shadow check failed: round-trip down: .*`)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "round-trip-down")
	c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "round_trip_down_error")
	c.Assert(
		shadowErr.Result.Mismatches[0].Message,
		qt.Contains,
		"cannot drop table orders because other objects depend on it",
	)
	c.Assert(shadowErr.Err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, shadowErr.Err)
}

func roundTripPriorHistory() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_users.up.sql": {Data: []byte(
			`CREATE TABLE "users" ("id" SERIAL PRIMARY KEY, "name" TEXT NOT NULL);`,
		)},
		"0000000001_users.down.sql": {Data: []byte(`DROP TABLE "users";`)},
	}
}

func roundTripDesiredSchema(c *qt.C) *schemamodel.Database {
	c.Helper()
	dir := c.TempDir()
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o755), qt.IsNil)
	content := `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)" foreign_key_name="fk_orders_user"
	UserID int64
}

//ptah:schema:table name="order_items"
type OrderItem struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="order_id" type="INTEGER" not_null="true" foreign="orders(id)" foreign_key_name="fk_order_items_order"
	OrderID int64
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0o600), qt.IsNil)
	desired, err := goschema.ParseFS(os.DirFS(dir), "entities")
	c.Assert(err, qt.IsNil)
	return desired
}

func readRoundTripShadowTables(c *qt.C, shadowURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), shadowURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = current_schema()
		  AND table_type = 'BASE TABLE'
		  AND table_name IN ('users', 'orders', 'order_items')
		ORDER BY table_name
	`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
