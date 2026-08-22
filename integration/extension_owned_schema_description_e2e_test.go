//go:build integration

package integration_test

import (
	"context"
	"net/url"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemascope"
)

// TestExtensionOwnedSchemaStaysOutOfTheRealmDescriptionE2E pins what a
// realm-scope read describes on a database where an extension owns a schema.
//
// The case was found on TimescaleDB and it is not TimescaleDB's. One
// `CREATE EXTENSION timescaledb` adds seven namespaces the extension owns --
// `_timescaledb_cache`, `_timescaledb_catalog`, `_timescaledb_config`,
// `_timescaledb_functions`, `_timescaledb_internal`,
// `timescaledb_experimental` and `timescaledb_information` -- and describing
// them describes the extension's own catalog as the operator's schema.
// Measured on TimescaleDB 2.29.2 / PostgreSQL 17.11 before the exclusion,
// `schema inspect` on a database holding one user table answered 4003 lines
// carrying 51 extension relations, against 24 lines from the pinned Atlas
// community binary v1.3.0, which describes the table and `public` and nothing
// else (stokaro/ptah#1026).
//
// The test runs on the ordinary PostgreSQL service rather than on a
// TimescaleDB one, because what it measures is the rule and not the vendor:
// `ALTER EXTENSION ... ADD SCHEMA` puts exactly the pg_depend edge on
// pg_namespace that TimescaleDB's own schemas carry, and reproducing it from
// SQL is what makes this a property of extension-owned namespaces rather than
// a fixture of one product. A second container would test one arrangement of
// it and cost the suite a database.
//
// The control is the half a vacuous test omits: the schema really is there,
// and it really is owned, while the read is running.
func TestExtensionOwnedSchemaStaysOutOfTheRealmDescriptionE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, realmURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropExtensionOwnedFixture(context.WithoutCancel(ctx), conn)
	defer dropExtensionOwnedFixture(context.WithoutCancel(ctx), conn)

	execPostgres(ctx, c, conn, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	execPostgres(ctx, c, conn, "CREATE SCHEMA ext_owned_probe")
	execPostgres(ctx, c, conn, "CREATE TABLE ext_owned_probe.owned_relation (id integer)")
	execPostgres(ctx, c, conn, "ALTER EXTENSION pg_trgm ADD SCHEMA ext_owned_probe")
	execPostgres(ctx, c, conn, "CREATE TABLE public.declared_relation (id integer)")

	// The control, read from the server rather than assumed: without the
	// pg_depend edge this test passes against any database at all.
	c.Assert(extensionOwnedSchemas(ctx, c, conn), qt.Contains, "ext_owned_probe")

	names, err := schemascope.ReadNames(ctx, conn.Info(), nil, conn)
	c.Assert(err, qt.IsNil)
	c.Assert(names, qt.Not(qt.Contains), "ext_owned_probe")
	c.Assert(names, qt.Contains, "public")

	read, err := dbschema.ReadSchemaWithSchemas(conn, names)
	c.Assert(err, qt.IsNil)

	// Both halves. The schema is not described, and neither is what it holds:
	// of the 51 relations the unfiltered TimescaleDB read carried, every one
	// arrived because its schema did.
	c.Assert(postgresSchemaNames(read.Schemas), qt.Not(qt.Contains), "ext_owned_probe")
	c.Assert(postgresRelationSchemas(read), qt.Not(qt.Contains), "ext_owned_probe")
	c.Assert(postgresTableNames(read.Tables), qt.Not(qt.Contains), "owned_relation")
	c.Assert(postgresTableNames(read.Tables), qt.Contains, "declared_relation")
}

// realmURL strips any schema the suite's URL pins, because the read under test
// is the realm read and a URL that pins a schema never reaches it.
func realmURL(c *qt.C, address string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Del("search_path")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// dropExtensionOwnedFixture removes what this test creates, tolerating a run
// that was killed before it created all of it.
//
// The schema has to leave the extension before it can be dropped: measured on
// PostgreSQL 18, DROP SCHEMA on an extension member answers
// `cannot drop schema ext_owned_probe because extension pg_trgm requires it`.
func dropExtensionOwnedFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	writer := conn.SchemaWriter()
	_ = writer.ExecuteSQL(ctx, "ALTER EXTENSION pg_trgm DROP SCHEMA ext_owned_probe")
	_ = writer.ExecuteSQL(ctx, "DROP SCHEMA IF EXISTS ext_owned_probe CASCADE")
	_ = writer.ExecuteSQL(ctx, "DROP TABLE IF EXISTS public.declared_relation")
}

// extensionOwnedSchemas asks the server which namespaces an extension owns.
func extensionOwnedSchemas(
	ctx context.Context,
	c *qt.C,
	conn *dbschema.DatabaseConnection,
) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `
		SELECT n.nspname
		FROM pg_namespace n
		JOIN pg_depend d
		  ON d.classid = 'pg_namespace'::regclass
		 AND d.objid = n.oid
		 AND d.deptype = 'e'
		ORDER BY n.nspname`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// postgresRelationSchemas names every schema the description places a relation
// in, deduplicated and sorted.
//
// Tables, views and materialized views together, because the leak did not stop
// at tables: most of the 51 relations the unfiltered read described were views
// of the extension's information catalog, and a check that looked only at
// tables would have called that read clean.
func postgresRelationSchemas(read *dbschematypes.DBSchema) []string {
	seen := map[string]bool{}
	for _, table := range read.Tables {
		seen[table.Schema] = true
	}
	for _, view := range read.Views {
		seen[view.Schema] = true
	}
	for _, view := range read.MatViews {
		seen[view.Schema] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func postgresSchemaNames(schemas []dbschematypes.DBSchemaInfo) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

func postgresTableNames(tables []dbschematypes.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

func execPostgres(
	ctx context.Context,
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	statement string,
) {
	c.Helper()
	c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
		qt.Commentf("statement: %s", statement))
}
