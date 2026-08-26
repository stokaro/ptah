//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestPostgresPooledProxyDescribesTheSameSchemaE2E is stokaro/ptah#1029's
// first acceptance criterion for the read half: Ptah connects to and inspects
// a database through a transaction-pooling proxy, and gets the same answer it
// gets directly.
//
// It is one server reached two ways, and the assertion is that the two
// descriptions agree. A pooler hands a client whichever backend is free
// between transactions, so anything the read left in a session -- a
// search_path, a temporary table, a prepared statement -- belongs to a backend
// the next statement may not get. A read that quietly depended on one would
// differ here and nowhere else.
func TestPostgresPooledProxyDescribesTheSameSchemaE2E(t *testing.T) {
	directURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	table := fmt.Sprintf("pooled_read_%d", time.Now().UnixNano())
	direct, err := dbschema.ConnectToDatabase(ctx, directURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(direct)

	writer := direct.SchemaWriter()
	c.Assert(writer.ExecuteSQL(ctx,
		fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, title TEXT NOT NULL)", table)), qt.IsNil)
	defer func() {
		c.Check(writer.ExecuteSQL(context.WithoutCancel(ctx),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", table)), qt.IsNil)
	}()

	pooled, err := dbschema.ConnectToDatabase(ctx, pooledURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(pooled)

	directSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, direct, []string{"public"})
	c.Assert(err, qt.IsNil)
	pooledSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, pooled, []string{"public"})
	c.Assert(err, qt.IsNil)

	// Non-vacuity first: the table this test created is in both reads, so two
	// empty descriptions cannot pass as agreement.
	c.Assert(pooledTableNames(pooledSchema), qt.Contains, table)
	c.Assert(pooledTableNames(directSchema), qt.DeepEquals, pooledTableNames(pooledSchema))
	c.Assert(pooledSchema.Tables, qt.DeepEquals, directSchema.Tables)
	c.Assert(pooledSchema.Constraints, qt.DeepEquals, directSchema.Constraints)
	c.Assert(pooledSchema.Indexes, qt.DeepEquals, directSchema.Indexes)
}

func pooledTableNames(schema *dbschematypes.DBSchema) []string {
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	return names
}

// TestPostgresPooledProxyCarriesTheRefusedSearchPathE2E is stokaro/ptah#1029's
// third criterion: schema selection must not depend on a persistent backend
// session.
//
// A `?search_path=` on a PostgreSQL URL is a STARTUP parameter, and PgBouncer
// refuses the connection outright rather than ignoring it:
//
//	FATAL: unsupported startup parameter: search_path (SQLSTATE 08P01)
//
// Nothing about the schema is wrong there. So Ptah reconnects without the
// parameter and lets the SERVER resolve the selection inside a transaction
// instead, which a pooler keeps on one backend. This asserts the result is the
// selection the URL named -- not the connection default the session would
// report for a URL it never received.
//
// The direct connection with the identical URL is the control, and the
// assertion is that the two agree. A carry that resolved to `public` would
// pass a test that only checked the connection succeeded.
func TestPostgresPooledProxyCarriesTheRefusedSearchPathE2E(t *testing.T) {
	directURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := fmt.Sprintf("pooled_sp_%d", time.Now().UnixNano())
	direct, err := dbschema.ConnectToDatabase(ctx, directURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(direct)

	writer := direct.SchemaWriter()
	c.Assert(writer.ExecuteSQL(ctx, fmt.Sprintf("CREATE SCHEMA %s", schema)), qt.IsNil)
	defer func() {
		c.Check(writer.ExecuteSQL(context.WithoutCancel(ctx),
			fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)), qt.IsNil)
	}()

	pooled, err := dbschema.ConnectToDatabase(ctx, withSearchPath(c, pooledURL, schema))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(pooled)

	selected, err := dbschema.ConnectToDatabase(ctx, withSearchPath(c, directURL, schema))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(selected)

	c.Assert(pooled.Info().Schema, qt.Equals, schema)
	c.Assert(pooled.Info().Schema, qt.Equals, selected.Info().Schema)
}

// TestPostgresPooledProxyRefusesASchemaThatDoesNotExistE2E is the other half of
// the carry, and the one that would otherwise pass silently.
//
// The session answer being carried is the refusal too: a URL naming a schema
// the database does not have is rejected rather than folded back to `public`,
// which is stokaro/ptah#1198's failure. A carry that skipped the resolution
// would connect happily and leave the writer owning the wrong schema.
func TestPostgresPooledProxyRefusesASchemaThatDoesNotExistE2E(t *testing.T) {
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	absent := fmt.Sprintf("pooled_absent_%d", time.Now().UnixNano())

	_, err := dbschema.ConnectToDatabase(ctx, withSearchPath(c, pooledURL, absent))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "which does not exist in this database")
	c.Assert(err.Error(), qt.Contains, absent)
}

// TestPostgresPooledProxyExplainsAParameterItCannotCarryE2E is stokaro/ptah#1029's
// eighth criterion, on the refusals that remain refusals.
//
// The schema selection is carried because Ptah knows what it was for. Every
// other startup parameter is the operator's, and dropping one silently would
// run their command under settings they did not ask for. So it stays a failure,
// and the message says which parameter and whose configuration refused it.
//
// Measured against PgBouncer 1.25.2 in transaction mode: `statement_timeout`
// and `default_transaction_isolation` are outside its allow-list and produce
// the same 08P01; `TimeZone` and `application_name` are inside it and connect.
func TestPostgresPooledProxyExplainsAParameterItCannotCarryE2E(t *testing.T) {
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	working, err := dbschema.ConnectToDatabase(ctx, pooledURL)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(working)

	_, err = dbschema.ConnectToDatabase(ctx, withParameter(c, pooledURL, "statement_timeout", "5000"))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `carries the startup parameter "statement_timeout"`)
	// The server's own words survive, so nobody has to take Ptah's account of
	// the failure on trust.
	c.Assert(err.Error(), qt.Contains, "unsupported startup parameter")
}

// withSearchPath selects a schema the way an operator does.
func withSearchPath(c *qt.C, address, schema string) string {
	c.Helper()
	return withParameter(c, address, "search_path", schema)
}

// withParameter puts one query parameter on a URL, which pgx sends in the
// startup packet.
func withParameter(c *qt.C, address, name, value string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set(name, value)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
