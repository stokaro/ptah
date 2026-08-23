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

	directSchema, err := dbschema.ReadSchemaWithSchemas(direct, []string{"public"})
	c.Assert(err, qt.IsNil)
	pooledSchema, err := dbschema.ReadSchemaWithSchemas(pooled, []string{"public"})
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

// TestPostgresPooledProxyExplainsTheRefusedSearchPathE2E is stokaro/ptah#1029's
// eighth criterion for the one failure that reaches an operator before
// anything else: a diagnostic that distinguishes proxy incompatibility from a
// database SQL error.
//
// A `?search_path=` on a PostgreSQL URL is a STARTUP parameter, and PgBouncer
// refuses the connection outright rather than ignoring it. What the operator
// saw was the proxy's own message about its own configuration, arriving where
// they asked to read a schema:
//
//	FATAL: unsupported startup parameter: search_path (SQLSTATE 08P01)
//
// The control is the same URL without the parameter, against the same proxy:
// it connects. Without it, a refusal from a proxy that was simply down would
// read as this one.
func TestPostgresPooledProxyExplainsTheRefusedSearchPathE2E(t *testing.T) {
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	working, err := dbschema.ConnectToDatabase(ctx, pooledURL)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(working)

	_, err = dbschema.ConnectToDatabase(ctx, withSearchPath(c, pooledURL, "public"))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `selects a schema with "search_path"`)
	c.Assert(err.Error(), qt.Contains, "track_extra_parameters = search_path")
	// The server's own words survive, so nobody has to take Ptah's account of
	// the failure on trust.
	c.Assert(err.Error(), qt.Contains, "unsupported startup parameter")
}

// withSearchPath adds the parameter the proxy refuses.
func withSearchPath(c *qt.C, address, schema string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
