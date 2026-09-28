//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A PostgreSQL or CockroachDB foreign key may reference a table in another
// schema of the same database. A read of the schema holding the key keeps the
// other schema's name, every command that renders the read writes it, and a
// schema file declaring the key compares equal to the database
// (stokaro/ptah#3906). This is the PostgreSQL side of what
// mysql_cross_database_foreign_key_e2e_test.go holds for MySQL and MariaDB,
// and it uses that file's shop schema.
//
// The fixture gives public a `customers` table of its own, so a read that
// dropped the other schema's name would still name a table that exists: the
// output would parse, and the comparison would call a key into `crm` equal to
// a key into public's own table.

// crossSchemaEngines are the PostgreSQL-wire servers the key is measured on.
var crossSchemaEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
}

// crossSchemaKeys is a database of its own holding the schema crm with
// customers, and in public its own customers, orders with the key
// orders_customer into crm's customers, and invoices with the key
// invoices_customer into public's.
type crossSchemaKeys struct {
	url string
}

func newCrossSchemaKeys(c *qt.C, engine dbtarget.Engine) crossSchemaKeys {
	c.Helper()
	keys := newCrossSchemaDatabase(c, engine, "public_tables")
	for _, statement := range []string{
		"CREATE TABLE customers (id bigint PRIMARY KEY)",
		"CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, " +
			"CONSTRAINT orders_customer FOREIGN KEY (customer_id) REFERENCES crm.customers (id))",
		"CREATE TABLE invoices (id bigint PRIMARY KEY, customer_id bigint NOT NULL, " +
			"CONSTRAINT invoices_customer FOREIGN KEY (customer_id) REFERENCES customers (id))",
	} {
		execPostgresFamilySQL(c, c.Context(), keys.url, statement)
	}
	return keys
}

// newCrossSchemaDatabase creates a database, dropped when the test ends,
// holding crm.customers and nothing in public.
func newCrossSchemaDatabase(c *qt.C, engine dbtarget.Engine, prefix string) crossSchemaKeys {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_xschema_%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	keys := crossSchemaKeys{url: replaceDatabaseName(c, adminURL, name)}
	for _, statement := range []string{
		"CREATE SCHEMA crm",
		"CREATE TABLE crm.customers (id bigint PRIMARY KEY)",
	} {
		execPostgresFamilySQL(c, c.Context(), keys.url, statement)
	}
	return keys
}

// withURLParameter adds name=value to a database URL's query.
func withURLParameter(rawURL, name, value string) string {
	return rawURL + map[bool]string{true: "&", false: "?"}[strings.Contains(rawURL, "?")] + name + "=" + value
}

// crossSchemaSQLReference is how SQL output writes the key into crm.
const crossSchemaSQLReference = `CONSTRAINT "orders_customer" FOREIGN KEY ("customer_id") REFERENCES "crm"."customers"("id")`

// TestReadingAPostgresKeyIntoAnotherSchemaE2E holds each command that renders
// a read of public to the other schema's name: a read with no schema list, one
// the list scopes to public, and one the URL's search_path scopes, in SQL and
// in HCL.
func TestReadingAPostgresKeyIntoAnotherSchemaE2E(t *testing.T) {
	tests := []struct {
		name string
		args func(url string) []string
		want string
	}{
		{
			name: "db read",
			args: func(url string) []string { return []string{"db", "read", "--db-url", url} },
			want: crossSchemaSQLReference,
		},
		{
			name: "schema inspect of public, sql",
			args: func(url string) []string {
				return []string{"schema", "inspect", "--db-url", url, "--schemas", "public", "--format", "sql"}
			},
			want: crossSchemaSQLReference,
		},
		{
			name: "schema inspect of public, hcl",
			args: func(url string) []string {
				return []string{"schema", "inspect", "--db-url", url, "--schemas", "public"}
			},
			want: "ref_columns = [table.crm.customers.column.id]",
		},
		{
			name: "schema inspect of the search_path, sql",
			args: func(url string) []string {
				return []string{"schema", "inspect", "--db-url", withURLParameter(url, "search_path", "public"), "--format", "sql"}
			},
			want: crossSchemaSQLReference,
		},
	}

	for _, engine := range crossSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossSchemaKeys(c, engine.engine)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)

					got := runPtahIn(c, test.args(keys.url)...)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stdout, qt.Contains, test.want)
				})
			}
		})
	}
}

// TestSchemaCompareFindsAPostgresKeyIntoAnotherSchemaSyncedE2E compares the
// database with a schema file declaring the same keys, and with what
// `schema inspect` wrote for public. Each is synced.
func TestSchemaCompareFindsAPostgresKeyIntoAnotherSchemaSyncedE2E(t *testing.T) {
	for _, engine := range crossSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossSchemaKeys(c, engine.engine)
			inspected := filepath.Join(c.TempDir(), "inspected")
			c.Assert(os.MkdirAll(inspected, 0o750), qt.IsNil)
			sources := []string{writeShopSchema(c, "crm.customers", "customers")}
			for _, format := range []string{"sql", "hcl"} {
				described := runPtahIn(c, "schema", "inspect", "--db-url", keys.url, "--schemas", "public",
					"--exclude", "*[type=role]", "--format", format)
				c.Assert(described.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", described.Stderr))
				path := filepath.Join(inspected, "public."+format)
				c.Assert(os.WriteFile(path, []byte(described.Stdout), 0o600), qt.IsNil)
				sources = append(sources, path)
			}

			for _, source := range sources {
				got := runPtahIn(c, "schema", "compare", "--db-url", keys.url, "--schema-file", source, "--exit-code")

				c.Assert(got.ExitCode, qt.Equals, 0,
					qt.Commentf("source %s\nstdout:\n%s\nstderr:\n%s", source, got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "No schema differences detected.\n")
			}
		})
	}
}

// TestSchemaApplyCreatesAPostgresKeyIntoAnotherSchemaE2E applies the schema
// file to public, empty, of a database holding crm.customers, and reads the
// keys back from the server: the one declared into crm references crm, and the
// unqualified one references public. The next apply has nothing to do.
//
// The URL's search_path scopes the run to public. A URL naming no schema covers
// every schema, crm included, and the test below holds that case to a refusal.
func TestSchemaApplyCreatesAPostgresKeyIntoAnotherSchemaE2E(t *testing.T) {
	for _, engine := range crossSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := newCrossSchemaDatabase(c, engine.engine, "empty_public")
			target := withURLParameter(database.url, "search_path", "public")
			schema := writeShopSchema(c, "crm.customers", "customers")

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database.url))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			var referenced string
			err = db.QueryRowContext(c.Context(), `
				SELECT string_agg(k.conname || ' -> ' || n.nspname || '.' || t.relname, ', ' ORDER BY k.conname)
				FROM pg_constraint AS k
				JOIN pg_class AS t ON t.oid = k.confrelid
				JOIN pg_namespace AS n ON n.oid = t.relnamespace
				WHERE k.contype = 'f'`).Scan(&referenced)
			c.Assert(err, qt.IsNil)
			c.Assert(referenced, qt.Equals, "invoices_customer -> public.customers, orders_customer -> crm.customers")

			reapplied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(reapplied.ExitCode, qt.Equals, 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied.Stdout, reapplied.Stderr))
			c.Assert(reapplied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSchemaApplyRefusesAPostgresKeyIntoASchemaItWouldDropE2E applies the same
// file through a URL that names no schema, which covers crm. The file declares
// nothing of crm, so the plan would drop crm.customers while adding a key into
// it; the run refuses the key by name before the server changes, and public
// stays empty.
func TestSchemaApplyRefusesAPostgresKeyIntoASchemaItWouldDropE2E(t *testing.T) {
	for _, engine := range crossSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := newCrossSchemaDatabase(c, engine.engine, "whole_database")
			schema := writeShopSchema(c, "crm.customers", "customers")

			got := runPtahIn(c, "schema", "apply", "--db-url", database.url, "--schema-file", schema, "--auto-approve")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `invalid foreign key: constraint "orders_customer" references unknown table `+
				`"crm.customers": the database compared holds schema "crm" and the desired schema declares nothing of it`)
			c.Assert(postgresFamilyObjectCount(c, c.Context(), database.url), qt.Equals, 0)
		})
	}
}

// TestSchemaCompareReportsAPostgresKeyThatReferencesTheOtherTableE2E is the
// control for the tests above: a key into crm and a key into public's own
// customers are two keys, in both directions. The file swaps the targets of
// the two keys, and the plan drops and adds both.
func TestSchemaCompareReportsAPostgresKeyThatReferencesTheOtherTableE2E(t *testing.T) {
	for _, engine := range crossSchemaEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossSchemaKeys(c, engine.engine)
			schema := writeShopSchema(c, "customers", "crm.customers")

			got := runPtahIn(c, "schema", "compare", "--db-url", keys.url, "--schema-file", schema, "--exit-code")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains,
				"constraints_added (2): invoices_customer invoices FOREIGN KEY crm.customers, orders_customer orders FOREIGN KEY customers\n")
			c.Assert(got.Stdout, qt.Contains,
				"constraints_removed (2): invoices_customer invoices FOREIGN KEY, orders_customer orders FOREIGN KEY\n")
			c.Assert(got.Stdout, qt.Contains,
				`ADD CONSTRAINT "orders_customer" FOREIGN KEY ("customer_id") REFERENCES "customers"("id")`)
			c.Assert(got.Stdout, qt.Contains,
				`ADD CONSTRAINT "invoices_customer" FOREIGN KEY ("customer_id") REFERENCES "crm"."customers"("id")`)
		})
	}
}
