//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A MySQL or MariaDB foreign key may reference a table in another database. A
// read of the database holding the key keeps the other database's name, every
// command that renders the read writes it, and a schema file declaring the key
// compares equal to the database (stokaro/ptah#3891).
//
// The fixture gives the key's database a `customers` table of its own, so a
// read that dropped the other database's name would still name a table that
// exists: the output would parse, and the comparison would call a key into
// `crm` equal to a key into the database's own table.

// crossDatabaseKeys is a server holding two databases. crm holds customers.
// shop holds its own customers, orders with the key orders_customer into crm's
// customers, and invoices with the key invoices_customer into its own.
type crossDatabaseKeys struct {
	scratch   mysqlScratch
	crm, shop string
	shopURL   string
	serverURL string
}

func newCrossDatabaseKeys(c *qt.C, engine dbtarget.Engine) crossDatabaseKeys {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	// crm is created first so that it is dropped last: the server refuses to
	// drop a table another database's key references.
	crm, _ := scratch.database(c, "xdb_crm")
	shop, shopURL := scratch.database(c, "xdb_shop")
	for _, statement := range []string{
		"CREATE TABLE `" + crm + "`.customers (id bigint PRIMARY KEY)",
		"CREATE TABLE `" + shop + "`.customers (id bigint PRIMARY KEY)",
		"CREATE TABLE `" + shop + "`.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL, " +
			"CONSTRAINT orders_customer FOREIGN KEY (customer_id) REFERENCES `" + crm + "`.customers (id))",
		"CREATE TABLE `" + shop + "`.invoices (id bigint PRIMARY KEY, customer_id bigint NOT NULL, " +
			"CONSTRAINT invoices_customer FOREIGN KEY (customer_id) REFERENCES customers (id))",
	} {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
	c.Assert(err, qt.IsNil)
	return crossDatabaseKeys{scratch: scratch, crm: crm, shop: shop, shopURL: shopURL, serverURL: server}
}

// shopSchema declares shop's tables. ordersTarget and invoicesTarget are the
// tables the two keys reference, as the file spells them.
func shopSchema(ordersTarget, invoicesTarget string) string {
	return "CREATE TABLE customers (id bigint PRIMARY KEY);\n" +
		"CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL,\n" +
		"  CONSTRAINT orders_customer FOREIGN KEY (customer_id) REFERENCES " + ordersTarget + " (id));\n" +
		"CREATE TABLE invoices (id bigint PRIMARY KEY, customer_id bigint NOT NULL,\n" +
		"  CONSTRAINT invoices_customer FOREIGN KEY (customer_id) REFERENCES " + invoicesTarget + " (id));\n"
}

// writeShopSchema writes shopSchema into a fresh directory and returns the
// file's path.
func writeShopSchema(c *qt.C, ordersTarget, invoicesTarget string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "shop.sql")
	c.Assert(os.WriteFile(path, []byte(shopSchema(ordersTarget, invoicesTarget)), 0o600), qt.IsNil)
	return path
}

// runPtahIn runs the native binary and returns what it printed and its exit
// status.
func runPtahIn(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
}

// TestReadingAMySQLKeyIntoAnotherDatabaseE2E holds each command that renders a
// read of the key's database to the other database's name. A read of the
// database alone and a read of the whole server selecting that database are
// the same scope, so they give the same answer, in SQL and in HCL.
func TestReadingAMySQLKeyIntoAnotherDatabaseE2E(t *testing.T) {
	tests := []struct {
		name string
		args func(keys crossDatabaseKeys) []string
		// want is what the output must carry, with {crm} for crm's name.
		want string
	}{
		{
			name: "db read",
			args: func(keys crossDatabaseKeys) []string { return []string{"db", "read", "--db-url", keys.shopURL} },
			want: crossDatabaseSQLReference,
		},
		{
			name: "schema inspect, sql",
			args: func(keys crossDatabaseKeys) []string {
				return []string{"schema", "inspect", "--db-url", keys.shopURL, "--format", "sql"}
			},
			want: crossDatabaseSQLReference,
		},
		{
			name: "schema inspect, hcl",
			args: func(keys crossDatabaseKeys) []string {
				return []string{"schema", "inspect", "--db-url", keys.shopURL}
			},
			want: crossDatabaseHCLReference,
		},
		{
			name: "schema inspect of the whole server, the key's database, sql",
			args: func(keys crossDatabaseKeys) []string {
				return []string{"schema", "inspect", "--db-url", keys.serverURL, "--schemas", keys.shop, "--format", "sql"}
			},
			want: crossDatabaseSQLReference,
		},
		{
			name: "schema inspect of the whole server, the key's database, hcl",
			args: func(keys crossDatabaseKeys) []string {
				return []string{"schema", "inspect", "--db-url", keys.serverURL, "--schemas", keys.shop}
			},
			want: crossDatabaseHCLReference,
		},
		{
			name: "schema inspect of the whole server, both databases, sql",
			args: func(keys crossDatabaseKeys) []string {
				return []string{
					"schema", "inspect", "--db-url", keys.serverURL,
					"--schemas", keys.crm + "," + keys.shop, "--format", "sql",
				}
			},
			want: crossDatabaseSQLReference,
		},
	}

	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossDatabaseKeys(c, engine.admin)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)

					got := runPtahIn(c, test.args(keys)...)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stdout, qt.Contains, strings.ReplaceAll(test.want, "{crm}", keys.crm))
				})
			}
		})
	}
}

// crossDatabaseSQLReference is how SQL output writes the key into crm, and
// crossDatabaseHCLReference how HCL output writes it, with {crm} for crm's
// name.
const (
	crossDatabaseSQLReference = "CONSTRAINT `orders_customer` FOREIGN KEY (`customer_id`) REFERENCES `{crm}`.`customers`(`id`)"
	crossDatabaseHCLReference = "ref_columns = [table.{crm}.customers.column.id]"
)

// TestSchemaCompareFindsAMySQLKeyIntoAnotherDatabaseSyncedE2E compares the
// database with a schema file declaring the same keys, and with what
// `schema inspect` wrote for it. Each is synced.
//
// The inspection leaves roles out. A role is server-wide, so a description of
// shop carries every role another test holds at that moment, and one dropped
// before the comparison reads as a difference this test is not about. The SQL
// form also cannot be read back while it holds one (stokaro/ptah#3902).
func TestSchemaCompareFindsAMySQLKeyIntoAnotherDatabaseSyncedE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossDatabaseKeys(c, engine.admin)
			inspected := filepath.Join(c.TempDir(), "inspected")
			c.Assert(os.MkdirAll(inspected, 0o750), qt.IsNil)
			sources := []string{writeShopSchema(c, keys.crm+".customers", "customers")}
			for _, format := range []string{"sql", "hcl"} {
				described := runPtahIn(c, "schema", "inspect", "--db-url", keys.shopURL,
					"--exclude", "*[type=role]", "--format", format)
				c.Assert(described.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", described.Stderr))
				path := filepath.Join(inspected, "shop."+format)
				c.Assert(os.WriteFile(path, []byte(described.Stdout), 0o600), qt.IsNil)
				sources = append(sources, path)
			}

			for _, source := range sources {
				got := runPtahIn(c, "schema", "compare", "--db-url", keys.shopURL, "--schema-file", source, "--exit-code")

				c.Assert(got.ExitCode, qt.Equals, 0,
					qt.Commentf("source %s\nstdout:\n%s\nstderr:\n%s", source, got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "No schema differences detected.\n")
			}
		})
	}
}

// TestSchemaApplyCreatesAMySQLKeyIntoAnotherDatabaseE2E applies the schema
// file to an empty database and reads the keys back from the server: the one
// declared into crm references crm, and the unqualified one references the
// database it was applied to. The next apply has nothing to do.
func TestSchemaApplyCreatesAMySQLKeyIntoAnotherDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossDatabaseKeys(c, engine.admin)
			target, targetURL := keys.scratch.database(c, "xdb_target")
			schema := writeShopSchema(c, keys.crm+".customers", "customers")

			applied := runPtahIn(c, "schema", "apply", "--db-url", targetURL, "--schema-file", schema, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			var referenced string
			err := keys.scratch.admin.QueryRowContext(c.Context(), `
				SELECT GROUP_CONCAT(CONCAT(CONSTRAINT_NAME, ' -> ', REFERENCED_TABLE_SCHEMA, '.', REFERENCED_TABLE_NAME)
					ORDER BY CONSTRAINT_NAME SEPARATOR ', ')
				FROM information_schema.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL`, target).Scan(&referenced)
			c.Assert(err, qt.IsNil)
			c.Assert(referenced, qt.Equals,
				"invoices_customer -> "+target+".customers, orders_customer -> "+keys.crm+".customers")

			reapplied := runPtahIn(c, "schema", "apply", "--db-url", targetURL, "--schema-file", schema, "--auto-approve")

			c.Assert(reapplied.ExitCode, qt.Equals, 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied.Stdout, reapplied.Stderr))
			c.Assert(reapplied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSchemaCompareReportsAMySQLKeyThatReferencesTheOtherTableE2E is the
// control for the tests above: a key into crm and a key into the database's
// own customers are two keys, in both directions. The file swaps the targets
// of the two keys, and the plan drops and adds both.
func TestSchemaCompareReportsAMySQLKeyThatReferencesTheOtherTableE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			keys := newCrossDatabaseKeys(c, engine.admin)
			schema := writeShopSchema(c, "customers", keys.crm+".customers")

			got := runPtahIn(c, "schema", "compare", "--db-url", keys.shopURL, "--schema-file", schema, "--exit-code")

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "constraints_added (2): invoices_customer invoices FOREIGN KEY "+
				keys.crm+".customers, orders_customer orders FOREIGN KEY customers\n")
			c.Assert(got.Stdout, qt.Contains,
				"constraints_removed (2): invoices_customer invoices FOREIGN KEY, orders_customer orders FOREIGN KEY\n")
			c.Assert(got.Stdout, qt.Contains,
				"ADD CONSTRAINT `orders_customer` FOREIGN KEY (`customer_id`) REFERENCES `customers`(`id`)")
			c.Assert(got.Stdout, qt.Contains,
				"ADD CONSTRAINT `invoices_customer` FOREIGN KEY (`customer_id`) REFERENCES `"+keys.crm+"`.`customers`(`id`)")
		})
	}
}
