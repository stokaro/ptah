//go:build integration

package providercli_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/providerhost"
	"ptah.run/migration/migrator"
)

func transactionConnection(c *qt.C, host *providerhost.Host, fault string) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "transactions.db")), "/"), RawQuery: url.Values{"fault": {fault}}.Encode()}).String()
	return connectTransactionAddress(c, host, address), address
}
func connectTransactionAddress(c *qt.C, host *providerhost.Host, address string) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	connection, err := dbschema.ConnectToDatabase(ctx, address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	return connection
}
func providerTablePresent(c *qt.C, connection *dbschema.DatabaseConnection, name string) bool {
	c.Helper()
	schema, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	return slices.ContainsFunc(schema.Tables, func(table catalog.Table) bool { return table.Name == name })
}

func TestProviderTransactionsCommitRollbackAndClose(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	t.Run("commit survives RPC lifetime and repeated finish", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "")
		c.Assert(connection.Info().Capabilities.Has(capability.TransactionalDDL), qt.IsTrue)
		tx, err := connection.SchemaWriter().BeginTransaction(c.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(tx.IsDryRun(), qt.IsFalse)
		c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "committed" ("id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
		c.Assert(tx.Commit(), qt.IsNil)
		c.Assert(tx.Commit(), qt.IsNil)
		c.Assert(tx.Rollback(), qt.ErrorIs, provider.ErrExecution)
		c.Assert(providerTablePresent(c, connection, "committed"), qt.IsTrue)
		c.Assert(tx.ExecuteSQL(c.Context(), `DROP TABLE "committed";`), qt.IsNotNil)
		c.Assert(providerTablePresent(c, connection, "committed"), qt.IsTrue)
	})
	t.Run("rollback removes actual schema changes", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "")
		tx, err := connection.SchemaWriter().BeginTransaction(c.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "rolled_back" ("id" INTEGER);`), qt.IsNil)
		c.Assert(tx.Rollback(), qt.IsNil)
		c.Assert(tx.Rollback(), qt.IsNil)
		c.Assert(tx.Commit(), qt.ErrorIs, provider.ErrExecution)
		c.Assert(providerTablePresent(c, connection, "rolled_back"), qt.IsFalse)
	})
	t.Run("closing a session releases an unfinished transaction", func(t *testing.T) {
		c := qt.New(t)
		connection, address := transactionConnection(c, host, "")
		tx, err := connection.SchemaWriter().BeginTransaction(c.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "abandoned" ("id" INTEGER);`), qt.IsNil)
		c.Assert(connection.Close(), qt.IsNil)
		c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "late" ("id" INTEGER);`), qt.IsNotNil)
		fresh := connectTransactionAddress(c, host, address)
		c.Assert(providerTablePresent(c, fresh, "abandoned"), qt.IsFalse)
		c.Assert(providerTablePresent(c, fresh, "late"), qt.IsFalse)
	})
}

func TestProviderTransactionsRefuseCommitAfterFailureAndPreserveUnknownCommit(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "transactions")
	t.Run("failed statement prevents partial commit", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "")
		tx, err := connection.SchemaWriter().BeginTransaction(c.Context())
		c.Assert(err, qt.IsNil)
		statement := `CREATE TABLE "partial" ("id" INTEGER);`
		c.Assert(tx.ExecuteSQL(c.Context(), statement), qt.IsNil)
		c.Assert(tx.ExecuteSQL(c.Context(), statement), qt.ErrorIs, provider.ErrOutcomeUnknown)
		c.Assert(tx.Commit(), qt.ErrorIs, provider.ErrOutcomeUnknown)
		c.Assert(tx.Rollback(), qt.IsNil)
		c.Assert(providerTablePresent(c, connection, "partial"), qt.IsFalse)
	})
	t.Run("lost commit reply is not retried or reported as rollback", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "lost-commit")
		tx, err := connection.SchemaWriter().BeginTransaction(c.Context())
		c.Assert(err, qt.IsNil)
		c.Assert(tx.ExecuteSQL(c.Context(), `CREATE TABLE "committed" ("id" INTEGER);`), qt.IsNil)
		c.Assert(tx.Commit(), qt.ErrorIs, provider.ErrOutcomeUnknown)
		c.Assert(tx.Commit(), qt.ErrorIs, provider.ErrOutcomeUnknown)
		c.Assert(tx.Rollback(), qt.ErrorIs, provider.ErrExecution)
		c.Assert(providerTablePresent(c, connection, "committed"), qt.IsTrue)
	})
	t.Run("rollback remains available after caller cancellation", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "")
		ctx, cancel := context.WithCancel(c.Context())
		defer cancel()
		tx, err := connection.SchemaWriter().BeginTransaction(ctx)
		c.Assert(err, qt.IsNil)
		c.Assert(tx.ExecuteSQL(ctx, `CREATE TABLE "canceled" ("id" INTEGER);`), qt.IsNil)
		cancel()
		c.Assert(tx.Commit(), qt.ErrorIs, context.Canceled)
		c.Assert(tx.Rollback(), qt.IsNil)
		c.Assert(providerTablePresent(c, connection, "canceled"), qt.IsFalse)
	})
	t.Run("shared apply preserves a lost rollback reply", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "reject-rollback")
		err := atlasschema.ApplyStatements(c.Context(), connection, migrator.MigrationTxModeAll, []string{`CREATE TABLE "prefix" ("id" INTEGER);`, "DROP TABLE refused;"})
		c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(providerTablePresent(c, connection, "prefix"), qt.IsFalse)
	})
	t.Run("shared apply rolls back the executed prefix", func(t *testing.T) {
		c := qt.New(t)
		connection, _ := transactionConnection(c, host, "")
		err := atlasschema.ApplyStatements(c.Context(), connection, migrator.MigrationTxModeAll, []string{`CREATE TABLE "prefix" ("id" INTEGER);`, `CREATE TABLE "prefix" ("id" INTEGER);`})
		c.Assert(err, qt.IsNotNil)
		c.Assert(providerTablePresent(c, connection, "prefix"), qt.IsFalse)
	})
}

func TestProviderTransactionsThroughNativeCLI(t *testing.T) {
	c := qt.New(t)
	executable := independentCLIProviderCommand(c, "transactions")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	spec := providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
	t.Run("lost commit reply stays unknown in JSON", func(t *testing.T) {
		c := qt.New(t)
		fixture := newProviderCLI(c, spec)
		fixture.plan(c)
		fixture.address += "?fault=lost-commit"
		output, diagnostics, code := fixture.run(c, c.Context(), "apply", "--plan", fixture.planPath, "--tx-mode", "all", "--auto-approve", "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(output, qt.Contains, `"outcome": "unknown"`)
		c.Assert(diagnostics, qt.Contains, "UNKNOWN")
		fixture.address = strings.Split(fixture.address, "?")[0]
		c.Assert(fixture.success(c, "inspect", "--format", "json"), qt.Contains, `"name":"items"`)
	})
	for _, mode := range []string{"file", "all"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			fixture := newProviderCLI(c, spec)
			fixture.plan(c)
			applied := fixture.success(c, "apply", "--plan", fixture.planPath, "--tx-mode", mode, "--auto-approve", "--json")
			c.Assert(applied, qt.Contains, `"outcome": "applied"`)
			observed := fixture.success(c, "inspect", "--format", "json")
			c.Assert(observed, qt.Contains, `"name":"items"`)
			c.Assert(observed, qt.Contains, `"name":"second"`)
			c.Assert(fixture.plan(c), qt.Contains, `"outcome": "no-changes"`)
		})
	}
}
