//go:build integration

package providercli_test

import (
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// An Atlas-shaped history records a failed migration's error without the
// context Ptah adds around it. A provider's execution error is the provider's
// answer: its message gives the outcome and the provider's reason, and it
// unwraps to a sentinel that says only that the execution did not complete.
// The history records the answer, and never the result's message, which the
// error-reasons fixture fills with a fake credential.
func TestProviderAtlasHistoryRecordsTheProvidersReason(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "error-reasons")
	path := filepath.Join(c.TempDir(), "database.db")
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
	conn := connectTransactionAddress(c, host, address)
	migration := migrator.CreateMigrationFromSQL(1, "refused", `CREATE TABLE "refused" ("id" INTEGER);`, `DROP TABLE "refused";`)
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", atlasHistoryTable().Name).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(migrator.MigrationTxModeNone).
		WithoutMigrationLock()

	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorMatches, `(?s).*`+refusedUnitReason+`.*`)

	revisions, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Error, qt.Not(qt.Contains), "hunter2")
	c.Assert(revisions[0].Error, qt.Equals, "provider execution: NOT_APPLIED (0 units confirmed applied): "+refusedUnitReason)
}
