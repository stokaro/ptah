package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// A registered provider carries the migrations the caller registered, which is
// often the one they want to apply next. The rule that refuses an applied
// revision with no file reads a provider's migrations as the whole history, and
// that reading is only the file-system provider's contract: against a
// registered one it refuses every run after the first (stokaro/ptah#3445).

func TestMigrateUp_ARegisteredProviderIsNotTheWholeHistory(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "registered.db")
	first := migrator.CreateMigrationFromSQL(1, "users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n", "DROP TABLE users;\n")
	second := migrator.CreateMigrationFromSQL(2, "orders",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);\n", "DROP TABLE orders;\n")

	c.Assert(migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(first)).
		MigrateUp(c.Context()), qt.IsNil)
	err := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(second)).
		MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	version, versionErr := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(second)).
		GetCurrentVersion(c.Context())
	c.Assert(versionErr, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))
}
