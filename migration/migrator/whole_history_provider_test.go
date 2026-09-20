package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// registered_provider_history_test.go covers the in-memory provider, which is
// the one this package ships for a caller to declare. These are the same rule
// asked of an embedder's own provider, which reaches it through the exported
// [migrator.WholeHistoryProvider], and of the read-only verifier, which a
// caller runs before it writes anything.

// Both providers this package ships satisfy the exported interface, so an
// embedder reading it sees the contract the rule asks through.
var (
	_ migrator.WholeHistoryProvider = (*migrator.RegisteredMigrationProvider)(nil)
	_ migrator.WholeHistoryProvider = (*migrator.FSMigrationProvider)(nil)
)

// partialHistoryProvider carries one run's migrations and promises nothing
// about the rest, which is what an embedder assembling a step writes.
type partialHistoryProvider struct {
	migrations []*migrator.Migration
}

func (p *partialHistoryProvider) Migrations() []*migrator.Migration { return p.migrations }

// customWholeHistoryProvider is an embedder's own provider that does promise to
// be the whole history.
type customWholeHistoryProvider struct {
	migrations []*migrator.Migration
}

func (p *customWholeHistoryProvider) Migrations() []*migrator.Migration { return p.migrations }

func (p *customWholeHistoryProvider) DescribesWholeHistory() bool { return true }

// wholeHistoryFixture is the downgrade the rule exists to catch: a database
// that recorded two migrations, and a binary that carries only the first.
type wholeHistoryFixture struct {
	first  *migrator.Migration
	second *migrator.Migration
}

func newWholeHistoryFixture() wholeHistoryFixture {
	return wholeHistoryFixture{
		first: migrator.CreateMigrationFromSQL(1, "users",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n", "DROP TABLE users;\n"),
		second: migrator.CreateMigrationFromSQL(2, "orders",
			"CREATE TABLE orders (id INTEGER PRIMARY KEY);\n", "DROP TABLE orders;\n"),
	}
}

func TestVerifyAppliedChecksums_ADeclaredRegisteredProviderReportsAMissingMigration(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()
	conn := sqliteConnection(c, "declared-verify.db")
	newer := migrator.NewRegisteredMigrationProvider(fixture.first, fixture.second).AsWholeHistory()
	c.Assert(migrator.NewMigrator(conn, newer).MigrateUp(c.Context()), qt.IsNil)

	older := migrator.NewRegisteredMigrationProvider(fixture.first).AsWholeHistory()
	reconcile, err := migrator.NewMigrator(conn, older).VerifyAppliedChecksums(c.Context())

	var missing *migrator.MissingMigrationError
	c.Assert(err, qt.ErrorAs, &missing)
	c.Assert(missing.RevisionKey, qt.Equals, "2")
	c.Assert(reconcile, qt.IsFalse)
}

func TestVerifyAppliedChecksums_ACustomWholeHistoryProviderReportsAMissingMigration(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()
	conn := sqliteConnection(c, "custom-whole-verify.db")
	newer := &customWholeHistoryProvider{
		migrations: []*migrator.Migration{fixture.first, fixture.second},
	}
	c.Assert(migrator.NewMigrator(conn, newer).MigrateUp(c.Context()), qt.IsNil)

	older := &customWholeHistoryProvider{migrations: []*migrator.Migration{fixture.first}}
	_, err := migrator.NewMigrator(conn, older).VerifyAppliedChecksums(c.Context())

	var missing *migrator.MissingMigrationError
	c.Assert(err, qt.ErrorAs, &missing)
	c.Assert(missing.RevisionKey, qt.Equals, "2")
}

func TestMigrateUp_ACustomWholeHistoryProviderRefusesAMissingMigration(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()
	conn := sqliteConnection(c, "custom-whole-up.db")
	newer := &customWholeHistoryProvider{
		migrations: []*migrator.Migration{fixture.first, fixture.second},
	}
	c.Assert(migrator.NewMigrator(conn, newer).MigrateUp(c.Context()), qt.IsNil)

	older := &customWholeHistoryProvider{migrations: []*migrator.Migration{fixture.first}}
	err := migrator.NewMigrator(conn, older).MigrateUp(c.Context())

	var missing *migrator.MissingMigrationError
	c.Assert(err, qt.ErrorAs, &missing)
	c.Assert(missing.RevisionKey, qt.Equals, "2")
}

// A custom provider that says nothing is left unchecked, the same way an
// undeclared registered one is: an embedder assembling one run's migrations
// would otherwise be refused on every run after the first.
func TestMigrateUp_APartialCustomProviderIsNotAsked(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()
	conn := sqliteConnection(c, "custom-partial.db")
	c.Assert(migrator.NewMigrator(conn,
		&partialHistoryProvider{migrations: []*migrator.Migration{fixture.first}}).
		MigrateUp(c.Context()), qt.IsNil)

	err := migrator.NewMigrator(conn,
		&partialHistoryProvider{migrations: []*migrator.Migration{fixture.second}}).
		MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
}

// A declared provider holding every applied revision stays silent, which is the
// control that the refusals above are about the absence rather than about the
// declaration.
func TestVerifyAppliedChecksums_ADeclaredRegisteredProviderWithEveryFileIsSilent(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()
	conn := sqliteConnection(c, "declared-intact.db")
	provider := migrator.NewRegisteredMigrationProvider(fixture.first, fixture.second).AsWholeHistory()
	c.Assert(migrator.NewMigrator(conn, provider).MigrateUp(c.Context()), qt.IsNil)

	reconcile, err := migrator.NewMigrator(conn, provider).VerifyAppliedChecksums(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsFalse)
	version, versionErr := migrator.NewMigrator(conn, provider).GetCurrentVersion(c.Context())
	c.Assert(versionErr, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))
}

func TestAsWholeHistory_DeclaresTheProvider(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()

	plain := migrator.NewRegisteredMigrationProvider(fixture.first)
	declared := migrator.NewRegisteredMigrationProvider(fixture.first).AsWholeHistory()

	c.Assert(plain.DescribesWholeHistory(), qt.IsFalse)
	c.Assert(declared.DescribesWholeHistory(), qt.IsTrue)
}

func TestAsWholeHistory_SurvivesALaterRegister(t *testing.T) {
	c := qt.New(t)
	fixture := newWholeHistoryFixture()

	provider := migrator.NewRegisteredMigrationProvider(fixture.first).AsWholeHistory()
	provider.Register(fixture.second)

	c.Assert(provider.DescribesWholeHistory(), qt.IsTrue)
	c.Assert(provider.Migrations(), qt.HasLen, 2)
}
