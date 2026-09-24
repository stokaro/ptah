package migrator

// White-box testing required: the statements a migration's timeouts become are
// chosen by the unexported timeoutStatements, and the only black-box view of
// them is a live server, where integration/migrator reads the setting back.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/migration/migrationfile"
)

// A no_transaction migration runs each statement in its own implicit
// transaction, where `SET LOCAL` is a warning that changes nothing. The session
// spelling is what bounds those statements, so the PostgreSQL family must not
// be handed the transaction spelling for this scope.
func TestTimeoutStatements_SessionScopeSetsAndResetsThePostgresSession(t *testing.T) {
	timeouts := migrationfile.Timeouts{
		LockTimeout:         3 * time.Second,
		StatementTimeout:    30 * time.Second,
		HasLockTimeout:      true,
		HasStatementTimeout: true,
	}
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			setup, restore, err := timeoutStatements(
				dialect, capability.ForDialect(dialect), timeouts, timeoutScopeSession)

			c.Assert(err, qt.IsNil)
			c.Assert(setup, qt.DeepEquals, []string{
				"SET lock_timeout = '3000ms'",
				"SET statement_timeout = '30000ms'",
			})
			c.Assert(restore, qt.DeepEquals, []string{
				"RESET statement_timeout",
				"RESET lock_timeout",
			})
		})
	}
}

// Only the timeout a migration declares is set, and only that one is reset: a
// reset of a setting nobody changed would discard a value the session was
// opened with.
func TestTimeoutStatements_SessionScopeResetsOnlyWhatItSet(t *testing.T) {
	c := qt.New(t)

	setup, restore, err := timeoutStatements(
		platform.Postgres,
		capability.ForDialect(platform.Postgres),
		migrationfile.Timeouts{LockTimeout: 1500 * time.Millisecond, HasLockTimeout: true},
		timeoutScopeSession,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(setup, qt.DeepEquals, []string{"SET lock_timeout = '1500ms'"})
	c.Assert(restore, qt.DeepEquals, []string{"RESET lock_timeout"})
}

// The MySQL family already sets session variables and restores the values it
// found, so one spelling serves a migration inside a transaction and one
// outside it.
func TestTimeoutStatements_MySQLFamilySpellingDoesNotDependOnScope(t *testing.T) {
	timeouts := migrationfile.Timeouts{
		LockTimeout:         2 * time.Second,
		StatementTimeout:    5 * time.Second,
		HasLockTimeout:      true,
		HasStatementTimeout: true,
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.ForDialect(dialect)

			transactionSetup, transactionRestore, err := timeoutStatements(dialect, caps, timeouts, timeoutScopeTransaction)
			c.Assert(err, qt.IsNil)
			sessionSetup, sessionRestore, err := timeoutStatements(dialect, caps, timeouts, timeoutScopeSession)
			c.Assert(err, qt.IsNil)

			c.Assert(sessionSetup, qt.DeepEquals, transactionSetup)
			c.Assert(sessionRestore, qt.DeepEquals, transactionRestore)
			c.Assert(sessionRestore, qt.HasLen, 2)
		})
	}
}
