// Package mysql gives migration/migrator what MySQL and MariaDB do
// differently from what their descriptors state: the catalog queries about
// the migrator's own tables, which resolve an unconfigured schema to the
// connected database, the widening of an older revision table's version
// column, and the session variables that bound a migration's waits, which
// the two engines name differently.
package mysql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/migratorext"
	"ptah.run/migration/migrationfile"
)

// MySQL returns MySQL's services.
func MySQL() migratorext.Services {
	return migratorext.Services{
		Dialects:          []string{platform.MySQL},
		Catalog:           catalog{},
		VersionColumn:     catalog{},
		Timeouts:          timeouts{statementVariable: "max_execution_time", statementValue: millis},
		ReadOnlyChecks:    true,
		AutocommitControl: true,
		// MySQL shows a table's triggers in information_schema only to an
		// account holding TRIGGER on it.
		TriggerGrant: true,
	}
}

// MariaDB returns MariaDB's services.
func MariaDB() migratorext.Services {
	return migratorext.Services{
		Dialects:          []string{platform.MariaDB},
		Catalog:           catalog{},
		VersionColumn:     catalog{},
		Timeouts:          timeouts{statementVariable: "max_statement_time", statementValue: seconds},
		ReadOnlyChecks:    true,
		AutocommitControl: true,
	}
}

type catalog struct{}

// schema is the configured schema, or the database the connection selected.
func schema(table migratorext.Table) string {
	if table.ConfiguredSchema != "" {
		return table.ConfiguredSchema
	}
	return strings.TrimSpace(table.ConnectionSchema)
}

// PresenceQuery asks information_schema in the configured schema or the
// connected database.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	return migratorext.InformationSchemaPresence(table.ConfiguredOrConnectionSchema(), table.Name)
}

// ColumnExists asks information_schema in the configured schema or the
// connected database.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	return migratorext.InformationSchemaColumnExists(ctx, q,
		sqlutil.Rebind(platform.MySQL, migratorext.InformationSchemaColumnsQuery), schema(table), table, column)
}

// WidenVersion widens a version column an older layout created narrower than
// BIGINT.
func (catalog) WidenVersion(ctx context.Context, q migratorext.Queryer, table migratorext.Table) error {
	var dataType string
	if err := q.QueryRowContext(ctx, `SELECT data_type
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = 'version'`,
		schema(table),
		table.Name,
	).Scan(&dataType); err != nil {
		return fmt.Errorf("failed to inspect migrations version column: %w", err)
	}
	return migratorext.WidenVersion(ctx, q, dataType,
		fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s BIGINT NOT NULL", table.Qualified, table.Quote("version")))
}

// timeouts sets and restores session variables, which are session-scoped
// already and serve a migration inside a transaction and outside one alike.
type timeouts struct {
	// statementVariable bounds a statement: max_execution_time on MySQL, in
	// milliseconds, and max_statement_time on MariaDB, in seconds.
	statementVariable string
	statementValue    func(time.Duration) string
}

func millis(duration time.Duration) string {
	return strconv.FormatInt(migratorext.Millis(duration), 10)
}

func seconds(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'f', -1, 64)
}

func (t timeouts) Statements(timeouts migrationfile.Timeouts, _ bool) (setup, restore []string, err error) {
	setup = make([]string, 0, 6)
	restore = make([]string, 0, 3)
	if timeouts.HasLockTimeout {
		lockSetup, lockRestore := lockTimeoutStatements(timeouts.LockTimeout)
		setup = append(setup, lockSetup...)
		restore = append(restore, lockRestore...)
	}
	if timeouts.HasStatementTimeout {
		setup = append(setup,
			"SET @ptah_prev_"+t.statementVariable+" = @@SESSION."+t.statementVariable,
			"SET SESSION "+t.statementVariable+" = "+t.statementValue(timeouts.StatementTimeout),
		)
		restore = append(restore, "SET SESSION "+t.statementVariable+" = @ptah_prev_"+t.statementVariable)
	}
	return setup, migratorext.Reversed(restore), nil
}

// lockTimeoutStatements bounds both lock waits a MySQL or MariaDB statement
// can make, in the order they are restored.
//
// innodb_lock_wait_timeout bounds a wait for a row lock and nothing else. DDL
// waits for a metadata lock, and lock_wait_timeout bounds that wait; its
// server default is a year on MySQL and a day on MariaDB. Measured on MySQL
// 8.4.11 and MariaDB 11.8.9 with a second session holding an open transaction
// that read the table: under innodb_lock_wait_timeout = 3 alone, DROP TRIGGER
// waited until that transaction ended, 7 and 8 seconds later, and every later
// write to the table queued behind it; under lock_wait_timeout = 3 it failed
// with ERROR 1205 after 3 seconds (stokaro/ptah#4012). Both take whole seconds,
// and a timeout is never zero, so rounding up never asks for no wait.
func lockTimeoutStatements(timeout time.Duration) (setup, restore []string) {
	seconds := strconv.FormatInt(migratorext.Seconds(timeout), 10)
	setup = []string{
		"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
		"SET SESSION innodb_lock_wait_timeout = " + seconds,
		"SET @ptah_prev_lock_wait_timeout = @@SESSION.lock_wait_timeout",
		"SET SESSION lock_wait_timeout = " + seconds,
	}
	restore = []string{
		"SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout",
		"SET SESSION lock_wait_timeout = @ptah_prev_lock_wait_timeout",
	}
	return setup, restore
}
