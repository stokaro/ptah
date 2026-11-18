//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// repeatableEngine is a server engine whose descriptor states the record of
// repeatable migrations, and what the server does with a repeatable attempt.
type repeatableEngine struct {
	name    string
	engine  dbtarget.Engine
	dialect string
	// admin is the engine a scratch database is created through: the
	// engine itself in the PostgreSQL family, its administrative account in
	// the MySQL family.
	admin dbtarget.Engine
	// family selects how the scratch database is created.
	family string
	// drop is what DROP DATABASE takes after the name, so the drop does not
	// wait on a session the test left open.
	drop string
	// schema is the expression that names the schema an unqualified name of
	// the test resolves to.
	schema string
	// noTransaction is what the record says of every attempt: the engine
	// rolls schema changes back as a unit, or it runs repeatable migrations
	// without a transaction.
	noTransaction bool
	// keepsPartial is what a failed attempt leaves of its first statement.
	keepsPartial bool
	// replaceView is the statement prefix that creates a view or replaces
	// the one under that name.
	replaceView string
	// fixed creates partial_marker unless it exists.
	fixed string
	// catalogColumns counts the columns of the relation its parameter names,
	// where information_schema does not exist. Empty reads
	// information_schema.columns in schema.
	catalogColumns string
	// clearBody sets body to NULL in every row of the record named {table}.
	// Empty uses UPDATE.
	clearBody string
}

// view is a view over widgets with the given select list, in the engine's
// spelling.
func (e repeatableEngine) view(name, columns string) string {
	return e.replaceView + " " + name + " AS SELECT " + columns + " FROM widgets;\n"
}

// widgetIDs is the view the lifecycle tests apply first, and widerIDs its
// replacement with a second column.
func (e repeatableEngine) widgetIDs() string { return e.view("widget_ids", "id") }

func (e repeatableEngine) widerIDs() string { return e.view("widget_ids", "id, id AS copy") }

var (
	repeatablePostgres = repeatableEngine{
		name: "postgres", engine: dbtarget.PostgreSQL, dialect: "postgres", admin: dbtarget.PostgreSQL,
		family: "postgres", drop: " WITH (FORCE)", schema: "current_schema()",
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
	}
	repeatableCockroachDB = repeatableEngine{
		name: "cockroachdb", engine: dbtarget.CockroachDB, dialect: "cockroachdb", admin: dbtarget.CockroachDB,
		family: "postgres", drop: " CASCADE", schema: "current_schema()", noTransaction: true, keepsPartial: true,
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
	}
	repeatableYugabyteDB = repeatableEngine{
		name: "yugabytedb", engine: dbtarget.YugabyteDB, dialect: "yugabytedb", admin: dbtarget.YugabyteDB,
		family: "postgres", drop: " WITH (FORCE)", schema: "current_schema()", noTransaction: true, keepsPartial: true,
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
	}
	repeatableMySQL = repeatableEngine{
		name: "mysql", engine: dbtarget.MySQL, dialect: "mysql", admin: dbtarget.MySQLAdmin,
		family: "mysql", schema: "DATABASE()", noTransaction: true, keepsPartial: true,
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
	}
	repeatableMariaDB = repeatableEngine{
		name: "mariadb", engine: dbtarget.MariaDB, dialect: "mariadb", admin: dbtarget.MariaDBAdmin,
		family: "mysql", schema: "DATABASE()", noTransaction: true, keepsPartial: true,
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
	}
	// repeatableSQLServer has no CREATE OR REPLACE VIEW and no CREATE TABLE
	// IF NOT EXISTS, so its fixtures say both its own way.
	// repeatableOracle commits its own DDL, so every repeatable migration runs
	// without a transaction, and its migration lock is a row lock on a session
	// of its own. A schema is an account on Oracle, so the scratch database is
	// one.
	repeatableOracle = repeatableEngine{
		name: "oracle", engine: dbtarget.Oracle, dialect: "oracle", admin: dbtarget.OracleAdmin,
		family: "oracle", noTransaction: true, keepsPartial: true,
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
		catalogColumns: "SELECT count(*) FROM user_tab_columns WHERE table_name = UPPER(?)",
	}
	// repeatableClickHouse has no transaction and no migration lock, and its
	// rows change through mutations.
	repeatableClickHouse = repeatableEngine{
		name: "clickhouse", engine: dbtarget.ClickHouse, dialect: "clickhouse", admin: dbtarget.ClickHouse,
		family: "clickhouse", schema: "currentDatabase()", noTransaction: true, keepsPartial: true,
		replaceView: repeatableReplaceView, fixed: repeatableEnginesFixed,
		clearBody: "ALTER TABLE {table} UPDATE body = NULL WHERE 1 SETTINGS mutations_sync = 1",
	}
	repeatableSQLServer = repeatableEngine{
		name: "sqlserver", engine: dbtarget.SQLServer, dialect: "sqlserver", admin: dbtarget.SQLServer,
		family: "sqlserver", schema: "SCHEMA_NAME()", replaceView: "CREATE OR ALTER VIEW",
		fixed: "IF OBJECT_ID(N'partial_marker', N'U') IS NULL CREATE TABLE partial_marker (id INTEGER PRIMARY KEY);\n",
	}

	// repeatableEngines is every server engine that runs repeatable
	// migrations.
	repeatableEngines = []repeatableEngine{
		repeatablePostgres, repeatableCockroachDB, repeatableYugabyteDB, repeatableMySQL, repeatableMariaDB,
		repeatableSQLServer, repeatableOracle, repeatableClickHouse,
	}
	// repeatableLockingEngines take the migration lock. CockroachDB has no
	// session advisory lock, and Ptah runs every migration there without one.
	repeatableLockingEngines = []repeatableEngine{
		repeatablePostgres, repeatableYugabyteDB, repeatableMySQL, repeatableMariaDB, repeatableSQLServer,
		repeatableOracle,
	}
	// repeatableMySQLFamilyEngines read session variables from the URL.
	repeatableMySQLFamilyEngines = []repeatableEngine{repeatableMySQL, repeatableMariaDB}
	// repeatableNonAtomicEngines do not roll schema changes back as a unit.
	repeatableNonAtomicEngines = []repeatableEngine{
		repeatableCockroachDB, repeatableYugabyteDB, repeatableMySQL, repeatableMariaDB, repeatableOracle,
		repeatableClickHouse,
	}
)

const (
	repeatableEnginesTable = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
	repeatableReplaceView  = "CREATE OR REPLACE VIEW"
	// repeatableEnginesFailing creates a table and then fails, so what the
	// server kept of the attempt can be read back.
	// The table has a primary key because CockroachDB gives one without a
	// hidden rowid column, which the catalog lists.
	repeatableEnginesFailing = "CREATE TABLE partial_marker (id INTEGER PRIMARY KEY);\n" +
		"INSERT INTO missing_table VALUES (1);\n"
	repeatableEnginesFixed = "CREATE TABLE IF NOT EXISTS partial_marker (id INTEGER PRIMARY KEY);\n"
)

// repeatableEngineMigrations is one versioned migration and the given
// repeatable migrations, keyed by file name.
func repeatableEngineMigrations(repeatables map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte(repeatableEnginesTable)},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE widgets;\n")},
	}
	for name, body := range repeatables {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// repeatableEngineTarget is a scratch database of one engine and a connection
// to it.
type repeatableEngineTarget struct {
	engine repeatableEngine
	url    string
	conn   *dbschema.DatabaseConnection
}

// newRepeatableEngineTarget creates a scratch database on the engine, which
// the test drops when it ends.
func newRepeatableEngineTarget(c *qt.C, t *testing.T, engine repeatableEngine) *repeatableEngineTarget {
	c.Helper()
	target := &repeatableEngineTarget{engine: engine, url: repeatableScratchDatabaseURL(c, t, engine)}
	conn, err := dbschema.ConnectToDatabase(context.Background(), target.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	target.conn = conn
	return target
}

// repeatableScratchDatabaseURL creates a database of its own for one test.
func repeatableScratchDatabaseURL(c *qt.C, t *testing.T, engine repeatableEngine) string {
	c.Helper()
	if engine.family == "mysql" {
		return mySQLFamilyScratchDatabaseURL(t, engine.dialect, engine.admin, "ptah_rt_repeatable")
	}
	if engine.family == "sqlserver" {
		return sqlServerScratchDatabaseURL(c, engine.admin, "ptah_rt_repeatable")
	}
	if engine.family == "oracle" {
		return oracleScratchAccountURL(c, engine.admin, "ptah_rt_repeatable")
	}
	adminURL := dbtarget.URL(c, engine.admin)
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	database := fmt.Sprintf("ptah_rt_repeatable_%d_%d", os.Getpid(), time.Now().UnixNano())
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE "+database)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+database+engine.drop)
		c.Check(dropErr, qt.IsNil)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	return parsed.String()
}

// sqlServerScratchDatabaseURL creates a SQL Server database of its own for one
// test and drops it when the test ends, closing whatever session the test left
// open on it.
func sqlServerScratchDatabaseURL(c *qt.C, adminEngine dbtarget.Engine, prefix string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, adminEngine)
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	database := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE "+database)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx,
			"ALTER DATABASE "+database+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+database)
		c.Check(dropErr, qt.IsNil)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// oracleScratchAccountURL creates an Oracle account of its own for one test,
// with the privileges a migration needs, and drops it with everything it owns
// when the test ends.
func oracleScratchAccountURL(c *qt.C, adminEngine dbtarget.Engine, prefix string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, adminEngine)
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	account := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP USER "+account+" CASCADE")
		c.Check(dropErr, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE USER " + account + " IDENTIFIED BY ptah_password",
		"ALTER USER " + account + " QUOTA UNLIMITED ON USERS",
		"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO " + account,
	} {
		_, err := admin.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	return parsed.String()
}

// withSessionVariable returns a MySQL-family URL that sets a session variable
// on every connection it opens.
func withSessionVariable(c *qt.C, rawURL, name, value string) string {
	c.Helper()
	scheme, dsn, found := strings.Cut(rawURL, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	if config.Params == nil {
		config.Params = make(map[string]string)
	}
	config.Params[name] = value
	return scheme + "://" + config.FormatDSN()
}

// migrator opens a migrator that loads the repeatable migrations of fsys, on a
// connection of its own so a held lock is another session's.
func (r *repeatableEngineTarget) migrator(c *qt.C, fsys fstest.MapFS) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), r.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithRepeatableMigrations())
	c.Assert(err, qt.IsNil)
	return mig
}

// up runs every pending migration of fsys.
func (r *repeatableEngineTarget) up(c *qt.C, fsys fstest.MapFS, opts migrator.MigrateUpOptions) error {
	c.Helper()
	return r.migrator(c, fsys).MigrateUpWithOptions(context.Background(), opts)
}

// record is what the record says of identity, read through status.
func (r *repeatableEngineTarget) record(c *qt.C, fsys fstest.MapFS, identity string) migrator.RepeatableRecord {
	c.Helper()
	status, err := r.migrator(c, fsys).GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	for _, record := range status.Repeatables {
		if record.Identity == identity {
			return record
		}
	}
	return migrator.RepeatableRecord{}
}

// columns counts the columns the catalog gives relation in the test's schema,
// zero for a relation that does not exist.
func (r *repeatableEngineTarget) columns(c *qt.C, relation string) int {
	c.Helper()
	query := "SELECT count(*) FROM information_schema.columns WHERE table_schema = " + r.engine.schema +
		" AND table_name = ?"
	if r.engine.catalogColumns != "" {
		query = r.engine.catalogColumns
	}
	query = sqlutil.Rebind(r.engine.dialect, query)
	var count int
	c.Assert(r.conn.QueryRowContext(context.Background(), query, relation).Scan(&count), qt.IsNil)
	return count
}

// holdMigrationLock takes the default migration lock from a session of its own
// and keeps it until the test ends.
func (r *repeatableEngineTarget) holdMigrationLock(c *qt.C) {
	c.Helper()
	holder, err := dbschema.ConnectToDatabase(context.Background(), r.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(holder) })
	lock, err := dblock.Acquire(context.Background(), holder, "ptah_migrate", time.Second)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(lock.Release(context.Background()), qt.IsNil) })
}

// writeRepeatable replaces the body of one file of fsys.
func writeRepeatable(fsys fstest.MapFS, name, body string) {
	fsys[name] = &fstest.MapFile{Data: []byte(body)}
}

// Each server engine that states the record runs the same lifecycle: a new
// file runs, an unchanged one does not, a changed one runs again, and a failed
// one runs again once fixed. What the record says of an attempt is read back
// against what the catalog holds.
func TestRepeatableMigrationsOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name+": a changed file runs again and an unchanged one does not", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()})

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			first := r.record(c, fsys, "widget_ids")
			c.Assert(first.State, qt.Equals, migrator.RepeatableStateApplied)
			c.Assert(first.NoTransaction, qt.Equals, engine.noTransaction)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			c.Assert(r.record(c, fsys, "widget_ids").AttemptedAt, qt.DeepEquals, first.AttemptedAt)

			writeRepeatable(fsys, "R__widget_ids.sql", engine.widerIDs())
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			changed := r.record(c, fsys, "widget_ids")
			c.Assert(changed.State, qt.Equals, migrator.RepeatableStateApplied)
			c.Assert(changed.AppliedChecksum, qt.Not(qt.Equals), first.AppliedChecksum)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 2)
		})

		// A row no file carries and a new file whose identity differs from it
		// only in letter case are two migrations, as they are to the
		// directory.
		t.Run(engine.name+": an identity that differs from an orphaned row in letter case only is new", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{
				"R__Widgets.sql": engine.view("upper_ids", "id"),
			})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			delete(fsys, "R__Widgets.sql")
			writeRepeatable(fsys, "R__widgets.sql", engine.view("lower_ids", "id"))

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)

			c.Assert(r.record(c, fsys, "Widgets").State, qt.Equals, migrator.RepeatableStateOrphaned)
			c.Assert(r.record(c, fsys, "widgets").State, qt.Equals, migrator.RepeatableStateApplied)
			c.Assert(r.columns(c, "lower_ids"), qt.Equals, 1)
		})

		// Repair deletes one row by identity, which an engine whose rows
		// change through mutations spells its own way.
		t.Run(engine.name+": repair removes the row of a deleted file and keeps the others", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{
				"R__widget_ids.sql": engine.widgetIDs(),
				"R__copy_ids.sql":   engine.view("copy_ids", "id, id AS copy"),
			})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			delete(fsys, "R__widget_ids.sql")

			removed, err := r.migrator(c, fsys).RemoveRepeatableRecord(context.Background(),
				migrator.RemoveRepeatableRecordOptions{Identity: "widget_ids"})

			c.Assert(err, qt.IsNil)
			c.Assert(removed.State, qt.Equals, migrator.RepeatableStateOrphaned)
			c.Assert(r.record(c, fsys, "widget_ids"), qt.DeepEquals, migrator.RepeatableRecord{})
			c.Assert(r.record(c, fsys, "copy_ids").State, qt.Equals, migrator.RepeatableStateApplied)
		})

		t.Run(engine.name+": a failed attempt runs again with AllowDirty once its file is fixed", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__partial.sql": repeatableEnginesFailing})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.ErrorAs, new(*migrator.RepeatableMigrationError))

			writeRepeatable(fsys, "R__partial.sql", engine.fixed)
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)

			record := r.record(c, fsys, "partial")
			c.Assert(record.State, qt.Equals, migrator.RepeatableStateApplied)
			c.Assert(record.Error, qt.Equals, "")
			c.Assert(r.columns(c, "partial_marker"), qt.Equals, 1)
		})
	}
}

// The record keeps the time an attempt started however the session treats a
// TIMESTAMP column that states no default: with explicit_defaults_for_timestamp
// off, such a column is updated by every write to its row.
func TestRepeatableRecordOnMySQLFamilyLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableMySQLFamilyEngines {
		t.Run(engine.name+": the attempt time is not updated by writes to its row", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			r.url = withSessionVariable(c, r.url, "explicit_defaults_for_timestamp", "0")
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()})

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)

			var extra string
			c.Assert(r.conn.QueryRowContext(context.Background(), "SELECT EXTRA FROM information_schema.columns "+
				"WHERE table_schema = DATABASE() AND table_name = 'schema_migrations_repeatable' "+
				"AND column_name = 'attempted_at'").Scan(&extra), qt.IsNil)
			c.Assert(extra, qt.Not(qt.Matches), `(?i).*on update.*`)
		})
	}
}

func TestRepeatableMigrationsOnEnginesLive_FailurePath(t *testing.T) {
	for _, engine := range repeatableEngines {
		// The record says whether a failed attempt may have left part of
		// itself behind, and the catalog says whether it did: an engine that
		// keeps the first statement of a failed body has to be one whose
		// record says so.
		t.Run(engine.name+": a failed attempt is recorded as the engine left it, and blocks the next run", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__partial.sql": repeatableEnginesFailing})

			err := r.up(c, fsys, migrator.MigrateUpOptions{})
			c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationError))
			record := r.record(c, fsys, "partial")
			c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
			c.Assert(record.NoTransaction, qt.Equals, engine.noTransaction)
			c.Assert(r.columns(c, "partial_marker") > 0, qt.Equals, engine.keepsPartial)

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.ErrorAs, new(*migrator.DirtyRepeatableError))
			c.Assert(r.record(c, fsys, "partial").AttemptedAt, qt.DeepEquals, record.AttemptedAt)
		})
	}

	for _, engine := range repeatableLockingEngines {
		t.Run(engine.name+": a held migration lock keeps a changed file from running", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": engine.widgetIDs()})
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			applied := r.record(c, fsys, "widget_ids")
			writeRepeatable(fsys, "R__widget_ids.sql", engine.widerIDs())
			r.holdMigrationLock(c)

			err := r.migrator(c, fsys).WithMigrationLockTimeout(500*time.Millisecond).
				MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{})

			c.Assert(err, qt.ErrorAs, new(*migrator.MigrationLockTimeoutError))
			c.Assert(r.record(c, fsys, "widget_ids").AppliedChecksum, qt.Equals, applied.AppliedChecksum)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
		})
	}

	for _, engine := range repeatableNonAtomicEngines {
		t.Run(engine.name+": a file that asks for a transaction is refused before anything runs", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := repeatableEngineMigrations(map[string]string{
				"R__widget_ids.sql": "-- atlas:txmode file\n\n" + engine.widgetIDs(),
			})

			err := r.up(c, fsys, migrator.MigrateUpOptions{})

			c.Assert(err, qt.ErrorMatches, `repeatable migration R__widget_ids.sql asks for a transaction of its own, and `+
				engine.dialect+` does not roll schema changes back as a unit, .*`)
			c.Assert(r.columns(c, "widgets"), qt.Equals, 0)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 0)
		})
	}
}
