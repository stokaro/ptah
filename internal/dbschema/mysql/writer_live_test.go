//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/dbschema/mysql"
	"github.com/stokaro/ptah/internal/sqlident"
)

func TestWriterDropDatabaseRealm_LiveRejectsProtectedDatabase(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dsnEnv  string
		dialect string
	}{
		{
			name:    "mysql",
			dsnEnv:  "MYSQL_ADMIN_TEST_DSN",
			dialect: platform.MySQL,
		},
		{
			name:    "mariadb",
			dsnEnv:  "MARIADB_ADMIN_TEST_DSN",
			dialect: platform.MariaDB,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openMySQLWriterLiveDatabase(c, test.dsnEnv)
			c.Cleanup(func() {
				c.Check(db.Close(), qt.IsNil)
			})
			writer := mysql.NewMySQLWriter(db, "mysql", test.dialect)

			err := writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.ErrorMatches, `mysql: refusing to clean protected database "mysql"`)
		})
	}
}

func TestWriterDropDatabaseRealm_LiveRejectsExternalStoredProgram(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dsnEnv  string
		dialect string
	}{
		{
			name:    "mysql",
			dsnEnv:  "MYSQL_ADMIN_TEST_DSN",
			dialect: platform.MySQL,
		},
		{
			name:    "mariadb",
			dsnEnv:  "MARIADB_ADMIN_TEST_DSN",
			dialect: platform.MariaDB,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			db := openMySQLWriterLiveDatabase(c, test.dsnEnv)
			c.Cleanup(func() {
				c.Check(db.Close(), qt.IsNil)
			})
			suffix := time.Now().UnixNano()
			target := fmt.Sprintf("ptah_realm_%d", suffix)
			external := fmt.Sprintf("ptah_external_%d", suffix)
			quotedTarget := sqlident.Quote(test.dialect, target)
			quotedExternal := sqlident.Quote(test.dialect, external)
			events := sqlident.Qualified(test.dialect, target, "events")
			externalFunction := sqlident.Qualified(test.dialect, external, "event_count")
			_, err := db.ExecContext(
				t.Context(),
				"CREATE DATABASE "+quotedTarget,
			)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, cleanupErr := db.ExecContext(
					cleanupCtx,
					"DROP DATABASE IF EXISTS "+quotedTarget,
				)
				c.Check(cleanupErr, qt.IsNil)
			})
			_, err = db.ExecContext(t.Context(), "CREATE DATABASE "+quotedExternal)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, cleanupErr := db.ExecContext(
					cleanupCtx,
					"DROP DATABASE IF EXISTS "+quotedExternal,
				)
				c.Check(cleanupErr, qt.IsNil)
			})
			_, err = db.ExecContext(
				t.Context(),
				"CREATE TABLE "+events+" (id BIGINT PRIMARY KEY)",
			)
			c.Assert(err, qt.IsNil)
			_, err = db.ExecContext(
				t.Context(),
				"CREATE FUNCTION "+externalFunction+
					"() RETURNS BIGINT READS SQL DATA RETURN (SELECT COUNT(*) FROM "+events+")",
			)
			c.Assert(err, qt.IsNil)
			writer := mysql.NewMySQLWriter(db, target, test.dialect)

			err = writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "external function "+externalFunction)
			var tableCount int
			err = db.QueryRowContext(
				t.Context(),
				"SELECT COUNT(*) FROM information_schema.tables "+
					"WHERE table_schema = ? AND table_name = 'events'",
				target,
			).Scan(&tableCount)
			c.Assert(err, qt.IsNil)
			c.Assert(tableCount, qt.Equals, 1)
		})
	}
}

func TestWriterDropDatabaseRealm_LiveRejectsMissingTriggerPrivilege(t *testing.T) {
	c := qt.New(t)
	adminDB := openMySQLWriterLiveDatabase(c, "MYSQL_ADMIN_TEST_DSN")
	c.Cleanup(func() {
		c.Check(adminDB.Close(), qt.IsNil)
	})

	suffix := time.Now().UnixMilli()
	target := fmt.Sprintf("ptah_trigger_target_%d", suffix)
	external := fmt.Sprintf("ptah_trigger_external_%d", suffix)
	username := fmt.Sprintf("ptah_no_trigger_%d", suffix)
	password := "Ptah-test-2026!"
	createMySQLWriterLiveDatabase(c, adminDB, target)
	c.Cleanup(func() {
		dropMySQLWriterLiveDatabase(c, adminDB, target)
	})
	createMySQLWriterLiveDatabase(c, adminDB, external)
	c.Cleanup(func() {
		dropMySQLWriterLiveDatabase(c, adminDB, external)
	})
	_, err := adminDB.ExecContext(
		c.Context(),
		"CREATE TABLE "+sqlident.Qualified(platform.MySQL, target, "events")+
			" (id BIGINT PRIMARY KEY)",
	)
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(
		c.Context(),
		"CREATE TABLE "+sqlident.Qualified(platform.MySQL, external, "source_events")+
			" (id BIGINT PRIMARY KEY)",
	)
	c.Assert(err, qt.IsNil)
	_, err = adminDB.ExecContext(
		c.Context(),
		"CREATE TRIGGER "+sqlident.Qualified(platform.MySQL, external, "capture_event")+
			" BEFORE INSERT ON "+sqlident.Qualified(platform.MySQL, external, "source_events")+
			" FOR EACH ROW INSERT INTO "+sqlident.Qualified(platform.MySQL, target, "events")+
			" (id) VALUES (NEW.id)",
	)
	c.Assert(err, qt.IsNil)
	createMySQLWriterLiveUser(c, adminDB, username, password)
	c.Cleanup(func() {
		dropMySQLWriterLiveUser(c, adminDB, username)
	})
	grantMySQLWriterLivePrivilegesWithoutTrigger(c, adminDB, username)

	config, err := mysqldriver.ParseDSN(os.Getenv("MYSQL_ADMIN_TEST_DSN"))
	c.Assert(err, qt.IsNil)
	config.User = username
	config.Passwd = password
	config.DBName = target
	restrictedDB, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(restrictedDB.Close(), qt.IsNil)
	})
	c.Assert(restrictedDB.PingContext(c.Context()), qt.IsNil)
	writer := mysql.NewMySQLWriter(restrictedDB, target, platform.MySQL)

	err = writer.DropDatabaseRealm(c.Context())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "TRIGGER privileges are required")
	var tableCount int
	err = adminDB.QueryRowContext(
		c.Context(),
		"SELECT COUNT(*) FROM information_schema.tables "+
			"WHERE table_schema = ? AND table_name = 'events'",
		target,
	).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 1)
}

func openMySQLWriterLiveDatabase(c *qt.C, dsnEnv string) *sql.DB {
	c.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		c.Skipf("%s is not set", dsnEnv)
	}
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(c.Context()), qt.IsNil)
	return db
}

func createMySQLWriterLiveDatabase(c *qt.C, db *sql.DB, name string) {
	c.Helper()
	_, err := db.ExecContext(
		c.Context(),
		"CREATE DATABASE "+sqlident.Quote(platform.MySQL, name),
	)
	c.Assert(err, qt.IsNil)
}

func dropMySQLWriterLiveDatabase(c *qt.C, db *sql.DB, name string) {
	c.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(
		cleanupCtx,
		"DROP DATABASE IF EXISTS "+sqlident.Quote(platform.MySQL, name),
	)
	c.Check(err, qt.IsNil)
}

func createMySQLWriterLiveUser(c *qt.C, db *sql.DB, username, password string) {
	c.Helper()
	//nolint:gosec // The generated username and fixed test password contain no SQL metacharacters.
	query := fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", username, password)
	_, err := db.ExecContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
}

func dropMySQLWriterLiveUser(c *qt.C, db *sql.DB, username string) {
	c.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	//nolint:gosec // The generated username contains no SQL metacharacters.
	query := fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", username)
	_, err := db.ExecContext(cleanupCtx, query)
	c.Check(err, qt.IsNil)
}

func grantMySQLWriterLivePrivilegesWithoutTrigger(c *qt.C, db *sql.DB, username string) {
	c.Helper()
	//nolint:gosec // The generated username contains no SQL metacharacters.
	query := fmt.Sprintf(
		"GRANT SELECT, DROP, ALTER, ALTER ROUTINE, EVENT, LOCK TABLES, PROCESS, SHOW_ROUTINE "+
			"ON *.* TO '%s'@'%%'",
		username,
	)
	_, err := db.ExecContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
}
