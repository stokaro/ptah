//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// A MySQL or MariaDB history that drops or strips the account the run
// connects as is refused on a server the run owns and under trust, and the
// account still logs in and works afterwards. The same history naming another
// account replays there, so the refusal is about the account and not about
// account statements.

// connectingAccountEngines are the servers the tests below run on, each with
// the scheme its dev URL is spelled with.
var connectingAccountEngines = []struct {
	name   string
	engine dbtarget.Engine
	scheme string
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin, scheme: "mysql"},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin, scheme: "mariadb"},
	{name: "MariaDB 11.8", engine: dbtarget.MariaDB118, scheme: "mariadb"},
}

// connectingAccountFixture is a dev database on engine reached with an
// account of its own that holds every privilege, and another account, both
// dropped when the test ends.
type connectingAccountFixture struct {
	family  mysqlFamilyServer
	account string
	other   string
	devURL  string
}

func newConnectingAccountFixture(c *qt.C, engine dbtarget.Engine, scheme string) connectingAccountFixture {
	c.Helper()
	family := newMySQLFamilyServer(c, engine)
	suffix := time.Now().UnixNano() % 1_000_000_000
	account := fmt.Sprintf("ptah_acct_%d", suffix)
	other := fmt.Sprintf("ptah_other_%d", suffix)
	password := account + "_pw"
	createMySQLUser(c, c.Context(), family.admin, account, password)
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), family.admin, account) })
	createMySQLUser(c, c.Context(), family.admin, other, other+"_pw")
	c.Cleanup(func() { dropMySQLUser(c, context.Background(), family.admin, other) })
	_, err := family.admin.ExecContext(c.Context(), fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION", account))
	c.Assert(err, qt.IsNil)
	database := family.database(c, "acct")
	devURL := (&url.URL{Scheme: scheme, User: url.UserPassword(account, password), Host: family.config.Addr, Path: "/" + database}).String()
	return connectingAccountFixture{family: family, account: account, other: other, devURL: devURL}
}

// login opens a new connection as user, whose password is user+"_pw", and
// returns it with the error of its first round trip. The server reads the
// account the way it reads every new connection, so an account a history
// dropped, renamed or locked out is refused here even where the grant tables
// still hold its row.
func (fx connectingAccountFixture) login(c *qt.C, user string) (*sql.DB, error) {
	c.Helper()
	config := fx.family.config.Clone()
	config.User, config.Passwd, config.DBName = user, user+"_pw", ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db, db.PingContext(c.Context())
}

// reload makes the server read its grant tables again. MariaDB removes the
// row of an account that a padded name drops and keeps the account in memory
// until a reload, so a login asked before it would see the account the
// tables no longer hold.
func (fx connectingAccountFixture) reload(c *qt.C) {
	c.Helper()
	_, err := fx.family.admin.ExecContext(c.Context(), "FLUSH PRIVILEGES")
	c.Assert(err, qt.IsNil)
}

// works asserts that, once the server reloads its grant tables, the account
// the dev URL connects as still logs in on a new connection and still creates
// and drops a database, which an account dropped or stripped of its
// privileges cannot.
func (fx connectingAccountFixture) works(c *qt.C) {
	c.Helper()
	fx.reload(c)
	db, err := fx.login(c, fx.account)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_works_%d", time.Now().UnixNano())
	_, err = db.ExecContext(c.Context(), "CREATE DATABASE `"+name+"`")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), "DROP DATABASE `"+name+"`")
	c.Assert(err, qt.IsNil)
}

// validate replays a directory holding the statements of history, under trust and on a server
// declared disposable, with ptah migrations validate.
func (fx connectingAccountFixture) validate(c *qt.C, history string) (string, error) {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	body := "CREATE TABLE items (id bigint PRIMARY KEY);\n" + history + ";\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_account.sql"), []byte(body), 0o600), qt.IsNil)
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	return runPtahNativeWithError("migrations", "validate", "--dir", dir, "--dir-format", "atlas",
		"--dev-url", fx.devURL, "--trust-replay-sql")
}

// TestMigrationsValidateRefusesTheConnectingAccountE2E replays a history that
// reaches the account the dev URL connects as: by a spelling the server reads
// as that account, by the name only the session read tells the guard, padded
// with a space the server trims, or by a write to the grant tables that a
// failed account statement makes the server read. The refusal offers no way
// to own the server or to trust the SQL, since neither lifts it, and the
// account still logs in and works afterwards.
func TestMigrationsValidateRefusesTheConnectingAccountE2E(t *testing.T) {
	const (
		reaches = " that reaches the account the run connects as because its effects cannot be confined to the " +
			"disposable database realm; connect as an account the history does not name"
		confined = " because its effects cannot be confined to the disposable database realm"
	)
	histories := []struct {
		name, history, refused string
	}{
		{name: "DROP USER CURRENT_USER", history: "DROP USER CURRENT_USER", refused: "DROP USER" + reaches},
		{name: "account by name", history: "DROP USER '{account}'@'%'", refused: "DROP USER" + reaches},
		{name: "account padded with a space", history: "DROP USER '{account} '@'%'", refused: "DROP USER" + reaches},
		{name: "every privilege revoked", history: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM CURRENT_USER", refused: "REVOKE" + reaches},
		{name: "every privilege revoked from the padded account", history: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM '{account} '@'% '",
			refused: "REVOKE" + reaches},
		// Measured on MySQL 8.4.13 and 26.10.0: replayed, the failed GRANT
		// reloads the grant tables and the account the DELETE removed is
		// refused its next login.
		{name: "grant table row deleted", history: "DELETE FROM mysql.user WHERE User = '{account}';\nCREATE ROLE {account}_r;\nGRANT {account}_r TO 'nobody'@'%'",
			refused: `write to account table "mysql.user"` + confined},
	}
	for _, engine := range connectingAccountEngines {
		for _, test := range histories {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newConnectingAccountFixture(c, engine.engine, engine.scheme)
				history := strings.ReplaceAll(test.history, "{account}", fx.account)
				first, _, _ := strings.Cut(history, ";")

				out, err := fx.validate(c, history)

				c.Assert(err, qt.ErrorMatches, `(?s).* migration replay rejects `+regexp.QuoteMeta(test.refused)+
					`\nSQL: `+regexp.QuoteMeta(first), qt.Commentf("%s", out))
				fx.works(c)
			})
		}
	}
}

// TestMigrationsValidateReplaysAnotherAccountE2E is the control for the test
// above: the same statements naming another account replay, and that account
// can no longer log in once the server reloads its grant tables. The padded
// spelling drops it too, so the server trims the name as the guard reads it.
func TestMigrationsValidateReplaysAnotherAccountE2E(t *testing.T) {
	histories := []struct {
		name, history string
	}{
		{name: "by name", history: "REVOKE ALL PRIVILEGES, GRANT OPTION FROM '{other}'@'%';\nDROP USER '{other}'@'%'"},
		{name: "padded with a space", history: "DROP USER '{other} '@'% '"},
	}
	for _, engine := range connectingAccountEngines {
		for _, test := range histories {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				fx := newConnectingAccountFixture(c, engine.engine, engine.scheme)

				out, err := fx.validate(c, strings.ReplaceAll(test.history, "{other}", fx.other))

				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				fx.works(c)
				_, err = fx.login(c, fx.other)
				c.Assert(err, qt.ErrorMatches, `Error 1045 \(28000\): Access denied for user '`+fx.other+`'.*`)
			})
		}
	}
}

// TestMigrationsValidateRefusesAFoldedAccountTableE2E replays a write to
// global_priv spelled with a dotted capital I (U+0130) on a MariaDB server
// started with --lower-case-table-names=1, which lowercases the name to the
// account table and, unguarded, locks the account at the next reload of the
// grant tables. The guard refuses it, and the account still logs in and works
// after the reload. The control spells the table with a dotless i (U+0131),
// which the server does not lowercase to it: the guard replays the write, and
// the server refuses it as a table that does not exist.
func TestMigrationsValidateRefusesAFoldedAccountTableE2E(t *testing.T) {
	const lock = "UPDATE mysql.global_pr%sv SET Priv = JSON_SET(Priv, '$.account_locked', true) WHERE User = '%s'"

	t.Run("dotted capital I", func(t *testing.T) {
		c := qt.New(t)
		fx := newConnectingAccountFixture(c, dbtarget.MariaDBFoldedServer, "mariadb")
		history := fmt.Sprintf(lock, "\u0130", fx.account)

		out, err := fx.validate(c, history)

		c.Assert(err, qt.ErrorMatches, `(?s).* migration replay rejects `+
			regexp.QuoteMeta("write to account table \"mysql.global_pr\u0130v\" because its effects cannot be confined to the disposable database realm")+
			`\nSQL: `+regexp.QuoteMeta(history), qt.Commentf("%s", out))
		fx.works(c)
	})

	t.Run("dotless i", func(t *testing.T) {
		c := qt.New(t)
		fx := newConnectingAccountFixture(c, dbtarget.MariaDBFoldedServer, "mariadb")

		out, err := fx.validate(c, fmt.Sprintf(lock, "\u0131", fx.account))

		c.Assert(err, qt.ErrorMatches, `(?s).*Error 1146 \(42S02\): Table 'mysql\.global_pr\x{0131}v' doesn't exist.*`, qt.Commentf("%s", out))
		fx.works(c)
	})
}
