//go:build integration

package migratebaseline_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/shadow"
)

// On the MySQL family a shadow database is a different database, so it carries
// a different name, and baseline refused every one of them: the identifier
// semantics check compared the database each connection selected, and a shadow
// named like the target was refused as possibly the target itself. Baseline
// with a shadow could not succeed on these engines at all (stokaro/ptah#3375).
//
// The second half is the property the fix must not lose: a target whose schema
// the migrations do not produce is still refused, by the schema comparison that
// follows the replay.
func TestVerifyBaselineMySQLFamilyAcceptsAShadowDatabaseOfItsOwn(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			adminURL, admin := requireMySQLFamilyAdminConnection(t, c, ctx, test.engine, test.name)
			defer dbschema.CloseAndWarn(admin)

			suffix := time.Now().UnixNano()
			targetName := fmt.Sprintf("ptah_3375_target_%d", suffix)
			shadowName := fmt.Sprintf("ptah_3375_shadow_%d", suffix)
			for _, name := range []string{targetName, shadowName} {
				_, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteMySQLIdent(name))
				c.Assert(err, qt.IsNil)
				defer func(name string) {
					_, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quoteMySQLIdent(name))
				}(name)
			}

			target, err := dbschema.ConnectToDatabase(ctx, addressNamingDatabase(c, adminURL, targetName))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(target)
			// The schema exists and no history does: the case baseline is for.
			_, err = target.ExecContext(ctx,
				"CREATE TABLE widgets (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)")
			c.Assert(err, qt.IsNil)

			migrationsDir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"),
				[]byte("CREATE TABLE widgets (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.down.sql"),
				[]byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)

			info := target.Info()
			options := shadow.BaselineVerifyOptions{
				ShadowDatabaseURL: addressNamingDatabase(c, adminURL, shadowName),
				TargetConn:        target,
				MigrationsDir:     migrationsDir,
				Version:           1,
				Dialect:           info.Dialect,
				Capabilities:      info.Capabilities,
			}
			c.Assert(shadow.VerifyBaseline(ctx, options), qt.IsNil)

			// A column the migrations never create makes the target a schema they
			// do not produce, and that is still a refusal -- from the comparison,
			// not from the semantics check.
			_, err = target.ExecContext(ctx, "ALTER TABLE widgets ADD COLUMN color VARCHAR(32)")
			c.Assert(err, qt.IsNil)
			err = shadow.VerifyBaseline(ctx, options)
			c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: .*`)
			var shadowErr *shadow.VerificationError
			c.Assert(err, qt.ErrorAs, &shadowErr)
			c.Assert(shadowErr.Result.Stage, qt.Equals, "schema-match")
		})
	}
}

// requireMySQLFamilyAdminConnection is requirePostgresBaselineTestConnection for
// the MySQL family: an unset address or an unreachable server skips, because
// the contour that runs this test is the one that starts the server.
func requireMySQLFamilyAdminConnection(
	t *testing.T,
	c *qt.C,
	ctx context.Context,
	engine dbtarget.Engine,
	name string,
) (string, *dbschema.DatabaseConnection) {
	t.Helper()

	adminURL, err := dbtarget.Lookup(engine)
	c.Assert(err, qt.IsNil)
	if adminURL == "" {
		t.Skipf("%s admin test database URL is not set", name)
	}
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	return adminURL, admin
}

// addressNamingDatabase names another database on the server an address
// reaches. The address is not always a URL: CI configures the MySQL family in
// the driver's own form, user:pass@tcp(host:port)/db, which url.Parse refuses at
// the parenthesis. The database is the last path segment in every accepted form
// -- a URL, tcp(...), and unix(/socket/path)/db, whose slashes sit inside the
// parentheses -- so the name is replaced there, and the query string is kept.
// A clickhouse:// URL is the plain first case.
func addressNamingDatabase(c *qt.C, address, databaseName string) string {
	c.Helper()

	base, query, _ := strings.Cut(address, "?")
	separator := strings.LastIndex(base, "/")
	c.Assert(separator > strings.LastIndex(base, "@"), qt.IsTrue,
		qt.Commentf("the address names no database to replace: %s", address))
	replaced := base[:separator+1] + databaseName
	if query != "" {
		replaced += "?" + query
	}
	return replaced
}

func quoteMySQLIdent(value string) string {
	return "`" + value + "`"
}
