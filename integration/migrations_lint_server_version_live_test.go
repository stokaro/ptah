//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migrationlintreport"
)

// A lint run with --dev-url reads the version off the server it replays
// against, so a project that already points the linter at a dev database gets
// the version without declaring it twice.
//
// It is a live test because the value comes from the server: what the banner
// says, how it is parsed, and which release line it lands on are answers no
// fixture can give. The assertion is that the report names a version at all
// and that it is the one the same connection reports, not a literal, because a
// literal would pin the CI image rather than the path (stokaro/ptah#3420).
func TestMigrationsLintReadsTheServerVersionFromTheDevDatabaseLive(t *testing.T) {
	c := qt.New(t)
	devURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	dir := writeServerVersionLintDir(c, t)

	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Dir:     dir,
		DevURL:  devURL,
		FailOn:  migrationlintreport.FailOnError,
		Changed: migrationlintreport.ChangedOptions{Dir: true, DevURL: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Dialect, qt.Equals, "postgres")
	c.Assert(report.ServerVersion, qt.Not(qt.Equals), "")
	c.Assert(report.ServerVersion, qt.Equals, serverVersionOf(c, devURL))
}

// A declaration outranks the dev database: a dev database is a scratch server,
// often a different release from the one a migration will meet, so a project
// that wrote down the version it targets gets that one.
func TestMigrationsLintPrefersTheDeclaredServerVersionOverTheDevDatabaseLive(t *testing.T) {
	c := qt.New(t)
	devURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	dir := writeServerVersionLintDir(c, t)

	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Dir:           dir,
		DevURL:        devURL,
		ServerVersion: "13",
		FailOn:        migrationlintreport.FailOnError,
		Changed: migrationlintreport.ChangedOptions{
			Dir: true, DevURL: true, ServerVersion: true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.ServerVersion, qt.Equals, "13")
	c.Assert(report.ServerVersion, qt.Not(qt.Equals), serverVersionOf(c, devURL))
}

func writeServerVersionLintDir(c *qt.C, t *testing.T) string {
	c.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":   "CREATE TABLE ptah_lint_version_users (id BIGINT PRIMARY KEY);\n",
		"0000000001_users.down.sql": "DROP TABLE ptah_lint_version_users;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// serverVersionOf asks the same server the lint run asked, through the same
// connection path, so the two answers are comparable rather than both derived
// from the CI image name.
func serverVersionOf(c *qt.C, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	return conn.Info().Version
}

// A URL scheme is not a product, and MariaDB is where that costs something: it
// speaks the MySQL wire protocol, so its address is a `mysql://` URL and the
// scheme names MySQL. A declared `10.11.6-MariaDB` resolved against the scheme
// is refused for naming another product than the one it actually names.
//
// Only a server can settle it: the dialect comes off the connection, and the
// version the operator wrote is resolved against that (stokaro/ptah#3420).
func TestMigrationsLintResolvesTheDeclaredVersionAgainstTheConnectedProductLive(t *testing.T) {
	c := qt.New(t)
	devURL := mySQLSchemeFor(c, dbtarget.URL(c, dbtarget.MariaDBAdmin))
	dir := writeServerVersionLintDir(c, t)

	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Dir:           dir,
		DevURL:        devURL,
		ServerVersion: "10.11.6-MariaDB",
		FailOn:        migrationlintreport.FailOnError,
		Changed: migrationlintreport.ChangedOptions{
			Dir: true, DevURL: true, ServerVersion: true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(report.ServerVersion, qt.Equals, "10.11.6-MariaDB")
	// The lint dialect is still the scheme's. It decides which rules run, and
	// reading it off the connection would change that for every mysql:// URL
	// that reaches MariaDB, which is a separate decision from resolving a
	// version (stokaro/ptah#3466).
	c.Assert(report.Dialect, qt.Equals, "mysql")
}

// Deferring is not relaxing: a version that names another product is still
// refused, now against the product that answered rather than against the URL's
// scheme. A bare number names no product and is resolved on the connected
// server's ladder, which is why this row uses a banner.
func TestMigrationsLintRefusesADeclaredVersionTheServerDoesNotOwnLive(t *testing.T) {
	c := qt.New(t)
	devURL := mySQLSchemeFor(c, dbtarget.URL(c, dbtarget.MariaDBAdmin))
	dir := writeServerVersionLintDir(c, t)

	_, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Dir:           dir,
		DevURL:        devURL,
		ServerVersion: "PostgreSQL 16.3 (Debian)",
		FailOn:        migrationlintreport.FailOnError,
		Changed: migrationlintreport.ChangedOptions{
			Dir: true, DevURL: true, ServerVersion: true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.ErrorMatches, `(?s).*postgres.*`)
	// That the refusal arrives before the replay is measured where the replay
	// lives: TestReplayStopsWhenTheServerObserverRefuses asserts no migration
	// ran and the dev database is untouched. Here the error is the same either
	// way, so there is nothing for this test to tell apart.
}

// mySQLSchemeFor addresses a MariaDB server the way a MySQL client does, which
// is the premise of the two tests above and the reason they measure anything.
//
// The registry may hand out either spelling -- `mariadb://` names the product
// and `mysql://` names the protocol both speak -- and which one it is decides
// what the scheme-read dialect says. Forcing the protocol spelling makes the
// test about a scheme that does not name the product, whatever the environment
// happens to be configured with.
func mySQLSchemeFor(c *qt.C, url string) string {
	c.Helper()
	rewritten, found := strings.CutPrefix(url, "mariadb://")
	if !found {
		return url
	}
	return "mysql://" + rewritten
}
