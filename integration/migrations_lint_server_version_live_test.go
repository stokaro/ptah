//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
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
	dir := writeServerVersionLintDir(c)

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
	dir := writeServerVersionLintDir(c)

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

func writeServerVersionLintDir(c *qt.C) string {
	c.Helper()
	dir := c.TB.(*testing.T).TempDir()
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
