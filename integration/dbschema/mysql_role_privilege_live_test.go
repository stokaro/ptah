//go:build integration

package dbschema_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/schema"
	"ptah.run/internal/dbtarget"
)

// TestMySQLLiveRoleReadDegradesWithoutPrivilege is the account this feature
// must not break.
//
// The preset says whether the SERVER has roles. It says nothing about whether
// the connected ACCOUNT may read them, and mysql.user needs a privilege that
// reading a table does not. Failing the whole read over that would mean an
// account with SELECT on its own schema could no longer describe that schema at
// all -- not because anything about its tables changed, but because a kind it
// may not even declare became unreadable (stokaro/ptah#1762).
//
// Measured rather than assumed: the restricted account is created here, and the
// server itself decides what it may see. The refusal is MySQL error 1142, which
// is what the degradation recognizes.
func TestMySQLLiveRoleReadDegradesWithoutPrivilege(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MySQL)
	c := qt.New(t)
	ctx := t.Context()

	admin, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)

	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("ptah_priv_t_%d", suffix)
	account := fmt.Sprintf("ptah_priv_%d", suffix)
	const password = "ptah_priv_password" // #nosec G101 -- the password of an account this test creates and drops

	_, err = admin.ExecContext(ctx, fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", tableName))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = admin.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	}()

	// An account with exactly the privileges ordinary schema work needs, and
	// none of the ones a role read needs.
	_, err = admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", account, password))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = admin.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", account))
	}()
	_, err = admin.ExecContext(ctx, fmt.Sprintf(
		"GRANT SELECT, CREATE, DROP, ALTER, INSERT, UPDATE, DELETE ON %s.* TO '%s'@'%%'",
		databaseNameOf(c, dbURL), account))
	c.Assert(err, qt.IsNil)

	restricted, err := dbschema.ConnectToDatabase(ctx, replaceCredentials(dbURL, account, password))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(restricted)

	// The read succeeds, which is the whole point: before the degradation it
	// returned `failed to read roles: Error 1142 (42000): SELECT command denied`
	// and the account had no description of its own schema at all.
	described, err := restricted.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// What it could not look at is recorded rather than reported as empty, so a
	// comparison withholds instead of concluding a declared role is missing.
	c.Assert(described.NotDescribed.Describes(coverage.Role), qt.IsFalse)
	c.Assert(described.Roles, qt.HasLen, 0)

	// And everything the account CAN see is still described. A degradation that
	// dropped the rest would be no better than the failure it replaces.
	c.Assert(tableNamesOf(described.Tables), qt.Contains, tableName)
}

// TestMySQLLiveSchemaSecurity_FailurePath_RefusesUnreadRoles connects as an
// account that may work in its schema and may not read mysql.user. The read
// describes the tables and records the roles as not described, so the
// analysis refuses rather than report an access model with no role in it. The
// administrator's analysis of the same database is the control: it runs.
func TestMySQLLiveSchemaSecurity_FailurePath_RefusesUnreadRoles(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MySQL)
	c := qt.New(t)
	ctx := t.Context()

	admin, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	account := fmt.Sprintf("ptah_sec_%d", time.Now().UnixNano())
	const password = "ptah_sec_password" // #nosec G101 -- the password of an account this test creates and drops
	_, err = admin.ExecContext(ctx, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", account, password))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = admin.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", account))
	}()
	_, err = admin.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON %s.* TO '%s'@'%%'", databaseNameOf(c, dbURL), account))
	c.Assert(err, qt.IsNil)

	stdout, err := runSchemaSecurity(c, replaceCredentials(dbURL, account, password))
	c.Assert(err, qt.ErrorMatches, `the analysis reads the database's roles, and this connection may not read them, .*`)
	c.Assert(stdout, qt.Equals, "")

	stdout, err = runSchemaSecurity(c, dbURL)
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, `"findings"`)
}

// runSchemaSecurity runs `ptah schema security` in this process against
// dbURL, as JSON, with no failure threshold, and returns its standard output.
func runSchemaSecurity(c *qt.C, dbURL string) (string, error) {
	c.Helper()
	cmd := schema.NewSchemaSecurityCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetContext(c.Context())
	cmd.SetArgs([]string{"--db-url", dbURL, "--format", "json", "--fail-on", "none"})
	err := cmd.Execute()
	return stdout.String(), err
}

// databaseNameOf returns the schema the connection URL selects.
func databaseNameOf(c *qt.C, dbURL string) string {
	c.Helper()
	_, after, found := strings.Cut(dbURL, ")/")
	c.Assert(found, qt.IsTrue, qt.Commentf("no database in %q", dbURL))
	name, _, _ := strings.Cut(after, "?")
	return name
}

// replaceCredentials swaps the account in a MySQL URL, keeping everything else.
func replaceCredentials(dbURL, account, password string) string {
	scheme, rest, _ := strings.Cut(dbURL, "://")
	_, tail, _ := strings.Cut(rest, "@")
	return fmt.Sprintf("%s://%s:%s@%s", scheme, account, password, tail)
}

// tableNamesOf lists the table names a description holds.
func tableNamesOf(tables []catalog.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}
