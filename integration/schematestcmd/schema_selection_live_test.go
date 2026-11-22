//go:build integration

package schematestcmd_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/cli/schema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// TestSchemaTestSchemaSelectionPostgresScope_HappyPath pins that `ptah schema
// test --schema` names the schema an unqualified table lands in on the
// throwaway PostgreSQL database. The desired schema is `CREATE TABLE users`
// with no schema, and the case reads back from the catalog where the server
// created it: in `public` on a URL that pins no search_path, in `other` on one
// whose search_path is `other`. A selection that took `public` whatever the
// URL said keeps nothing on the second row and is refused before the case runs.
func TestSchemaTestSchemaSelectionPostgresScope_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		searchPath string
		schema     string
		desired    string
	}{
		{name: "no search_path", schema: "public", desired: "CREATE TABLE users (id integer);\n"},
		{name: "search_path other", searchPath: "other", schema: "other", desired: "CREATE TABLE users (id integer);\n"},
		{name: "two search_path schemas, a qualified table", searchPath: "other,public", schema: "other", desired: "CREATE TABLE other.users (id integer);\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := createPostgresDatabase(c, test.searchPath)
			root := writeSelectionFixture(c, test.desired, test.schema)

			out, err := runSchemaTest(c, root, "--schema", test.schema, "--db-url", devURL)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `PASS  case "the table lands in the selected schema"`)
		})
	}
}

// TestSchemaTestSchemaSelectionPostgresScope_FailurePath is the inverse: on a
// URL whose search_path is `other`, the server creates the unqualified table in
// `other`, so `--schema public` keeps nothing and is refused. Taking the
// dialect's default whatever the URL said, the selection kept the table, the
// run passed, and the table landed in `other`.
func TestSchemaTestSchemaSelectionPostgresScope_FailurePath(t *testing.T) {
	c := qt.New(t)
	devURL := createPostgresDatabase(c, "other")
	root := writeSelectionFixture(c, "CREATE TABLE users (id integer);\n", "public")

	out, err := runSchemaTest(c, root, "--schema", "public", "--db-url", devURL)

	c.Assert(err, qt.ErrorMatches, `--schema public selects no tables out of the desired schema`)
	c.Assert(out, qt.Not(qt.Contains), "PASS")
}

// TestSchemaTestSchemaSelectionMySQLScope_HappyPath pins that on a MySQL
// throwaway database the database the URL names is the schema an unqualified
// table lands in, so `--schema <database>` keeps it. Without the URL's
// database, no schema owns an unqualified MySQL table and every selection is
// refused.
func TestSchemaTestSchemaSelectionMySQLScope_HappyPath(t *testing.T) {
	c := qt.New(t)
	devURL, database := createMySQLDatabase(c)
	root := writeSelectionFixture(c, "CREATE TABLE users (id integer);\n", database)

	out, err := runSchemaTest(c, root, "--schema", database, "--db-url", devURL)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "the table lands in the selected schema"`)
}

// TestSchemaTestSchemaSelectionPostgresSource_HappyPath pins the database
// source: introspected through a search_path naming `other`, the source's
// tables come back unqualified, and the session's schema, `other`, owns them.
// So `--schema other` keeps the table, and the case finds it on the throwaway
// database. Taking the dialect's default instead, the selection kept nothing
// and the run was refused.
func TestSchemaTestSchemaSelectionPostgresSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	sourceURL := createPostgresDatabase(c, "other", "CREATE TABLE other.audit (id integer)", "CREATE TABLE public.users (id integer)")
	devURL := createPostgresDatabase(c, "")
	root := writeSourceFixture(c, "SELECT count(*) FROM audit", "0")

	out, err := runSchemaTestCommand(c, "--dir", root, "--source-db-url", sourceURL, "--db-url", devURL, "--schema", "other")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `PASS  case "the source table is provisioned"`)
}

// TestSchemaTestSchemaSelectionPostgresSource_FailurePath pins the refusals on
// a database source. Through a search_path naming `other`, `public` owns none
// of the introspected tables, so `--schema public` keeps nothing. Through a
// search_path listing `other` and `public`, the introspection leaves the tables
// of both unqualified, so no selection can separate them and it is refused.
func TestSchemaTestSchemaSelectionPostgresSource_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		searchPath string
		schema     string
		wantErr    string
	}{
		{
			name:       "search_path other",
			searchPath: "other",
			schema:     "public",
			wantErr:    `--schema public selects no tables out of the desired schema`,
		},
		{
			name:       "two search_path schemas",
			searchPath: "other,public",
			schema:     "other",
			wantErr: `--schema other: --source-db-url sets search_path "other,public", which lists more than one schema,` +
				` so the selection cannot tell which of them owns an object the desired schema leaves unqualified;` +
				` name one schema in search_path`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sourceURL := createPostgresDatabase(c, test.searchPath, "CREATE TABLE other.audit (id integer)", "CREATE TABLE public.users (id integer)")
			devURL := createPostgresDatabase(c, "")
			root := writeSourceFixture(c, "SELECT count(*) FROM audit", "0")

			out, err := runSchemaTestCommand(c, "--dir", root, "--source-db-url", sourceURL, "--db-url", devURL, "--schema", test.schema)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(out, qt.Not(qt.Contains), "PASS")
		})
	}
}

// writeSourceFixture writes a case directory whose one case expects query to
// answer scalar, and returns it.
func writeSourceFixture(c *qt.C, query, scalar string) string {
	c.Helper()
	root := c.TempDir()
	testCase := fmt.Sprintf(
		"cases:\n  - name: the source table is provisioned\n    steps:\n"+
			"      - assert:\n          query: %q\n          scalar: %q\n",
		query, scalar,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "source.yaml"), []byte(testCase), 0o600), qt.IsNil)
	return root
}

// writeSelectionFixture writes the desired schema and a case that counts a
// `users` table in schemaName through the catalog, and returns the directory.
func writeSelectionFixture(c *qt.C, desired, schemaName string) string {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(desired), 0o600), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(root, "tests"), 0o700), qt.IsNil)
	testCase := fmt.Sprintf(
		"cases:\n  - name: the table lands in the selected schema\n    steps:\n"+
			"      - assert:\n          query: \"SELECT count(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = 'users'\"\n          scalar: \"1\"\n",
		schemaName,
	)
	c.Assert(os.WriteFile(filepath.Join(root, "tests", "scope.yaml"), []byte(testCase), 0o600), qt.IsNil)
	return root
}

// runSchemaTest runs the native `ptah schema test` command over the fixture in
// root, with the desired schema from its schema.sql, and returns what it
// printed.
func runSchemaTest(c *qt.C, root string, args ...string) (string, error) {
	c.Helper()
	return runSchemaTestCommand(c, append([]string{
		"--dir", filepath.Join(root, "tests"),
		"--schema-file", filepath.Join(root, "schema.sql"),
	}, args...)...)
}

// runSchemaTestCommand runs the native `ptah schema test` command with args and
// returns what it printed.
func runSchemaTestCommand(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"test"}, args...))
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

// createPostgresDatabase creates a database holding a schema `other` on the
// PostgreSQL test server, runs statements in it, drops it on cleanup, and
// returns its URL with searchPath as its search_path when one is given.
func createPostgresDatabase(c *qt.C, searchPath string, statements ...string) string {
	c.Helper()
	serverURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	name := fmt.Sprintf("ptah_schema_test_scope_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	admin, err := dbschema.ConnectToDatabase(context.Background(), serverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(context.Background(), `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	created, err := dbschema.ConnectToDatabase(context.Background(), parsed.String())
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(created)
	for _, statement := range append([]string{`CREATE SCHEMA other`}, statements...) {
		_, err = created.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}

	query := parsed.Query()
	if searchPath != "" {
		query.Set("search_path", searchPath)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// createMySQLDatabase creates a database on the MySQL test server through the
// administrative account, drops it on cleanup, and returns a URL naming it
// together with its name.
func createMySQLDatabase(c *qt.C) (dbURL, database string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.MySQLAdmin)
	name := fmt.Sprintf("ptah_schema_test_scope_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	quoted := sqlident.Quote(platform.MySQL, name)
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE "+quoted)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, cleanupErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quoted)
		c.Check(cleanupErr, qt.IsNil)
	})
	scheme, dsn, found := strings.Cut(adminURL, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = name
	return scheme + "://" + config.FormatDSN(), name
}
