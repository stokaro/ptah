package compare_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/compare"
)

const (
	sqlServerSchemaCommand = "go run ../internal/schemaops/testdata/sqlserver-schema-command"
	sqlServerDatabaseURL   = "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable"
)

func TestCompareCommandExposesRepeatableSchemaFileFlag(t *testing.T) {
	c := qt.New(t)

	cmd := compare.NewCompareCommand()

	schemaFile := cmd.Flags().Lookup("schema-file")
	c.Assert(schemaFile, qt.IsNotNil)
	c.Assert(schemaFile.Value.Type(), qt.Equals, "stringArray")

	rootDir := cmd.Flags().Lookup("root-dir")
	c.Assert(rootDir, qt.IsNotNil)
	c.Assert(rootDir.Value.Type(), qt.Equals, "stringArray")
}

func TestCompareCommandExposesSchemaCommandFlags(t *testing.T) {
	c := qt.New(t)

	cmd := compare.NewCompareCommand()

	c.Assert(cmd.Flags().Lookup("schema-cmd"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("schema-format"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("allow-external-schema"), qt.IsNotNil)
}

func TestCompareCommandExposesOCITransportFlag(t *testing.T) {
	c := qt.New(t)

	cmd := compare.NewCompareCommand()

	c.Assert(cmd.Flags().Lookup("plain-http"), qt.IsNotNil)
}

func TestCompareCommandUsesDatabaseDialectForExternalSQL(t *testing.T) {
	c := qt.New(t)

	cmd := compare.NewCompareCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", sqlServerSchemaCommand,
		"--db-url", sqlServerDatabaseURL,
		"--connect-timeout", "1ns",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `error connecting to database: .*`)
}

func TestCompareCommandValidatesConnectTimeoutBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)

	cmd := compare.NewCompareCommand()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--schema-cmd", "/path/that/does/not/exist",
		"--db-url", "sqlite://test.db",
		"--connect-timeout", "invalid",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid --connect-timeout value "invalid": .*`)
}

func TestCompareCommandPlansNewSQLiteTablesWithForeignKey(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	schemaPath := filepath.Join(root, "schema.sql")
	databasePath := filepath.Join(root, "compare.db")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY
);
CREATE TABLE "posts" (
  "id" INTEGER PRIMARY KEY,
  "user_id" INTEGER NOT NULL,
  CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE
);
`), 0o600), qt.IsNil)
	stdout := &bytes.Buffer{}
	cmd := compare.NewCompareCommand()
	cmd.SetOut(stdout)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{
		"--root-dir", root,
		"--schema-file", schemaPath,
		"--schema-cmd=",
		"--db-url", "sqlite://" + filepath.ToSlash(databasePath),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, `CREATE TABLE "users"`)
	c.Assert(stdout.String(), qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(stdout.String(), qt.Contains, `CONSTRAINT "fk_posts_user" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE`)
}
