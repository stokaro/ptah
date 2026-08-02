package migrate_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/migration/safety"
)

func TestMigratePlanTextOutputContainsSQLNotASTPointers(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "models.go"), []byte(`package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`), 0o600), qt.IsNil)

	var out bytes.Buffer
	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--root-dir", modelsDir,
		"--db-url", "sqlite:///" + filepath.Join(dir, "ptah.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `CREATE TABLE "users"`)
	c.Assert(out.String(), qt.Not(qt.Contains), "[0x")
	c.Assert(out.String(), qt.Not(qt.Contains), "&{")
}

func TestMigratePlanJSONReportEmitsStructuredSafetyOnStdout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	var stdout, stderr bytes.Buffer
	cmd := migrate.NewMigrateCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--schema-file", schemaFile,
		"--db-url", "sqlite:///" + filepath.Join(dir, "ptah.db"),
		"--report", "json",
	})

	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	var report safety.Report
	c.Assert(json.Unmarshal(stdout.Bytes(), &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Safe)
	c.Assert(report.Destructive, qt.IsFalse)
	c.Assert(report.Assessments, qt.HasLen, 1)
	c.Assert(report.Assessments[0].Statement, qt.Contains, `CREATE TABLE "users"`)
}

func TestMigratePlan_OCIFlags(t *testing.T) {
	c := qt.New(t)
	cmd := migrate.NewMigrateCommand()

	c.Assert(cmd.Flag("attach"), qt.IsNotNil)
	c.Assert(cmd.Flag("plain-http"), qt.IsNotNil)
}

func TestMigratePlan_AttachRejectsLocalSchemaSource(t *testing.T) {
	c := qt.New(t)
	schemaFile := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER);\n"), 0o600), qt.IsNil)
	cmd := migrate.NewMigrateCommand()
	cmd.SetArgs([]string{
		"--schema-file", schemaFile,
		"--db-url", "sqlite:///" + filepath.Join(t.TempDir(), "ptah.db"),
		"--attach",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "--attach requires exactly one OCI --schema-file source")
}
