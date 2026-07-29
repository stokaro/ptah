package schema_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSchemaDiffTwoFilesWithDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `CREATE TABLE "orders"`)
	c.Assert(out, qt.Not(qt.Contains), `CREATE TABLE "users"`)
}

func TestSchemaDiffSyncedSchemasReportNoChanges(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced, no changes to be made.")
}

func TestSchemaDiffJSONFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--format", "json",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var document struct {
		Statements []string `json:"statements"`
	}
	c.Assert(json.Unmarshal([]byte(out), &document), qt.IsNil)
	c.Assert(document.Statements, qt.HasLen, 1)
	c.Assert(document.Statements[0], qt.Contains, `CREATE TABLE "orders"`)
}

func TestSchemaDiffRequiresFromAndTo(t *testing.T) {
	c := qt.New(t)

	out, err := runSchema("", "diff")
	c.Assert(err, qt.ErrorMatches, "--from is required", qt.Commentf("%s", out))

	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	out, err = runSchema("", "diff", "--from", fromPath)
	c.Assert(err, qt.ErrorMatches, "--to is required", qt.Commentf("%s", out))
}

func TestSchemaDiffRejectsInvalidConnectTimeout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", fromPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--connect-timeout", "invalid",
	)

	c.Assert(err, qt.ErrorMatches, `invalid --connect-timeout value "invalid": .*`, qt.Commentf("%s", out))
}

func TestSchemaDiffRejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", fromPath,
		"--format", "{{ sql . }}",
	)

	c.Assert(err, qt.ErrorMatches, `unsupported --format .*: expected sql or json`, qt.Commentf("%s", out))
}
