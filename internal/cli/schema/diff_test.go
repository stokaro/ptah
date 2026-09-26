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

// Native `ptah schema diff` compares a schema file with a database without a
// dev database, reading the file as written. `ptah-compat schema diff` refuses
// the same argv by default, as the community binary does, and this is the
// capability it keeps behind PTAH_ATLAS_DIFF_WITHOUT_DEV_URL
// (stokaro/ptah#3676). The native command never reads that variable.
func TestSchemaDiffComparesAFileWithADatabaseWithoutADevDatabase(t *testing.T) {
	tests := []struct {
		name       string
		fileSide   string
		wantInPlan string
	}{
		{name: "a SQL file on --from", fileSide: "--from", wantInPlan: `DROP TABLE IF EXISTS "orders"`},
		{name: "a SQL file on --to", fileSide: "--to", wantInPlan: `CREATE TABLE "orders"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "live.db")
			seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
			filePath := writeSchemaSQLFile(c, dir, "schema.sql",
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
			sides := map[string]string{"--from": "sqlite://" + dbPath, "--to": "sqlite://" + dbPath}
			sides[tt.fileSide] = filePath

			out, err := runSchema("", "diff", "--from", sides["--from"], "--to", sides["--to"])

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, tt.wantInPlan)
		})
	}
}

// TestSchemaDiffSQLiteRowidAliasDeclaredTwiceIsSynced: the rowid alias declared
// with and without NOT NULL is one table on SQLite, which stores no NULL in it
// either way, so native `ptah schema diff` plans no rebuild between the two
// (stokaro/ptah#3685).
func TestSchemaDiffSQLiteRowidAliasDeclaredTwiceIsSynced(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sqlPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	hclPath := writeSchemaSQLFile(c, dir, "schema.hcl", "schema \"main\" {\n}\n"+
		"table \"widgets\" {\n  schema = schema.main\n"+
		"  column \"id\" {\n    null = false\n    type = integer\n  }\n"+
		"  primary_key {\n    columns = [column.id]\n  }\n}\n")

	out, err := runSchema("", "diff",
		"--from", sqlPath,
		"--to", hclPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced, no changes to be made.")
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
