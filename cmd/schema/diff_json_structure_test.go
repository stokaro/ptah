package schema_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// schemaDiffJSON is the document `schema diff --format json` writes, decoded
// down to the fields these tests are about.
type schemaDiffJSON struct {
	FormatVersion int      `json:"format_version"`
	Statements    []string `json:"statements"`
	Changes       *struct {
		TablesAdded    []string `json:"tables_added"`
		TablesRemoved  []string `json:"tables_removed"`
		TablesModified []struct {
			TableName    string   `json:"table_name"`
			ColumnsAdded []string `json:"columns_added"`
		} `json:"tables_modified"`
	} `json:"changes"`
}

func runSchemaDiffJSON(c *qt.C, args ...string) schemaDiffJSON {
	c.Helper()

	out, err := runSchema("", append([]string{"diff", "--format", "json"}, args...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	document := schemaDiffJSON{}
	c.Assert(json.Unmarshal([]byte(out), &document), qt.IsNil, qt.Commentf("%s", out))
	return document
}

// TestSchemaDiffJSONReportsStructuralChanges pins the reading that does not go
// through SQL.
//
// Deciding "does this diff add a column to users" from the statements alone
// means parsing DDL, and that parser is wrong for every dialect it was not
// tested against. The structural half answers it as data (stokaro/ptah#1229).
func TestSchemaDiffJSONReportsStructuralChanges(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	document := runSchemaDiffJSON(c,
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(document.Changes, qt.IsNotNil)
	c.Assert(document.Changes.TablesAdded, qt.DeepEquals, []string{"orders"})
	c.Assert(document.Changes.TablesModified, qt.HasLen, 1)
	c.Assert(document.Changes.TablesModified[0].TableName, qt.Equals, "users")
	c.Assert(document.Changes.TablesModified[0].ColumnsAdded, qt.DeepEquals, []string{"email"})

	// The rendered half is unchanged, because a consumer reading it today must
	// keep working: the structural half was ADDED beside it, not instead of it.
	c.Assert(document.Statements, qt.HasLen, 2)
	c.Assert(document.FormatVersion, qt.Equals, 1)
}

// TestSchemaDiffJSONStructureAgreesWithStatementsUnderPolicy is the reason the
// document carries the comparison from AFTER the diff policy rather than the
// one the comparator produced.
//
// With drop_table skipped, no statement removes `legacy`. A document whose
// structural half still listed it would be telling a CI check that a table is
// being dropped while the statements it would run drop nothing -- one document
// answering its own question two ways. Measured: reporting the pre-policy
// comparison here prints `"statements": []` beside
// `"tables_removed": ["legacy"]`.
func TestSchemaDiffJSONStructureAgreesWithStatementsUnderPolicy(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	configPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(configPath, []byte("diff {\n  skip {\n    drop_table = true\n  }\n}\n"), 0o600), qt.IsNil)

	skipped := runSchemaDiffJSON(c,
		"--config", configPath,
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "skipped.db"),
	)

	c.Assert(skipped.Statements, qt.HasLen, 0)
	c.Assert(skipped.Changes, qt.IsNotNil)
	c.Assert(skipped.Changes.TablesRemoved, qt.HasLen, 0)

	// The control, without which both assertions above would pass on a
	// comparator that simply never reports a removed table.
	dropped := runSchemaDiffJSON(c,
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dropped.db"),
	)

	c.Assert(dropped.Statements, qt.HasLen, 1)
	c.Assert(dropped.Changes.TablesRemoved, qt.DeepEquals, []string{"legacy"})
}

// TestSchemaDiffJSONSyncedSchemasCarryBothHalves pins the empty document.
//
// A CI check reads the same fields whether or not there is a change, so
// "changes" is present and empty rather than absent: an absent key and an empty
// one are the same value to a consumer that does not distinguish them, and
// opposite values to one that does.
func TestSchemaDiffJSONSyncedSchemasCarryBothHalves(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	document := runSchemaDiffJSON(c,
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
	)

	c.Assert(document.Statements, qt.HasLen, 0)
	c.Assert(document.Changes, qt.IsNotNil)
	c.Assert(document.Changes.TablesAdded, qt.HasLen, 0)
}
