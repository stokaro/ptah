//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// collatedKeyRenderTable is the table every collatedKeyRenders document
// indexes.
const collatedKeyRenderTable = "CREATE TABLE t (id INT PRIMARY KEY, code VARCHAR(20));\n"

// collatedKeyRenders are documents whose index keys put a column under a
// COLLATE in the spellings a reviewer measured, each with the statements
// `schema render` writes for them. Only SQLite respells such a key. MySQL
// reads `"code"` as a string unless ANSI_QUOTES is set, so on MySQL, SQL
// Server and ClickHouse the key is written as the document wrote it, and the
// statements are the ones master renders byte for byte.
var collatedKeyRenders = []struct {
	name     string
	dialect  string
	document string
	want     []string
}{
	{
		name:    "mysql",
		dialect: "mysql",
		document: collatedKeyRenderTable + "CREATE INDEX i1 ON t (\"code\" COLLATE NOCASE DESC);\n" +
			"CREATE INDEX i2 ON t (code COLLATE NOCASE ASC);\nCREATE INDEX i3 ON t (`code` COLLATE /*x*/ NOCASE);\n",
		want: []string{
			"CREATE INDEX `i1` ON `t` ((\"code\" COLLATE NOCASE DESC));",
			"CREATE INDEX `i2` ON `t` ((code COLLATE NOCASE ASC));",
			"CREATE INDEX `i3` ON `t` ((`code` COLLATE /*x*/ NOCASE));",
		},
	},
	{
		name:    "sqlserver",
		dialect: "sqlserver",
		document: collatedKeyRenderTable + "CREATE INDEX i1 ON t (\"code\" COLLATE NOCASE DESC);\n" +
			"CREATE INDEX i2 ON t (code COLLATE NOCASE ASC);\nCREATE INDEX i3 ON t ([code] COLLATE /*x*/ NOCASE);\n",
		want: []string{
			`CREATE INDEX [i1] ON [t] ("code" COLLATE NOCASE DESC);`,
			`CREATE INDEX [i2] ON [t] (code COLLATE NOCASE ASC);`,
			`CREATE INDEX [i3] ON [t] ([code] COLLATE /*x*/ NOCASE);`,
		},
	},
	{
		name:     "clickhouse",
		dialect:  "clickhouse",
		document: collatedKeyRenderTable + "CREATE INDEX i1 ON t (\"code\" COLLATE NOCASE DESC);\nCREATE INDEX i2 ON t (code COLLATE NOCASE ASC);\n",
		want: []string{
			"ALTER TABLE `t` ADD INDEX `i1` \"code\" COLLATE NOCASE DESC TYPE minmax GRANULARITY 1;",
			"ALTER TABLE `t` ADD INDEX `i2` code COLLATE NOCASE ASC TYPE minmax GRANULARITY 1;",
		},
	},
	{
		name:    "sqlite respells the key",
		dialect: "sqlite",
		document: collatedKeyRenderTable + "CREATE INDEX i1 ON t (\"code\" COLLATE NOCASE DESC);\n" +
			"CREATE INDEX i2 ON t (code COLLATE \"NOCASE\" ASC);\nCREATE INDEX i3 ON t ([code] COLLATE /*x*/ NOCASE);\n",
		want: []string{
			`CREATE INDEX IF NOT EXISTS "i1" ON "t" (code COLLATE NOCASE DESC);`,
			`CREATE INDEX IF NOT EXISTS "i2" ON "t" (code COLLATE NOCASE);`,
			`CREATE INDEX IF NOT EXISTS "i3" ON "t" (code COLLATE NOCASE);`,
		},
	},
}

// TestSchemaRenderSpellsACollatedKeyAsItsDialectReadsIt_HappyPathE2E renders
// each of collatedKeyRenders and finds every statement it lists, with nothing
// reported as skipped.
func TestSchemaRenderSpellsACollatedKeyAsItsDialectReadsIt_HappyPathE2E(t *testing.T) {
	for _, test := range collatedKeyRenders {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(test.document), 0o600), qt.IsNil)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
				"schema", "render", "--dialect", test.dialect, "--schema-file", "schema.sql")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			for _, statement := range test.want {
				c.Assert(result.Stdout, qt.Contains, "\n"+statement+"\n")
			}
			c.Assert(result.Stdout+result.Stderr, qt.Not(qt.Contains), "skipped")
		})
	}
}
