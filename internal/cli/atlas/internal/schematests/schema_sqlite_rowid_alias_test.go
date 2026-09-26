package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// The same SQLite table declared in SQL and in HCL, where the HCL says the key
// column is NOT NULL and the SQL does not. On the rowid alias that flag changes
// nothing SQLite does, so neither `schema diff` nor `schema apply` may plan the
// table rebuild a nullability change costs there (stokaro/ptah#3685).
const (
	rowidAliasSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
	rowidAliasHCL = "schema \"main\" {\n}\n" +
		"table \"widgets\" {\n  schema = schema.main\n" +
		"  column \"id\" {\n    null = false\n    type = integer\n  }\n" +
		"  primary_key {\n    columns = [column.id]\n  }\n}\n"
)

// rowidAliasFiles writes both declarations and returns their file:// URLs.
func rowidAliasFiles(c *qt.C) (sqlURL, hclURL string) {
	c.Helper()
	dir := c.TempDir()
	sqlPath := filepath.Join(dir, "schema.sql")
	hclPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(sqlPath, []byte(rowidAliasSQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(hclPath, []byte(rowidAliasHCL), 0o600), qt.IsNil)
	return "file://" + sqlPath, "file://" + hclPath
}

func TestSchemaDiffSQLiteRowidAliasDeclaredTwiceIsSynced(t *testing.T) {
	tests := []struct {
		name string
		args func(sqlURL, hclURL, db, dev string) []string
	}{
		{
			name: "SQL to HCL",
			args: func(sqlURL, hclURL, _, dev string) []string {
				return []string{"--from", sqlURL, "--to", hclURL, "--dev-url", dev}
			},
		},
		{
			name: "HCL to SQL",
			args: func(sqlURL, hclURL, _, dev string) []string {
				return []string{"--from", hclURL, "--to", sqlURL, "--dev-url", dev}
			},
		},
		{
			name: "a database built from the SQL to HCL",
			args: func(_, hclURL, db, dev string) []string {
				return []string{"--from", db, "--to", hclURL, "--dev-url", dev}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sqlURL, hclURL := rowidAliasFiles(c)
			db := "sqlite://" + atlastest.SeedSQLiteDB(c, rowidAliasSQL)

			stdout, stderr, err := atlastest.RunCompat(append([]string{"schema", "diff"},
				tt.args(sqlURL, hclURL, db, atlastest.FreshDevURL(c))...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}

// TestSchemaApplySQLiteRowidAliasDeclaredTwiceIsSynced applies the HCL to a
// database the SQL built. An HCL desired state needs no dev database, and the
// plan is the same with one.
func TestSchemaApplySQLiteRowidAliasDeclaredTwiceIsSynced(t *testing.T) {
	tests := []struct {
		name  string
		extra func(dev string) []string
	}{
		{name: "without a dev database", extra: func(string) []string { return nil }},
		{name: "with a dev database", extra: func(dev string) []string { return []string{"--dev-url", dev} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, hclURL := rowidAliasFiles(c)
			db := "sqlite://" + atlastest.SeedSQLiteDB(c, rowidAliasSQL)

			stdout, stderr, err := atlastest.RunCompat(append([]string{
				"schema", "apply", "--url", db, "--to", hclURL, "--dry-run",
			}, tt.extra(atlastest.FreshDevURL(c))...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
			c.Assert(stdout, qt.Not(qt.Contains), "__ptah_rebuild_widgets")
			c.Assert(stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaDiffSQLiteTextKeyNullabilityStillRebuilds is the control: a TEXT
// key holds NULL on a rowid table, so declaring it NOT NULL is a real change
// and SQLite can only make it by rebuilding the table.
func TestSchemaDiffSQLiteTextKeyNullabilityStillRebuilds(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	from := filepath.Join(dir, "from.sql")
	to := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(from, []byte("CREATE TABLE widgets (id TEXT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte("CREATE TABLE widgets (id TEXT NOT NULL PRIMARY KEY);\n"), 0o600), qt.IsNil)

	stdout, stderr, err := atlastest.RunCompat("schema", "diff",
		"--from", "file://"+from, "--to", "file://"+to, "--dev-url", atlastest.FreshDevURL(c))

	c.Assert(err, qt.IsNil, qt.Commentf("stderr=%q", stderr))
	c.Assert(stdout, qt.Contains, `"id" TEXT NOT NULL PRIMARY KEY`)
	c.Assert(stdout, qt.Contains, "__ptah_rebuild_widgets")
}
