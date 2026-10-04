package schema_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
)

// rebuildFrom and rebuildTo differ by one column type, a change YDB makes only
// by rebuilding the table.
const (
	rebuildFrom = "table \"rb\" {\n  column \"id\" {\n    type = Int64\n  }\n  column \"v\" {\n    type = Int32\n" +
		"    null = true\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n"
	rebuildTo = "table \"rb\" {\n  column \"id\" {\n    type = Int64\n  }\n  column \"v\" {\n    type = Int64\n" +
		"    null = true\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n"
)

// The native surface asks for a rebuild with --allow-table-rebuild, and its
// refusal names that flag. The dev URL only names the dialect here: a schema
// diff between two files connects to nothing, so a URL the YDB connector would
// refuse is never opened.
func TestSchemaDiffNamesTheRebuildFlagOnYDB(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.hcl", rebuildFrom)
	toPath := writeSchemaSQLFile(c, dir, "to.hcl", rebuildTo)

	out, err := runSchema("", "diff", "--from", fromPath, "--to", toPath,
		"--dev-url", "ydb://127.0.0.1:1/local?bogus=1")

	c.Assert(err, qt.ErrorMatches, `.*`+regexp.QuoteMeta(
		"YDB makes it by rebuilding the table, which Ptah plans when asked with --allow-table-rebuild"),
		qt.Commentf("%s", out))
}

// The flag is the request: with it, the same diff plans the rebuild.
func TestSchemaDiffPlansARebuildWithTheFlagOnYDB(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.hcl", rebuildFrom)
	toPath := writeSchemaSQLFile(c, dir, "to.hcl", rebuildTo)

	out, err := runSchema("", "diff", "--from", fromPath, "--to", toPath,
		"--dev-url", "ydb://127.0.0.1:1/local?bogus=1", "--allow-table-rebuild")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "ALTER TABLE `__ptah_rebuild_rb` RENAME TO `rb`;")
}
