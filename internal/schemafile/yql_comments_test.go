package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

func TestLoadYQLCommentsAcrossFiles(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	table := filepath.Join(dir, "table.sql")
	comments := filepath.Join(dir, "comments.sql")
	c.Assert(os.WriteFile(table, []byte("CREATE TABLE `dir/t` (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id), INDEX by_body GLOBAL ON (body));"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(comments, []byte("COMMENT ON TABLE `dir/t` IS 'table'; COMMENT ON COLUMN `dir/t`.body IS 'column'; COMMENT ON INDEX by_body ON `dir/t` IS 'index';"), 0o600), qt.IsNil)
	database, err := schemafile.LoadAll([]string{table, comments}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables[0].Comment, qt.Equals, "table")
	c.Assert(database.Fields[1].Comment, qt.Equals, "column")
	c.Assert(database.Indexes[0].Comment, qt.Equals, "index")
}
