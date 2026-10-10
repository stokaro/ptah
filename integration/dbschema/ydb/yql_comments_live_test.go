//go:build integration

package ydb_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

func TestYDBDesiredYQL_Comments(t *testing.T) {
	const declarations = "CREATE TABLE items (id Int64 NOT NULL, body Utf8, PRIMARY KEY (id), INDEX by_body GLOBAL ON (body)); CREATE VIEW summary WITH (security_invoker = TRUE) AS SELECT id FROM items;"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connect(c, enterRealm(c, line))
			path := filepath.Join(c.TempDir(), "schema.sql")
			for _, suffix := range []string{
				"COMMENT ON TABLE items IS 'table'; COMMENT ON COLUMN items.body IS 'column'; COMMENT ON INDEX by_body ON items IS 'index'; COMMENT ON VIEW summary IS 'view';",
				"COMMENT ON TABLE items IS 'changed; table'; COMMENT ON COLUMN items.body IS 'line\\nnext'; COMMENT ON INDEX by_body ON items IS 'changed index'; COMMENT ON VIEW summary IS 'changed view';",
				"",
			} {
				c.Assert(os.WriteFile(path, []byte(declarations+suffix), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				statements := planAgainst(c, conn, desired, nil)
				c.Assert(statements, qt.Not(qt.HasLen), 0)
				apply(c, conn, statements)
				c.Assert(planAgainst(c, conn, desired, nil), qt.HasLen, 0)
			}
		})
	}
}
