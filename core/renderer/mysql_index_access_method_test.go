package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// An index carrying a USING clause has to render it, or the parser's work is
// undone on the way out. Measured 2026-09-03, MySQL 8.4.11 and MariaDB 11.8.9
// each accept the clause after the column list, before ON, and on the UNIQUE
// form; after the column list is where this renders it. See stokaro/ptah#2825.

func mysqlAccessMethodIndex(indexType string) *ast.IndexNode {
	index := ast.NewIndex("kh", "t", "a")
	index.Type = indexType
	return index
}

func TestRenderIndexAccessMethod_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, mysqlAccessMethodIndex("HASH"))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE INDEX `kh` ON `t` (`a`) USING HASH")
		})
	}
}

// The control that keeps the clause off every other index. BTREE is what the
// server reports for an index that asked for nothing, so emitting it would put
// a USING clause into the DDL of every index Ptah ever read back.
func TestRenderIndexAccessMethod_DefaultRendersNoClause(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
	}{
		{name: "btree", indexType: "BTREE"},
		{name: "unset", indexType: ""},
		{name: "fulltext prefix", indexType: "FULLTEXT"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				sql, err := renderer.RenderSQL(dialect, mysqlAccessMethodIndex(test.indexType))

				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Not(qt.Contains), "USING")
			})
		}
	}
}
