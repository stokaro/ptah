package builtin_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
)

func TestRender_IndexBlockSize_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.SQLite, platform.CockroachDB, platform.YugabyteDB, platform.Spanner, platform.SQLServer, platform.Oracle, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			db := indexOptionsSchema()
			db.Indexes[0].Invisible = false
			db.Indexes[0].Facets = blockSizeFacets(8)
			statements, err := builtin.GetOrderedCreateStatements(db, dialect)
			c.Assert(err, qt.ErrorMatches, `(?s).*KEY_BLOCK_SIZE.*does not support.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestRender_IndexBlockSizeLimits_FailurePath(t *testing.T) {
	for _, test := range []struct {
		dialect string
		size    uint64
	}{{"mysql", 4294967296}, {"mariadb", 65536}} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			db := indexOptionsSchema()
			db.Indexes[0].Invisible = false
			db.Indexes[0].Facets = blockSizeFacets(test.size)
			statements, err := builtin.GetOrderedCreateStatements(db, test.dialect)
			c.Assert(err, qt.ErrorMatches, `(?s).*KEY_BLOCK_SIZE.*exceeds.*limit.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// The public AST renderer must refuse the hint too, including nested ALTERs.
func TestRender_IndexBlockSizeAST_FailurePath(t *testing.T) {
	for _, node := range []ast.Node{
		&ast.IndexNode{Name: "k", Table: "t", Columns: []string{"a"}, Facets: blockSizeFacets(8)},
		&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"a"}, KeyBlockSize: 8},
		&ast.ConstraintNode{Type: ast.UniqueConstraint, Columns: []string{"a"}, KeyBlockSize: 8},
		&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddIndexOperation{Index: &ast.IndexNode{Name: "k", Columns: []string{"a"}, Facets: blockSizeFacets(8)}}}},
		&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ReplaceIndexOperation{Index: &ast.IndexNode{Name: "k", Columns: []string{"a"}, Facets: blockSizeFacets(8)}}}},
	} {
		t.Run(fmt.Sprintf("%T", node), func(t *testing.T) {
			c := qt.New(t)
			_, err := builtin.RenderSQL(platform.Postgres, node)
			c.Assert(err, qt.IsNotNil)
		})
	}
}

// blockSizeFacets holds a block-size declaration as the MySQL owner's facet,
// built without the limit check a source applies, so a render can be shown
// refusing a value no source would hand it.
func blockSizeFacets(size uint64) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: size}))
}
