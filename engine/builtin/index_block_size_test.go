package builtin_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
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
		&ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"a"}, Facets: blockSizeFacets(8)},
		&ast.ConstraintNode{Type: ast.UniqueConstraint, Columns: []string{"a"}, Facets: blockSizeFacets(8)},
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

// keyBlockSizeSchema is table t with primary key id and the comment,
// carrying the hint on the table or on a PRIMARY KEY constraint, and a UNIQUE
// constraint carrying one where unique says so.
func keyBlockSizeSchema(onConstraint, unique bool, comment string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKeyComment: comment}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT"}, {StructName: "T", Name: "a", Type: "INT"}},
	}
	db.Tables[0].PrimaryKey = map[bool][]string{false: {"id"}}[onConstraint]
	db.Tables[0].PrimaryKeyFacets = map[bool]schemaext.Facets{false: blockSizeFacets(8)}[onConstraint]
	db.Constraints = map[bool][]schemamodel.Constraint{true: {{StructName: "T", Name: "pk_t", Type: "PRIMARY KEY", Columns: []string{"id"},
		Facets: blockSizeFacets(8)}}}[onConstraint]
	db.Constraints = append(db.Constraints, map[bool][]schemamodel.Constraint{true: {{StructName: "T", Name: "u", Type: "UNIQUE",
		Columns: []string{"a"}, Facets: blockSizeFacets(8)}}}[unique]...)
	return db
}

// CREATE TABLE writes a primary key's hint where the MySQL owner keeps it,
// for a key declared on the table and for a PRIMARY KEY constraint.
func TestRender_PrimaryKeyBlockSize_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, comment string
		onConstraint           bool
		want                   string
	}{
		{"MySQL, on the table", "mysql", "lookup", false, "PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8 COMMENT 'lookup'"},
		{"MariaDB, on the table", "mariadb", "lookup", false, "PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8 COMMENT 'lookup'"},
		// The hint alone is what makes a one-column key a table constraint.
		{"MySQL, the hint alone", "mysql", "", false, "  PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8\n"},
		{"MySQL, a constraint", "mysql", "", true, "PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(keyBlockSizeSchema(test.onConstraint, false, test.comment), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// A target without the hint refuses a primary key that declares one, and the
// MySQL family refuses one on a UNIQUE constraint, which a unique index
// carries instead.
func TestRender_PrimaryKeyBlockSize_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, dialect        string
		onConstraint, unique bool
		wantErr              string
	}{
		{"PostgreSQL, on the table", "postgres", false, false, `(?s).*KEY_BLOCK_SIZE.*does not support.*`},
		{"PostgreSQL, a constraint", "postgres", true, false, `(?s).*KEY_BLOCK_SIZE.*does not support.*`},
		{"MySQL, a UNIQUE constraint", "mysql", false, true, `(?s).*KEY_BLOCK_SIZE on UNIQUE constraint "u" is not supported; declare a unique index for a UNIQUE key.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(keyBlockSizeSchema(test.onConstraint, test.unique, ""), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
