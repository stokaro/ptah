package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

func TestRead_PrimaryKeyOptions_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, sql := range []string{
			"CREATE TABLE t(a int, PRIMARY KEY(a) KEY_BLOCK_SIZE=8 COMMENT 'lookup')",
			"CREATE TABLE t(a int); ALTER TABLE t ADD PRIMARY KEY(a) COMMENT 'lookup' KEY_BLOCK_SIZE 8",
		} {
			t.Run(dialect+"/"+sql, func(t *testing.T) {
				c := qt.New(t)
				db, _, err := readWithOwners([]byte(sql), dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(primaryKeyBlockSize(c, db.Tables[0]), qt.Equals, uint64(8))
				c.Assert(db.Tables[0].PrimaryKeyComment, qt.Equals, "lookup")
				ddl, err := builtin.GetOrderedCreateStatements(&db, dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(strings.Join(ddl, "\n"), qt.Contains, "PRIMARY KEY (`a`) KEY_BLOCK_SIZE=8 COMMENT 'lookup'")
				files, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
				c.Assert(err, qt.IsNil)
				c.Assert(files, qt.HasLen, 1)
				again, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))
				c.Assert(err, qt.IsNil)
				c.Assert(primaryKeyBlockSize(c, again.Tables[0]), qt.Equals, uint64(8))
				c.Assert(again.Tables[0].PrimaryKeyComment, qt.Equals, "lookup")
			})
		}
	}
}

// primaryKeyBlockSize is the hint the MySQL owner keeps on table's primary
// key.
func primaryKeyBlockSize(c *qt.C, table schemamodel.Table) uint64 {
	c.Helper()
	size, found, err := mysqlschema.IndexBlockSize(table.PrimaryKeyFacets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return size
}
