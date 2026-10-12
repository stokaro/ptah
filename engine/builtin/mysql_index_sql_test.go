package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// TestMySQLIndexOptions_ReadFromSQL reads a key's KEY_BLOCK_SIZE, a FULLTEXT
// index's guarded WITH PARSER and a primary key's block size through the
// bundled runtime's SQL owners, in a MySQL document and in a MariaDB one, and
// the document's claim that it states every index's block size.
func TestMySQLIndexOptions_ReadFromSQL(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())

			database, _, err := sqlschema.ReadWith(c.Context(),
				[]byte("CREATE TABLE t (id int, a int, bio text, PRIMARY KEY (id) KEY_BLOCK_SIZE=4, KEY k (a) KEY_BLOCK_SIZE=8, "+
					"FULLTEXT KEY ft (bio) /*!50100 WITH PARSER `ngram` */);"),
				dialect, nil, sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary()})

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(declaredBlockSize(c, database.Tables[0].PrimaryKeyFacets), qt.Equals, uint64(4))
			c.Assert(database.Indexes, qt.HasLen, 2)
			c.Assert(declaredBlockSize(c, database.Indexes[0].Facets), qt.Equals, uint64(8))
			options, found, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](database.Indexes[1].Facets, mysqlschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(options.Parser, qt.Equals, "ngram")
			c.Assert(database.FeatureCoverage, qt.DeepEquals, must.Must(mysqlsource.BlockSizeCoverage()))
		})
	}
}

// declaredBlockSize is the block size facets declare, which they must.
func declaredBlockSize(c *qt.C, facets schemaext.Facets) uint64 {
	c.Helper()
	size, found, err := mysqlschema.IndexBlockSize(facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return size
}
