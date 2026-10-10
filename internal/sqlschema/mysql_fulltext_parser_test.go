package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/sqlschema"
)

// TestRead_AFulltextParserIsAMySQLIndexOption reads `WITH PARSER` as the MySQL
// owner's index option, bound to the MySQL family, on an index declared in the
// table body and on one an ALTER TABLE adds.
func TestRead_AFulltextParserIsAMySQLIndexOption(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "in the table body",
			sql: "CREATE TABLE docs (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT KEY ft_bio (bio) WITH PARSER ngram);"},
		{name: "added by ALTER TABLE",
			sql: "CREATE TABLE docs (id BIGINT PRIMARY KEY, bio TEXT); ALTER TABLE docs ADD FULLTEXT KEY ft_bio (bio) WITH PARSER ngram;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.MySQL)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Indexes, qt.HasLen, 1)
			options, found, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](database.Indexes[0].Facets, mysqlschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(*options, qt.Equals, mysqlschema.DesiredIndex{Parser: "ngram"})
			c.Assert(database.Indexes[0].Facets.TargetScope(mysqlschema.IndexKind), qt.DeepEquals, []string{"mariadb", "mysql"})
		})
	}
}

// TestRead_AnIndexWithoutAParserDeclaresNoOption is the control: an index
// without the clause carries no MySQL index option.
func TestRead_AnIndexWithoutAParserDeclaresNoOption(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("CREATE TABLE docs (id BIGINT PRIMARY KEY, bio TEXT, FULLTEXT KEY ft_bio (bio));"), platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Indexes, qt.HasLen, 1)
	c.Assert(database.Indexes[0].Facets.IsZero(), qt.IsTrue)
}
