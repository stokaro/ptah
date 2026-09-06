package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// functionalIndexDatabase reads one MySQL document and returns the model.
//
// The dialect is passed to ToDatabase as well as to the parser: the naming pass
// asks it which engine's rules apply, and a document converted with no platform
// gets no derived names at all.
func functionalIndexDatabase(c *qt.C, sql string) schemamodel.Database {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect("mysql")).Parse()
	c.Assert(err, qt.IsNil)
	database, err := sqlschema.ToDatabase(statements, "mysql")
	c.Assert(err, qt.IsNil)
	return database
}

// TestToDatabase_UnnamedFunctionalIndexTakesTheServersName covers the naming
// half of stokaro/ptah#2758.
//
// MySQL names an index whose first key part is an expression itself, and the
// name is not arbitrary: a desired schema that guessed differently would never
// converge with the catalog, which is why the rule is derived here rather than
// left to the author.
//
// Measured on MySQL 8.4. Three unnamed functional keys on one table come back
// from information_schema.STATISTICS as functional_index, functional_index_2
// and functional_index_3, which is the same `_N` walk every other derived name
// takes.
func TestToDatabase_UnnamedFunctionalIndexTakesTheServersName(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		want  []string
		table string
	}{
		{
			name:  "one unnamed functional key",
			sql:   "CREATE TABLE t (a INT NOT NULL, KEY ((a + 1)));",
			want:  []string{"functional_index"},
			table: "t",
		},
		{
			name:  "three of them walk the suffix",
			sql:   "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, KEY ((a + 1)), KEY ((b + 1)), KEY ((a + 2)));",
			want:  []string{"functional_index", "functional_index_2", "functional_index_3"},
			table: "t",
		},
		{
			name:  "an author's own name is left alone",
			sql:   "CREATE TABLE t (a INT NOT NULL, KEY k ((a + 1)));",
			want:  []string{"k"},
			table: "t",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := functionalIndexDatabase(c, test.sql)

			names := make([]string, 0, len(database.Indexes))
			for _, index := range database.Indexes {
				names = append(names, index.Name)
			}
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

// TestToDatabase_AFunctionalIndexKeepsItsExpression is the assertion the naming
// change would otherwise hide.
//
// A derived name proves the index was recognized; it does not prove the
// expression survived. An index named functional_index whose part lost `a + 1`
// is an index over nothing, and it would still satisfy every assertion above.
func TestToDatabase_AFunctionalIndexKeepsItsExpression(t *testing.T) {
	c := qt.New(t)
	database := functionalIndexDatabase(c, "CREATE TABLE t (a INT NOT NULL, KEY ((a + 1)));")

	c.Assert(database.Indexes, qt.HasLen, 1)
	c.Assert(database.Indexes[0].Parts, qt.HasLen, 1)
	c.Assert(database.Indexes[0].Parts[0].Expr, qt.Equals, "a + 1")
	c.Assert(database.Indexes[0].Parts[0].Name, qt.Equals, "")
}
