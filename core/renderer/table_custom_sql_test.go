package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// customSQLDatabase is one table carrying the raw tail the `custom` table
// attribute declares.
func customSQLDatabase(custom string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Event",
			Name:       "events",
			CustomSQL:  custom,
		}},
		Fields: []schemamodel.Field{
			{StructName: "Event", Name: "id", Type: "INTEGER", Primary: true},
		},
	}
}

// TestTableCustomSQL_HappyPath appends the author's raw tail to CREATE TABLE on
// every dialect.
//
// The attribute has been parsed into [schemamodel.Table.CustomSQL] since the
// first commit and read by nothing on the way to SQL, so a declared
// `custom="PARTITION BY RANGE (id)"` produced an ordinary unpartitioned table
// and reported success (stokaro/ptah#2590).
//
// The text is emitted verbatim on every dialect rather than gated on one. Ptah
// does not parse it, so it has nothing to decide a gate with -- the author
// wrote it for the target they are rendering.
func TestTableCustomSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlite", dialect: "sqlite"},
		{name: "sqlserver", dialect: "sqlserver"},
		{name: "oracle", dialect: "oracle"},
		{name: "clickhouse", dialect: "clickhouse"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(
				customSQLDatabase("PARTITION BY RANGE (id)"), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, ") PARTITION BY RANGE (id);")
		})
	}
}

// TestTableCustomSQL_ATableDeclaringNoneClosesNormally is the acceptance control
// for the test above: a renderer that always wrote a space before the semicolon
// would leave `) ;` behind, and one that wrote a fixed clause would satisfy the
// containment assertion without reading the declaration.
func TestTableCustomSQL_ATableDeclaringNoneClosesNormally(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(customSQLDatabase(""), "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, ");")
	c.Assert(sql, qt.Not(qt.Contains), ") ;")
	c.Assert(sql, qt.Not(qt.Contains), "PARTITION BY")
}

// TestTableCustomSQL_AnAnnotatedTableRendersItsTail drives the surface the
// attribute is documented on.
func TestTableCustomSQL_AnAnnotatedTableRendersItsTail(t *testing.T) {
	c := qt.New(t)

	const source = `package models

//ptah:schema:table name="events" custom="WITHOUT OIDS"
type Event struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`

	database, err := goschema.ParseSource("models.go", source)
	c.Assert(err, qt.IsNil)

	statements, renderErr := renderer.GetOrderedCreateStatements(&database, "postgres")

	c.Assert(renderErr, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, ") WITHOUT OIDS;")
}
