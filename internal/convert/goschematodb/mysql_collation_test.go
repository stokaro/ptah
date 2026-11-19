package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// utf8mb4Table declares table t with the default character set utf8mb4 and
// no collation, and its column n with neither.
func utf8mb4Table() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Charset: "utf8mb4"}))}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "n", Type: "varchar(20)", Nullable: true}},
	}
}

// TestToDBSchemaOn_PredictsTheCollations converts a declaration as a read of
// the table it creates reports it: the table holds the default collation of
// the character set it names, and the column the collation it takes from its
// table. The defaults are the facts a caller hands, a dev database's read, or
// the target descriptor's where it hands none, and the result carries them.
func TestToDBSchemaOn_PredictsTheCollations(t *testing.T) {
	for _, test := range []struct {
		name, dialect string
		facts         goschematodb.ServerFacts
		want          string
	}{
		{name: "MySQL, the descriptor", dialect: "mysql", want: "utf8mb4_0900_ai_ci"},
		{name: "MariaDB, the descriptor", dialect: "mariadb", want: "utf8mb4_uca1400_ai_ci"},
		{name: "MySQL, a session", dialect: "mysql", want: "utf8mb4_general_ci",
			facts: goschematodb.ServerFacts{CharsetDefaultCollations: map[string]string{"utf8mb4": "utf8mb4_general_ci"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := goschematodb.ToDBSchemaOn(c.Context(), utf8mb4Table(), test.dialect, must.Must(builtin.New()), test.facts)

			c.Assert(err, qt.IsNil)
			c.Assert(converted.Tables, qt.HasLen, 1)
			c.Assert(converted.Tables[0].Collate, qt.Equals, test.want)
			c.Assert(converted.Tables[0].Columns, qt.HasLen, 1)
			c.Assert(converted.Tables[0].Columns[0].EffectiveCollate, qt.Equals, test.want)
			c.Assert(converted.CharsetDefaultCollations["utf8mb4"], qt.Equals, test.want)
		})
	}
}
