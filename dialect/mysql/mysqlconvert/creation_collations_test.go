package mysqlconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqlschema"
)

// collationCreation declares table t with the default character set
// utf8mb4, the default collation collation where it is not empty, a column
// that names no collation, one that names its own, and one that names latin1.
func collationCreation(target, collation string, defaults map[string]string) schemaprojection.TableCreationRequest {
	semantics := identifier.ForDialect(target)
	return schemaprojection.TableCreationRequest{
		Target: target, Identifiers: semantics, CharsetDefaultCollations: defaults,
		Tables: []schemaprojection.TableCreationInput{{
			Subject: objectidentity.NewBuilder(semantics).TableParts("", "t"),
			Declaration: schemacapture.TableDeclaration{
				Table: schemamodel.Table{Name: "t", Collate: collation, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Charset: "utf8mb4"}))},
				Fields: []schemamodel.Field{
					{Name: "plain", Type: "varchar(20)"},
					{Name: "own", Type: "varchar(20)", Collate: "utf8mb4_bin"},
					{Name: "latin", Type: "varchar(20)", Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: "latin1"}))},
					{Name: "id", Type: "int"},
				},
			},
		}},
	}
}

// TestCreationService_PredictsTheCollations predicts the default collation
// a read of the created table reports and the collation of each column that
// names none, from the request's character set defaults: the table's own
// collation, or the default of the character set it names.
func TestCreationService_PredictsTheCollations(t *testing.T) {
	defaults := map[string]string{"latin1": "latin1_swedish_ci", "utf8mb4": "utf8mb4_0900_ai_ci"}
	for _, test := range []struct {
		name, target, collation string
		wantTable               string
		wantColumns             map[string]string
	}{
		{"a named collation", "mysql", "utf8mb4_unicode_ci", "utf8mb4_unicode_ci",
			map[string]string{"plain": "utf8mb4_unicode_ci", "latin": "latin1_swedish_ci"}},
		{"the character set's default", "mariadb", "", "utf8mb4_0900_ai_ci",
			map[string]string{"plain": "utf8mb4_0900_ai_ci", "latin": "latin1_swedish_ci"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqlconvert.CreationService{}.ProjectTableCreations(t.Context(), collationCreation(test.target, test.collation, defaults))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Tables, qt.HasLen, 1)
			c.Assert(result.Tables[0].Collation, qt.Equals, test.wantTable)
			c.Assert(result.Tables[0].ColumnCollations, qt.DeepEquals, test.wantColumns)
		})
	}
}

// TestCreationService_PredictsNoCollationWithoutDefaults is the control: a
// request that carries no character set defaults predicts no collation the
// defaults decide, and keeps the one the table names.
func TestCreationService_PredictsNoCollationWithoutDefaults(t *testing.T) {
	c := qt.New(t)

	result, err := mysqlconvert.CreationService{}.ProjectTableCreations(t.Context(), collationCreation("mysql", "", nil))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables, qt.HasLen, 1)
	c.Assert(result.Tables[0].Collation, qt.Equals, "")
	c.Assert(result.Tables[0].ColumnCollations, qt.IsNil)
}

// TestCreationService_PredictsTheStoredTypes predicts the type a read of the
// created table reports for each character column of the binary character
// set, the binary type of the same length, and leaves the other columns out.
func TestCreationService_PredictsTheStoredTypes(t *testing.T) {
	for _, target := range []string{"mysql", "mariadb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			request := collationCreation(target, "", nil)
			binary := must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: "binary"}))
			request.Tables[0].Declaration.Fields = append(request.Tables[0].Declaration.Fields,
				schemamodel.Field{Name: "v", Type: "varchar(10)", Facets: binary},
				schemamodel.Field{Name: "b", Type: "text", Facets: binary})

			result, err := mysqlconvert.CreationService{}.ProjectTableCreations(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Tables, qt.HasLen, 1)
			c.Assert(result.Tables[0].ColumnTypes, qt.DeepEquals, map[string]string{"v": "varbinary(10)", "b": "blob"})
		})
	}
}
