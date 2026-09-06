package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestTablesAndColumns_ACreationCarriesWhatCreateTableRenders pins the bundle a
// created table brings with it.
//
// The columns of every table live in one flat `Database.Fields` list keyed by
// the Go struct, so rendering one table meant being handed the whole desired
// schema and filtering it there. The filtering happens once, where the schema
// is already in hand (stokaro/ptah#2315).
//
// Three things travel: the declaration, this table's columns with embedded
// fields already folded in, and the enums those columns name. The other table's
// column is the control -- a bundle carrying every column would satisfy an
// assertion that only counted this one.
func TestTablesAndColumns_ACreationCarriesWhatCreateTableRenders(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "status", Type: "user_status"},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
		},
		// A json-mode embedding, which folds into ONE concrete column. The
		// fold is what makes `audit` a column of `users`, and a bundle built
		// from the raw field list would not carry it.
		EmbeddedFields: []schemamodel.EmbeddedField{
			{StructName: "User", Mode: "json", Name: "audit", Type: "JSONB", EmbeddedTypeName: "Audit"},
		},
		Enums: []schemamodel.Enum{
			{Name: "user_status", Values: []string{"active", "gone"}},
			{Name: "post_state", Values: []string{"draft", "live"}},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.TablesAndColumns(desired, &catalog.Database{}, diff)

	c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"posts", "users"})

	users := creationNamed(c, diff.TablesAdded, "users")
	c.Assert(users.Table.StructName, qt.Equals, "User")
	c.Assert(fieldNames(users.Fields), qt.DeepEquals, []string{"id", "status", "audit"},
		qt.Commentf("this table's columns, the embedded one folded in, and not the other table's"))
	c.Assert(enumNames(users.Enums), qt.DeepEquals, []string{"user_status"},
		qt.Commentf("the enums this table's columns name, and not every declared enum"))

	posts := creationNamed(c, diff.TablesAdded, "posts")
	c.Assert(fieldNames(posts.Fields), qt.DeepEquals, []string{"id"})
	c.Assert(posts.Enums, qt.HasLen, 0,
		qt.Commentf("a table naming no enum carries none"))
}

// creationNamed is the one creation the list holds under a name.
func creationNamed(c *qt.C, creations difftypes.TableChanges, name string) difftypes.TableCreation {
	c.Helper()
	matched := make(difftypes.TableChanges, 0, 1)
	for _, creation := range creations {
		if creation.Name == name {
			matched = append(matched, creation)
		}
	}
	c.Assert(matched, qt.HasLen, 1)
	return matched[0]
}

func fieldNames(fields []schemamodel.Field) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

func enumNames(enums []schemamodel.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

// TestTableChanges_NamesIsTheWireShape pins what the JSON keeps: `tables_added`
// has always been an array of names, and carrying the bundle in memory must not
// change what a stored plan holds.
func TestTableChanges_NamesIsTheWireShape(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.TableChanges{
		{Name: "users", Table: schemamodel.Table{StructName: "User", Name: "users"}},
		{Name: "billing.invoices"},
	}

	c.Assert(changes.Names(), qt.DeepEquals, []string{"users", "billing.invoices"})

	encoded, err := changes.MarshalJSON()
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `["users","billing.invoices"]`)
}
