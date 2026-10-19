// Package registration_test drives platform.Register for the packages under
// migration/internal, which an importer outside migration cannot reach.
// platform.Register changes the process registry for the rest of the process,
// so the registrations stay in a test binary of their own, as the ones of
// core/platform/internal/registration do.
package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/internal/generatedschema"
)

// TestRegister_TypesTheRelationKeyOfAnEngineNobodyShips registers an engine
// that writes a SERIAL key as INT4. The generated key column of an embedded
// relation takes that type on it, beside MySQL and MariaDB, though nothing
// names the engine. The fields are read first, so the answer derived from the
// earlier registry exists and has to give way to the new one.
func TestRegister_TypesTheRelationKeyOfAnEngineNobodyShips(t *testing.T) {
	database := &schemamodel.Database{EmbeddedFields: []schemamodel.EmbeddedField{{
		StructName: "Post", Mode: "relation", Field: "author_id", Ref: "users(id)", EmbeddedTypeName: "Author",
	}}}
	table := schemamodel.Table{StructName: "Post"}
	c := qt.New(t)
	c.Assert(generatedschema.FieldsForTable(database, table)[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		platform.MySQL:   {"type": "INT"},
		platform.MariaDB: {"type": "INT"},
	})

	engine := descriptor.Dialect{
		Name:        "relational",
		Aliases:     []string{"reldb"},
		Identifiers: descriptor.Identifiers{IndexNamespace: "schema", IndexNames: "exact", TableNames: "exact", ColumnNames: "exact"},
		Placeholder: descriptor.PlaceholderDollar,
		Conversion:  descriptor.Conversion{SerialForeignKeyType: "INT4"},
	}
	c.Assert(platform.Register(engine), qt.IsNil)

	c.Assert(generatedschema.FieldsForTable(database, table)[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		platform.MySQL:   {"type": "INT"},
		platform.MariaDB: {"type": "INT"},
		"relational":     {"type": "INT4"},
	})
}
