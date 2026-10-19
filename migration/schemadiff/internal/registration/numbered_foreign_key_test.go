// Package registration_test drives platform.Register for the packages under
// migration/schemadiff/internal, which an importer outside migration/schemadiff
// cannot reach. platform.Register changes the process registry for the rest of
// the process, so the registrations stay in a test binary of their own, as the
// ones of core/platform/internal/registration do.
package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestRegister_PairsTheNumberedForeignKeysOfAnEngineNobodyShips registers an
// engine whose descriptor states that some of its release lines name an
// unnamed foreign key by its number alone, as MariaDB 12.1 and later do. A
// schema file's key `c_ibfk_1` then compares equal to the database key `1` of
// the same definition, though nothing names the engine. The comparison is
// asked first, and plans to add the one and drop the other.
func TestRegister_PairsTheNumberedForeignKeysOfAnEngineNobodyShips(t *testing.T) {
	c := qt.New(t)
	before := compareNumbered("pairdb")
	c.Assert(before.ConstraintsAdded.Names(), qt.DeepEquals, []string{"c_ibfk_1"})
	c.Assert(before.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"1"})

	engine := descriptor.Dialect{
		Name:             "pairing",
		Aliases:          []string{"pairdb"},
		Identifiers:      descriptor.Identifiers{IndexNamespace: "table", IndexNames: "exact", TableNames: "exact", ColumnNames: "exact"},
		Placeholder:      descriptor.PlaceholderQuestion,
		IdentifierLimit:  descriptor.IdentifierLimit{Max: 64, Unit: "characters"},
		IndexNaming:      descriptor.IndexNaming{MaxBytes: 64},
		ForeignKeyNaming: descriptor.ForeignKeyNamingTableNumbered,
		ForeignKeyNameRules: descriptor.ForeignKeyNameRules{
			BareNumbers: true,
		},
	}
	c.Assert(platform.Register(engine), qt.IsNil)

	after := compareNumbered("pairdb")
	c.Assert(after.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(after.ConstraintsRemoved, qt.HasLen, 0)
}

// compareNumbered compares a schema file holding the unnamed key of `c(a)`,
// which the reader names `c_ibfk_1`, with a database holding the same key as
// `1`, as dialect.
func compareNumbered(dialect string) *difftypes.SchemaDiff {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "P", Name: "p"}, {StructName: "C", Name: "c"}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "a", Type: "int", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "C", Name: "c_ibfk_1", Type: "FOREIGN KEY", Table: "c",
			Columns: []string{"a"}, ForeignTable: "p", ForeignColumn: "id", OnDelete: "NO ACTION",
		}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true}}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "a", DataType: "int", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "1", TableName: "c", Type: "FOREIGN KEY", ColumnName: "a", ColumnNames: []string{"a"},
			ForeignTable: new("p"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
			DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
		}},
	}
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	diff := &difftypes.SchemaDiff{}
	compare.Constraints(desired, current, diff, opts)
	return diff
}
