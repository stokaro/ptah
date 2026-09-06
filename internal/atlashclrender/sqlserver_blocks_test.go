package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRenderWritesTheSQLServerBlocksHCLHadNoWordFor pins the shape of the two
// blocks stokaro/ptah#1031 added.
//
// The document is what `ptah schema inspect` hands an operator, so a family it
// cannot name is a family that round trip loses: rendering an inspected SQL
// Server database, reading the file back and applying it planned DROP SYNONYM
// and sp_dropextendedproperty for every one of them.
//
// The property's owner is written as PLAIN STRINGS rather than as `table.users`
// and `column.title` references, and that is the decision this test pins. SQL
// Server addresses a property by name -- `@level1name = N'users'` -- and a
// reference would resolve to nothing the moment selection removed the table,
// producing a document naming a block it does not contain.
func TestRenderWritesTheSQLServerBlocksHCLHadNoWordFor(t *testing.T) {
	c := qt.New(t)

	db := sqlServerObjects()

	result, err := atlashclrender.RenderInspected(db, platform.SQLServer, "dbo")

	c.Assert(err, qt.IsNil)
	document := string(result.Data)
	c.Assert(document, qt.Contains,
		"synonym \"s_users\" {\n  schema = schema.dbo\n  target = \"other.dbo.users\"\n}")
	c.Assert(document, qt.Contains,
		"extended_property \"MS_Description\" {\n  schema = schema.dbo\n"+
			"  table = \"users\"\n  column = \"title\"\n  value = \"the title\"\n}")
}

// TestRenderInspectedKeepsADatabaseScopedPropertyAtDatabaseScope is the row the
// schema fallback must not reach.
//
// An empty Schema on an extended property is not a schema the read failed to
// report: it is the DATABASE scope, a fourth address alongside schema, table
// and column, which the SQL Server renderer emits by passing no `@level0type`
// at all. Substituting the connection default for it -- the fallback every
// other schema-scoped block wants -- moves the property onto a schema, and the
// round trip then reports it as a property on `dbo` that the database never
// had.
func TestRenderInspectedKeepsADatabaseScopedPropertyAtDatabaseScope(t *testing.T) {
	c := qt.New(t)

	db := sqlServerObjects()
	db.ExtendedProperties = append(db.ExtendedProperties, schemamodel.ExtendedProperty{
		Name: "ptah_flag", Value: "database scope",
	})

	result, err := atlashclrender.RenderInspected(db, platform.SQLServer, "dbo")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains,
		"extended_property \"ptah_flag\" {\n  value = \"database scope\"\n}")
}

// sqlServerObjects is one table with a synonym and a column property on it.
func sqlServerObjects() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "dbo"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "users", Schema: "dbo"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "title", Type: "NVARCHAR(50)"},
		},
		Synonyms: []schemamodel.Synonym{{
			Name: "s_users", Schema: "dbo", Target: "other.dbo.users",
		}},
		ExtendedProperties: []schemamodel.ExtendedProperty{{
			Name: "MS_Description", Schema: "dbo", Table: "users",
			Column: "title", Value: "the title",
		}},
	}
}
