package schemadiff_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// TestCompare_ARoundTripThroughPtahsOwnOutputKeepsThem is the regression for a
// round trip through Ptah's own output.
//
// The loop is the one an operator runs: `schema inspect > out.hcl`, then
// `schema apply --to file://out.hcl`. HCL had no block for a SQL Server synonym
// and none for an extended property, so the rendered document carried neither,
// and reading its silence as intent planned `DROP SYNONYM` and
// `sp_dropextendedproperty` for every one of them -- against the database the
// document was written from (stokaro/ptah#1031). It is the defect
// stokaro/ptah#1276 closed for extensions, sequences and policies, on the two
// kinds that fix did not reach.
//
// The document now NAMES them, which is the stronger of the two answers: a
// coverage record protects an object from removal but still loses it on the way
// out, so an operator who edited the file and applied it had no way to keep
// what the render dropped.
//
// The whole loop is exercised rather than the comparator alone, because a test
// that handed the comparator a hand-built schema would pass on a render that
// emits nothing at all.
func TestCompare_ARoundTripThroughPtahsOwnOutputKeepsThem(t *testing.T) {
	c := qt.New(t)
	live := sqlServerDatabaseWithUnwritableObjects()

	document := renderInspectedDocument(c, describedSQLServerSchema())
	parsed := loadDocument(c, document)

	diff := schemadiff.Compare(parsed, live)

	c.Assert(diff.ExtendedPropertiesRemoved, qt.HasLen, 0)
	c.Assert(diff.SynonymsRemoved, qt.HasLen, 0)
	// Non-vacuity: the two empty lists above are the objects surviving rather
	// than a comparison of two empty sets.
	c.Assert(parsed.Synonyms, qt.DeepEquals, describedSQLServerSchema().Synonyms)
	c.Assert(parsed.ExtendedProperties, qt.DeepEquals, describedSQLServerSchema().ExtendedProperties)
	c.Assert(string(document), qt.Contains, "s_users")
}

// TestCompare_AnHCLDocumentThatOmitsThemNowDropsThem is the control the
// blocks cost, and the direction the coverage record used to make impossible.
//
// A format that CAN name an object is a format whose silence about one is
// intent, so the moment HCL gained the two blocks it had to start planning the
// removals again. Without this, a fix that kept the loader's old record
// alongside the new blocks would pass every test above while leaving both kinds
// permanently undroppable from the format Ptah itself writes.
func TestCompare_AnHCLDocumentThatOmitsThemNowDropsThem(t *testing.T) {
	c := qt.New(t)
	live := sqlServerDatabaseWithUnwritableObjects()

	// The same schema with both declarations taken out, rendered and read back:
	// a document whose author does not want them.
	declared := describedSQLServerSchema()
	declared.ExtendedProperties = nil
	declared.Synonyms = nil
	parsed := loadDocument(c, renderInspectedDocument(c, declared))

	diff := schemadiff.Compare(parsed, live)

	c.Assert(diff.ExtendedPropertiesRemoved, qt.HasLen, 1)
	c.Assert(diff.SynonymsRemoved.Names(), qt.DeepEquals, []string{"dbo.s_users"})
}

// TestCompare_AGoSchemaThatCouldNameThemStillDropsThem is the control.
//
// Go annotations DO have `//ptah:schema:synonym` and
// `//ptah:schema:extendedproperty`, so a Go schema that omits one is asking for
// it to go. A fix that suppressed the removal everywhere would pass the test
// above and take the capability away.
func TestCompare_AGoSchemaThatCouldNameThemStillDropsThem(t *testing.T) {
	c := qt.New(t)
	live := sqlServerDatabaseWithUnwritableObjects()

	// The same schema with both declarations taken out, which is what a Go
	// source that does not want them looks like.
	declared := describedSQLServerSchema()
	declared.ExtendedProperties = nil
	declared.Synonyms = nil

	diff := schemadiff.Compare(declared, live)

	c.Assert(diff.ExtendedPropertiesRemoved, qt.HasLen, 1)
	c.Assert(diff.SynonymsRemoved.Names(), qt.DeepEquals, []string{"dbo.s_users"})
}

// sqlServerDatabaseWithUnwritableObjects is a database holding one of each of
// the two kinds only HCL and a Go schema can name.
func sqlServerDatabaseWithUnwritableObjects() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "dbo"}},
		Tables:  []catalog.Table{{Schema: "dbo", Name: "users"}},
		ExtendedProperties: []catalog.ExtendedProperty{{
			Name: "MS_Description", Value: "the users",
			Schema: "dbo", Table: "users", ValueType: "nvarchar",
		}},
		Synonyms: []catalog.Synonym{{
			Schema: "dbo", Name: "s_users", Target: "other.dbo.users",
		}},
	}
}

// describedSQLServerSchema is what a read of that database produces: the table,
// and both objects the HCL render has to carry.
func describedSQLServerSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "dbo"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "users", Schema: "dbo"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
		ExtendedProperties: []schemamodel.ExtendedProperty{{
			Name: "MS_Description", Value: "the users", Schema: "dbo", Table: "users",
		}},
		Synonyms: []schemamodel.Synonym{{
			Name: "s_users", Schema: "dbo", Target: "other.dbo.users",
		}},
	}
}

func renderInspectedDocument(c *qt.C, db *schemamodel.Database) []byte {
	c.Helper()
	result, err := atlashclrender.RenderInspected(db, platform.SQLServer, "dbo")
	c.Assert(err, qt.IsNil)
	return result.Data
}

func loadDocument(c *qt.C, document []byte) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "out.hcl")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	parsed, err := schemafile.Load(path, schemafile.Options{Dialect: platform.SQLServer})
	c.Assert(err, qt.IsNil)
	return parsed
}
