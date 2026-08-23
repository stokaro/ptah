package schemadiff_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompare_ADocumentThatCouldNotNameThemDoesNotDropThem is the regression
// for a round trip through Ptah's own output.
//
// The loop is the one an operator runs: `schema inspect > out.hcl`, then
// `schema apply --to file://out.hcl`. HCL has no block for a SQL Server synonym
// and none for an extended property, so the rendered document carries neither,
// and reading its silence as intent planned `DROP SYNONYM` and
// `sp_dropextendedproperty` for every one of them -- against the database the
// document was written from (stokaro/ptah#1031). It is the defect
// stokaro/ptah#1276 closed for extensions, sequences and policies, on the two
// kinds that fix did not reach.
//
// The whole loop is exercised rather than the comparator alone, because the
// record is made by the LOADER rather than by the render: the limit belongs to
// the format, not to this document, and a `ptah:not-described` header nobody
// wrote would be re-emitted as a claim the author never made. So a test that
// handed the comparator a hand-built Coverage would pass while every real
// caller still dropped the objects.
func TestCompare_ADocumentThatCouldNotNameThemDoesNotDropThem(t *testing.T) {
	c := qt.New(t)
	live := sqlServerDatabaseWithUnwritableObjects()

	document := renderInspectedDocument(c, describedSQLServerSchema())
	parsed := loadDocument(c, document)

	diff := schemadiff.Compare(parsed, live)

	c.Assert(diff.ExtendedPropertiesRemoved, qt.HasLen, 0)
	c.Assert(diff.SynonymsRemoved, qt.HasLen, 0)
	// Non-vacuity: the document really does carry neither object, so the two
	// empty lists above are a decision rather than an accident of a render that
	// happened to emit them.
	c.Assert(parsed.ExtendedProperties, qt.HasLen, 0)
	c.Assert(parsed.Synonyms, qt.HasLen, 0)
	c.Assert(string(document), qt.Not(qt.Contains), "s_users")
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
	c.Assert(diff.SynonymsRemoved, qt.DeepEquals, []string{"dbo.s_users"})
}

// sqlServerDatabaseWithUnwritableObjects is a database holding one of each of
// the two kinds no document format can name.
func sqlServerDatabaseWithUnwritableObjects() *dbtypes.DBSchema {
	return &dbtypes.DBSchema{
		Schemas: []dbtypes.DBSchemaInfo{{Name: "dbo"}},
		Tables:  []dbtypes.DBTable{{Schema: "dbo", Name: "users"}},
		ExtendedProperties: []dbtypes.DBExtendedProperty{{
			Name: "MS_Description", Value: "the users",
			Schema: "dbo", Table: "users", ValueType: "nvarchar",
		}},
		Synonyms: []dbtypes.DBSynonym{{
			Schema: "dbo", Name: "s_users", Target: "other.dbo.users",
		}},
	}
}

// describedSQLServerSchema is what a read of that database produces: the table,
// and both objects the HCL render is about to drop on the floor.
func describedSQLServerSchema() *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: "dbo"}},
		Tables:  []goschema.Table{{StructName: "T", Name: "users", Schema: "dbo"}},
		Fields:  []goschema.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
		ExtendedProperties: []goschema.ExtendedProperty{{
			Name: "MS_Description", Value: "the users", Schema: "dbo", Table: "users",
		}},
		Synonyms: []goschema.Synonym{{
			Name: "s_users", Schema: "dbo", Target: "other.dbo.users",
		}},
	}
}

func renderInspectedDocument(c *qt.C, db *goschema.Database) []byte {
	c.Helper()
	result, err := atlashclrender.RenderInspected(db, platform.SQLServer, "dbo")
	c.Assert(err, qt.IsNil)
	return result.Data
}

func loadDocument(c *qt.C, document []byte) *goschema.Database {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "out.hcl")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	parsed, err := schemafile.Load(path, schemafile.Options{Dialect: platform.SQLServer})
	c.Assert(err, qt.IsNil)
	return parsed
}
