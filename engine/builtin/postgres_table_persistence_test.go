package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

func persistenceDocument(c *qt.C) *schemamodel.Database {
	c.Helper()
	runtime := must.Must(builtin.New())
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(`CREATE UNLOGGED TABLE app.cache (id bigint PRIMARY KEY); CREATE TABLE app.plain (id bigint PRIMARY KEY);`), "postgres", nil, sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
	c.Assert(err, qt.IsNil)
	return &database
}

func tablePersistence(c *qt.C, db *schemamodel.Database) map[string]bool {
	c.Helper()
	result := make(map[string]bool)
	for _, table := range db.Tables {
		value, _, err := pgtable.PersistenceOf(table.Facets)
		c.Assert(err, qt.IsNil)
		result[table.Schema+"."+table.Name] = value.Unlogged
	}
	return result
}

func TestPostgresTablePersistence_Conversion(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := persistenceDocument(c)
	current, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	captured, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(tablePersistence(c, captured), qt.DeepEquals, map[string]bool{"app.cache": true, "app.plain": false})
	diff, err := schemadiff.CompareWithDialect(t.Context(), captured, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}

func TestPostgresTablePersistence_GoExport(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := persistenceDocument(c)
	files, err := goschematogo.Render(t.Context(), desired, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime, SingleFile: true, Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `platform.postgres.unlogged="true"`)
	parsed, err := goschema.ParseSource(runtime.Annotations(), "schema.go", files[0].Data)
	c.Assert(err, qt.IsNil)
	decoded, err := schemaproperties.Decode(t.Context(), &parsed, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(tablePersistence(c, decoded), qt.DeepEquals, tablePersistence(c, desired))
}

func TestPostgresTablePersistence_HCLExport(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := persistenceDocument(c)
	rendered, err := atlashclrender.Render(desired, atlashclrender.Options{HCL: runtime.HCL(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	parsed, err := atlashcl.ParseWithOptions(t.Context(), rendered.Data, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(tablePersistence(c, parsed), qt.DeepEquals, tablePersistence(c, desired))
}

func TestPostgresTablePersistence_ASTOmissions(t *testing.T) {
	for _, dialect := range []string{"postgres", "yugabytedb", "mysql", "sqlite"} {
		for _, unlogged := range []bool{true, false} {
			c := qt.New(t)
			facets := must.Must(schemaext.NewFacets(&pgtable.DesiredPersistence{Unlogged: unlogged}))
			node := &ast.CreateTableNode{Name: "cache", Facets: facets, Columns: []*ast.ColumnNode{{Name: "id", Type: "bigint"}}}
			_, omissions, err := builtin.RenderSQLReportingOmissions(dialect, capability.ForDialect(dialect), &ast.StatementList{Statements: []ast.Node{node}})
			c.Assert(err, qt.IsNil)
			c.Assert(len(omissions) == 1, qt.Equals, dialect != "postgres" && unlogged)
			c.Assert(node.Facets.Equal(facets), qt.IsTrue)
		}
	}
}

func TestPostgresTablePersistence_RefusesExportOfUnknownTable(t *testing.T) {
	for _, unlogged := range []bool{true, false} {
		c := qt.New(t)
		runtime := must.Must(builtin.New())
		desired := persistenceDocument(c)
		desired.Tables[0].Facets = must.Must(pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: unlogged}))
		subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("app", "cache")
		desired.FeatureCoverage = must.Must(pgtable.PersistenceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: pgtable.PersistenceKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog persistence is unknown"}}}))
		files, err := goschematogo.Render(t.Context(), desired, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime, SingleFile: true, Dialect: "postgres"})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(files, qt.HasLen, 0)
		_, err = atlashclrender.Render(desired, atlashclrender.Options{HCL: runtime.HCL(), Dialect: "postgres"})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	}
}
