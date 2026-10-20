package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/platform/capability"
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

func partitionDocument(c *qt.C, target string) *schemamodel.Database {
	c.Helper()
	runtime := must.Must(builtin.New())
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(`CREATE TABLE app.words (word varchar(20), "a.b" integer) PARTITION BY RANGE (word COLLATE "C" text_pattern_ops, "a.b");`), target, nil, sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
	c.Assert(err, qt.IsNil)
	return &db
}
func partitionValue(c *qt.C, db *schemamodel.Database) *pgtable.Partition {
	c.Helper()
	c.Assert(db.Tables, qt.HasLen, 1)
	value, found, err := pgtable.PartitionOf(db.Tables[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return value
}
func TestPartitionOwner_GoExportRetainsTargetAndKey(t *testing.T) {
	for _, target := range []string{"postgres", "yugabytedb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := partitionDocument(c, target)
			files, err := goschematogo.Render(t.Context(), desired, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime, SingleFile: true, Dialect: target})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			c.Assert(string(files[0].Data), qt.Contains, "platform."+target+".partition_by=")
			parsed, err := goschema.ParseSource(runtime.Annotations(), "schema.go", files[0].Data)
			c.Assert(err, qt.IsNil)
			decoded, err := schemaproperties.Decode(t.Context(), &parsed, target, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(partitionValue(c, decoded), qt.DeepEquals, partitionValue(c, desired))
		})
	}
}
func TestPartitionOwner_HCLExportRetainsKeyOrderAndModifiers(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := partitionDocument(c, "postgres")
	rendered, err := atlashclrender.Render(desired, atlashclrender.Options{HCL: runtime.HCL(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	parsed, err := atlashcl.ParseWithOptions(t.Context(), rendered.Data, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(partitionValue(c, parsed), qt.DeepEquals, partitionValue(c, desired))
}
func TestPartitionOwner_ConversionRetainsCapturedKeys(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := partitionDocument(c, "postgres")
	current, err := goschematodb.ToDBSchema(t.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	captured, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(partitionValue(c, captured), qt.DeepEquals, partitionValue(c, desired))
	diff, err := schemadiff.CompareWithDialect(t.Context(), captured, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}
func TestPartitionOwner_NormalizationFactDoesNotLeakIntoExport(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := partitionDocument(c, "postgres")
	value, _, err := schemaext.FacetAs[*pgtable.DesiredPartition](desired.Tables[0].Facets, pgtable.PartitionKind)
	c.Assert(err, qt.IsNil)
	key := pgtable.PartitionComparisonKey(partitionValue(c, desired))
	value.Normalized = &pgtable.NormalizedPartition{SourceKey: key, Key: key}
	desired.Tables[0].Facets = must.Must(schemaext.NewFacets(value))
	files, err := goschematogo.Render(t.Context(), desired, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime, SingleFile: true, Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Not(qt.Contains), "source_key")
}

func TestPartitionOwner_SQLAbsenceRefusesPartitionRemoval(t *testing.T) {
	for _, target := range []string{"postgres", "yugabytedb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			declared := partitionDocument(c, target)
			current, err := goschematodb.ToDBSchema(t.Context(), declared, target, runtime)
			c.Assert(err, qt.IsNil)
			plain, _, err := sqlschema.ReadWith(t.Context(), []byte(`CREATE TABLE app.words (word varchar(20), "a.b" integer);`), target, nil, sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
			c.Assert(err, qt.IsNil)
			_, err = schemadiff.CompareWithDialect(t.Context(), &plain, current, target, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*partition key cannot change in place`)
		})
	}
}

func TestPartitionOwner_YugabyteOmitsUnloggedAndKeepsPartitioning(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateTable("words").AddColumn(ast.NewColumn("tenant", "integer"))
	node.Facets = must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "tenant"}}}, &pgtable.DesiredPersistence{Unlogged: true}))
	sql, omissions, err := builtin.RenderSQLReportingOmissions("yugabytedb", capability.ForDialect("yugabytedb"), node)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `PARTITION BY RANGE ("tenant")`)
	c.Assert(sql, qt.Not(qt.Contains), "UNLOGGED")
	c.Assert(omissions, qt.HasLen, 1)
	c.Assert(omissions[0].Property, qt.Equals, "unlogged")
}
