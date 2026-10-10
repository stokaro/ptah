package atlasreport_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsource"
)

func TestSplitPreservesOwnedYQLLimitsInEveryFile(t *testing.T) {
	const source = `-- ptah:not-described resource_pool reason=not-inspected provenance=observed "batch.jobs"
-- ptah:not-described resource_pool_classifier
CREATE TABLE events (id Int64 NOT NULL, PRIMARY KEY (id));
CREATE TABLE archive (id Int64 NOT NULL, PRIMARY KEY (id));
`
	for _, mode := range []string{"object", "schema", "type"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			output, err := atlasreport.RenderSchemaInspect(fmt.Sprintf(`{{ %q | split %q | write "out" }}`, source, mode), sampleSchemaInspectReport(c))
			c.Assert(err, qt.IsNil)
			c.Assert(len(output.Files) > 0, qt.IsTrue)
			for _, file := range output.Files {
				var limits ydbsource.Limits
				var owned []coverage.Object
				common, err := coverage.DecodeHeader(file.Data, func(record coverage.Object) (bool, error) {
					owned = append(owned, record)
					return limits.ConsumeDirective(record)
				})
				c.Assert(err, qt.IsNil)
				c.Assert(common.IsZero(), qt.IsTrue)
				c.Assert(owned, qt.Contains, coverage.Object{Kind: "resource_pool", Name: "batch.jobs", Reason: coverage.NotInspected, Provenance: coverage.Observed})
				db, _, err := sqlschema.Read([]byte(file.Data), "ydb")
				c.Assert(err, qt.IsNil)
				c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("batch.jobs")).State, qt.Equals, schemaext.Uninspected)
				c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("other")).State, qt.Equals, schemaext.Complete)
				c.Assert(db.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("other")).State, qt.Equals, schemaext.Uninspected)
			}
		})
	}
}

func TestSplitPreservesOwnedHCLLimitsInEveryFile(t *testing.T) {
	const source = `// ptah:not-described coordination_node "app/locks"
schema "app" {}
coordination_node "app" "other" {}
`
	for _, mode := range []string{"object", "schema", "type"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			output, err := atlasreport.RenderSchemaInspect(fmt.Sprintf(`{{ %q | split %q | write "out" }}`, source, mode), sampleSchemaInspectReport(c))
			c.Assert(err, qt.IsNil)
			c.Assert(len(output.Files) > 0, qt.IsTrue)
			for _, file := range output.Files {
				db, err := atlashcl.Parse([]byte(file.Data), file.Path)
				c.Assert(err, qt.IsNil)
				c.Assert(db.NotDescribed.IsZero(), qt.IsTrue)
				c.Assert(db.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks")).State, qt.Equals, schemaext.Uninspected)
				c.Assert(db.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "other")).State, qt.Equals, schemaext.Complete)
			}
		})
	}
}

func TestSplitPreservesExplicitHCLModelsInEveryFile(t *testing.T) {
	known := must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	for _, mode := range []string{"object", "schema", "type"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			source, err := atlashclrender.RenderForDialect(&schemamodel.Database{
				FeatureCoverage: known, Schemas: []schemamodel.Schema{{Name: "app"}},
				Tables: []schemamodel.Table{{Name: "events", Schema: "app", StructName: "Events"}},
				Fields: []schemamodel.Field{{StructName: "Events", Name: "id", Type: "Int64"}},
			}, "ydb")
			c.Assert(err, qt.IsNil)
			output, err := atlasreport.RenderSchemaInspect(fmt.Sprintf(`{{ %q | split %q | write "out" }}`, source.Data, mode), sampleSchemaInspectReport(c))
			c.Assert(err, qt.IsNil)
			c.Assert(len(output.Files) > 0, qt.IsTrue)
			for _, file := range output.Files {
				parsed, err := atlashcl.Parse([]byte(file.Data), file.Path)
				c.Assert(err, qt.IsNil)
				c.Assert(parsed.FeatureCoverage.Equal(withHCLBlockCoverage(c, known)), qt.IsTrue)
			}
		})
	}
}
