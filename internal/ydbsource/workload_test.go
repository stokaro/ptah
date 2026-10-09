package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsource"
)

func TestWorkloadLimitsKeepDatabaseScopeAndExactNames(t *testing.T) {
	c := qt.New(t)
	known, err := ydbsource.Coverage(ydbsource.Limits{
		Pools: []string{"Batch.jobs", "Batch.jobs"}, Classifiers: []string{"route.jobs"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(known.SubjectRecords(), qt.HasLen, 2)
	for _, ref := range []objectidentity.ID{ydbworkload.PoolRef("Batch.jobs"), ydbworkload.ClassifierRef("route.jobs")} {
		c.Assert(known.Lookup(schemaext.Kind(ref.Kind), ref).State, qt.Equals, schemaext.Uninspected)
	}
	for _, ref := range []objectidentity.ID{ydbworkload.PoolRef("batch.jobs"), ydbworkload.ClassifierRef("Batch.jobs"), ydbworkload.PoolRef("route.jobs")} {
		c.Assert(known.Lookup(schemaext.Kind(ref.Kind), ref).State, qt.Equals, schemaext.Complete)
	}
}

func TestGoSourceWorkloadLimitsBelongToFeatureCoverage(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseSource("limits.go", `package entities
//ptah:schema:notdescribed kind="resource_pool" name="batch.jobs"
//ptah:schema:notdescribed kind="resource_pool_classifier"
type Unmanaged struct{}
`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.NotDescribed.IsZero(), qt.IsTrue)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("batch.jobs")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("other")).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("other")).State, qt.Equals, schemaext.Uninspected)
}

func TestSQLHeaderWorkloadLimitsRemainScoped(t *testing.T) {
	c := qt.New(t)
	db, _, err := sqlschema.Read([]byte(`-- ptah:not-described resource_pool "batch.jobs"
-- ptah:not-described resource_pool_classifier
CREATE RESOURCE POOL other WITH (CONCURRENT_QUERY_LIMIT = 0);
`), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("batch.jobs")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("other")).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("other")).State, qt.Equals, schemaext.Uninspected)
}

func TestGoExportRefusesWorkloadInspectionLoss(t *testing.T) {
	for _, kind := range []schemaext.Kind{ydbworkload.PoolKind, ydbworkload.ClassifierKind} {
		for _, test := range []struct {
			name      string
			namespace schemaext.Knowledge
			subjects  []schemaext.SubjectCoverage
		}{
			{name: "namespace", namespace: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "permission denied"}},
			{name: "subject", namespace: schemaext.Knowledge{State: schemaext.Complete}, subjects: []schemaext.SubjectCoverage{{
				Kind: kind, Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(kind), "", "broken"),
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unsupported server setting"},
			}}},
		} {
			t.Run(string(kind)+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				db := &schemamodel.Database{FeatureCoverage: must.Must(ydbworkload.Coverage(kind, schemaext.Desired, test.namespace, test.subjects))}
				files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(files, qt.IsNil)
			})
		}
	}
}
