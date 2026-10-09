package ydbsource_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsource"
)

// Empty supported documents establish absence. Enrollment stays tied to each
// format's declared vocabulary, including the Go walk with no entity files.
func TestEmptySourcesRecordSupportedFeatureNamespaces(t *testing.T) {
	tests := []struct {
		name        string
		parse       func() (*schemamodel.Database, error)
		changefeeds schemaext.KnowledgeState
	}{
		{name: "Go source", parse: func() (*schemamodel.Database, error) {
			db, err := goschema.ParseSource("empty.go", "package entities")
			return &db, err
		}, changefeeds: schemaext.Complete},
		{name: "empty Go directory", parse: func() (*schemamodel.Database, error) { return goschema.ParseFS(fstest.MapFS{}, ".") }, changefeeds: schemaext.Complete},
		{name: "YAML", parse: func() (*schemamodel.Database, error) { return yamlschema.Parse([]byte("{}")) }, changefeeds: schemaext.Complete},
		{name: "YQL", parse: func() (*schemamodel.Database, error) { db, _, err := sqlschema.Read(nil, "ydb"); return &db, err }, changefeeds: schemaext.Complete},
		{name: "HCL", parse: func() (*schemamodel.Database, error) { return atlashcl.Parse(nil, "empty.hcl") }, changefeeds: schemaext.Uninspected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := test.parse()
			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(db.FeatureCoverage.Representation(), qt.Equals, schemaext.Desired)
			c.Assert(db.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "absent")).State, qt.Equals, schemaext.Complete)
			c.Assert(db.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, ydbschema.ChangefeedRef("app", "t", "absent")).State, qt.Equals, test.changefeeds)
			c.Assert(db.FeatureCoverage.Lookup(ydbstreaming.Kind, ydbstreaming.Ref("app", "absent")).State, qt.Equals, test.changefeeds)
			c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("absent")).State, qt.Equals, test.changefeeds)
			c.Assert(db.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("absent")).State, qt.Equals, test.changefeeds)
		})
	}
}

func TestCoordinationLimitsPreserveLiteralPathDots(t *testing.T) {
	c := qt.New(t)
	known, err := ydbsource.Coverage(ydbsource.Limits{Coordination: []string{"app/locks.v1", "app.v1/locks"}})
	c.Assert(err, qt.IsNil)
	c.Assert(known.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "locks.v1")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(known.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app.v1", "locks")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(known.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app/locks", "v1")).State, qt.Equals, schemaext.Complete)
	c.Assert(known.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app/v1", "locks")).State, qt.Equals, schemaext.Complete)
}

// Source export refuses incomplete inspection before writing an output prefix.
func TestExportsDoNotTurnCoordinationLimitsIntoAbsence(t *testing.T) {
	tests := []struct {
		name      string
		namespace schemaext.Knowledge
		subjects  []schemaext.SubjectCoverage
	}{
		{name: "namespace", namespace: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not enumerated"}},
		{name: "subject", namespace: schemaext.Knowledge{State: schemaext.Complete}, subjects: []schemaext.SubjectCoverage{{Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("app", "locks"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown server mode"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{FeatureCoverage: must.Must(ydbcoordination.Coverage(schemaext.Desired, test.namespace, test.subjects))}
			files, goErr := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})
			hcl, hclErr := atlashclrender.RenderForDialect(db, "ydb")
			c.Assert(goErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(hclErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(files, qt.IsNil)
			c.Assert(hcl, qt.DeepEquals, atlashclrender.Result{})
		})
	}
}

func TestGoExportDoesNotTurnStreamingLimitsIntoAbsence(t *testing.T) {
	for _, test := range []struct {
		name      string
		namespace schemaext.Knowledge
		subjects  []schemaext.SubjectCoverage
	}{
		{name: "namespace", namespace: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not enumerated"}},
		{name: "subject", namespace: schemaext.Knowledge{State: schemaext.Complete}, subjects: []schemaext.SubjectCoverage{{Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref("app", "copy"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "permission denied"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{FeatureCoverage: must.Must(ydbstreaming.Coverage(schemaext.Desired, test.namespace, test.subjects))}
			files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(files, qt.IsNil)
		})
	}
}
