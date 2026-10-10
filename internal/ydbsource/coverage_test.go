package ydbsource_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsource"
)

// Go, YAML, and YQL enroll their supported vocabulary even when empty. HCL
// requires an explicit feature account and does not infer one from the runtime.
func TestEmptySourcesRecordSupportedFeatureNamespaces(t *testing.T) {
	tests := []struct {
		name         string
		parse        func() (*schemamodel.Database, error)
		changefeeds  schemaext.KnowledgeState
		coordination schemaext.KnowledgeState
	}{
		{name: "Go source", parse: func() (*schemamodel.Database, error) {
			db, err := goschema.ParseSource(builtintest.Annotations(), "empty.go", "package entities")
			return &db, err
		}, changefeeds: schemaext.Complete, coordination: schemaext.Complete},
		{name: "empty Go directory", parse: func() (*schemamodel.Database, error) {
			return goschema.ParseFS(builtintest.Annotations(), fstest.MapFS{}, ".")
		}, changefeeds: schemaext.Complete, coordination: schemaext.Complete},
		{name: "YAML", parse: func() (*schemamodel.Database, error) { return yamlschema.Parse([]byte("{}")) }, changefeeds: schemaext.Complete, coordination: schemaext.Complete},
		{name: "YQL", parse: func() (*schemamodel.Database, error) { db, _, err := sqlschema.Read(nil, "ydb"); return &db, err }, changefeeds: schemaext.Complete, coordination: schemaext.Complete},
		{name: "HCL", parse: func() (*schemamodel.Database, error) { return atlashcl.Parse(nil, "empty.hcl") }, changefeeds: schemaext.Uninspected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := test.parse()
			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(db.FeatureCoverage.Representation(), qt.Equals, schemaext.Desired)
			c.Assert(db.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("app", "absent")).State, qt.Equals, test.coordination)
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

// Go export refuses inspection details its annotations cannot preserve. HCL's
// versioned header keeps the exact account, including the reason for a limit.
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
			c.Assert(hclErr, qt.IsNil)
			c.Assert(files, qt.IsNil)
			parsed, err := atlashcl.Parse(hcl.Data, "export.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.FeatureCoverage.Equal(withHCLTimescaleCoverage(c, db.FeatureCoverage)), qt.IsTrue)
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

// A known kind spelling does not make another owner's model or a newer
// definition understood. Export must not silently enroll its own definition.
func TestGoExportRefusesDifferentSourceModels(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	for _, known := range []schemaext.Coverage{
		must.Must(ydbcoordination.Coverage(schemaext.Desired, complete, nil)),
		must.Must(ydbstreaming.Coverage(schemaext.Desired, complete, nil)),
		must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Desired, complete, nil)),
		must.Must(ydbworkload.Coverage(ydbworkload.ClassifierKind, schemaext.Desired, complete, nil)),
	} {
		original := known.KindRecords()[0].Model
		owner, version, definition, observed := original, original, original, original
		owner.Owner = "example.test/other"
		version.Version++
		definition.Definition += "/changed"
		observed.Representation = schemaext.Observed
		for _, test := range []struct {
			name  string
			model schemaext.CodecIdentity
		}{{"owner", owner}, {"version", version}, {"definition", definition}, {"representation", observed}} {
			t.Run(string(original.Kind)+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				captured := must.Must(schemaext.NewCoverage(test.model.Representation,
					[]schemaext.KindCoverage{{Model: test.model, Knowledge: complete}}, nil))
				files, err := goschematogo.Render(t.Context(), &schemamodel.Database{FeatureCoverage: captured},
					goschematogo.Options{SingleFile: true, Dialect: "ydb"})
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(files, qt.IsNil)
			})
		}
	}
}

func TestHCLExportRefusesDifferentSourceModel(t *testing.T) {
	c := qt.New(t)
	known := must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	records := known.KindRecords()
	records[0].Model.Version++
	captured := must.Must(schemaext.NewCoverage(schemaext.Desired, records, nil))
	result, err := atlashclrender.RenderForDialect(&schemamodel.Database{FeatureCoverage: captured}, "ydb")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, atlashclrender.Result{})
}

func TestSQLSourceRefusesForeignCoverageInsteadOfReplacingIt(t *testing.T) {
	known := must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	header := must.Must(ydbsource.HCLCoordinationDirectives(known))[0]
	for _, dialect := range []string{"ydb", "sqlite"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			db, statements, err := sqlschema.Read([]byte("-- "+header+"\n"), dialect)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
			c.Assert(db.FeatureCoverage.IsZero(), qt.IsTrue)
		})
	}
}

// Go annotations cannot express a positive override inside an unmanaged
// namespace. HCL carries that distinction in its captured model account.
func TestExportsPreserveOrRefuseCompleteSubjectsInUnmanagedNamespaces(t *testing.T) {
	known := must.Must(ydbsource.Coverage(ydbsource.Limits{
		Coordination: []string{""}, Streaming: []string{""}, Pools: []string{""}, Classifiers: []string{""},
	}))
	for _, ref := range []objectidentity.ID{
		ydbcoordination.Ref("", "locks"), ydbstreaming.Ref("", "copy"),
		ydbworkload.PoolRef("batch"), ydbworkload.ClassifierRef("route"),
	} {
		t.Run(string(ref.Kind), func(t *testing.T) {
			c := qt.New(t)
			captured := must.Must(schemaext.NewCoverage(schemaext.Desired, known.KindRecords(), []schemaext.SubjectCoverage{{
				Kind: schemaext.Kind(ref.Kind), Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Complete},
			}}))
			files, err := goschematogo.Render(t.Context(), &schemamodel.Database{FeatureCoverage: captured},
				goschematogo.Options{SingleFile: true, Dialect: "ydb"})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(files, qt.IsNil)
		})
	}
	c := qt.New(t)
	withNode := must.Must(schemaext.NewCoverage(schemaext.Desired, known.KindRecords(), []schemaext.SubjectCoverage{{
		Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("", "locks"), Knowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}))
	result, err := atlashclrender.RenderForDialect(&schemamodel.Database{FeatureCoverage: withNode}, "ydb")
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(result.Data, "export.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureCoverage.Equal(withHCLTimescaleCoverage(c, withNode.SelectKinds([]schemaext.Kind{ydbcoordination.Kind}))), qt.IsTrue)
}

// withHCLTimescaleCoverage adds the claim every HCL document makes by its
// format: it has a block for each TimescaleDB model, so a document without one
// describes a database without one. The YDB account must survive beside it
// unchanged.
func withHCLTimescaleCoverage(c *qt.C, known schemaext.Coverage) schemaext.Coverage {
	c.Helper()
	combined, err := known.Combine(must.Must(tsschema.CompleteCoverage(schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	// An HCL document also describes PostgreSQL row-level security.
	combined, err = combined.Combine(must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	return combined
}
