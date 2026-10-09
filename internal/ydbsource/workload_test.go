package ydbsource_test

import (
	"fmt"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
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

// A Go source's secret limit belongs to the secret owner's coverage: the named
// secret is unmanaged, and the rest of the namespace is still described.
func TestGoSourceSecretLimitsBelongToFeatureCoverage(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseSource("limits.go", `package entities
//ptah:schema:notdescribed kind="secret" name="ext/pg.pw"
type Unmanaged struct{}
`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.NotDescribed.IsZero(), qt.IsTrue)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("ext", "pg.pw")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "ext.pg.pw")).State, qt.Equals, schemaext.Complete)
}

// TestGoSecretLimitsReadThePath reads a Go source's secret limit as the
// secret's path: a dot stays in its segment and only a slash separates a
// directory.
func TestGoSecretLimitsReadThePath(t *testing.T) {
	tests := []struct {
		name      string
		limit     string
		unmanaged objectidentity.ID
		described objectidentity.ID
	}{
		{name: "a dotted root name", limit: "pg.pw", unmanaged: ydbsecret.Ref("", "pg.pw"), described: ydbsecret.Ref("pg", "pw")},
		{name: "a directory", limit: "pg/pw", unmanaged: ydbsecret.Ref("pg", "pw"), described: ydbsecret.Ref("", "pg.pw")},
		{name: "a leading slash", limit: "/pg.pw", unmanaged: ydbsecret.Ref("", "pg.pw"), described: ydbsecret.Ref("pg", "pw")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("limits.go", fmt.Sprintf("package entities\n//ptah:schema:notdescribed kind=%q name=%q\ntype Unmanaged struct{}\n", "secret", test.limit))
			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, test.unmanaged).State, qt.Equals, schemaext.Uninspected)
			c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, test.described).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestYQLSecretLimitsReadThePath reads a YQL header's secret limit as the
// secret's path, as the Go annotation reads it.
func TestYQLSecretLimitsReadThePath(t *testing.T) {
	c := qt.New(t)
	db, _, err := sqlschema.Read([]byte("-- ptah:not-described secret \"pg.pw\"\nCREATE SECRET `other` WITH (value = $PTAH_SECRET_OTHER);\n"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "pg.pw")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("pg", "pw")).State, qt.Equals, schemaext.Complete)
}

func TestSQLHeaderWorkloadLimitsRemainScoped(t *testing.T) {
	c := qt.New(t)
	db, _, err := sqlschema.Read([]byte(`-- ptah:not-described resource_pool "batch.jobs"
-- ptah:not-described resource_pool_classifier
CREATE RESOURCE POOL other WITH (CONCURRENT_QUERY_LIMIT = 0);
`), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(db.NotDescribed.IsZero(), qt.IsTrue)
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

// Omitting a declaration cannot stand in for an explicit absence or default
// assertion. Workload omission preserves current global objects.
func TestGoExportRefusesUnrepresentableWorkloadIntent(t *testing.T) {
	for _, family := range []struct {
		kind schemaext.Kind
		ref  objectidentity.ID
	}{
		{ydbworkload.PoolKind, ydbworkload.PoolRef("batch")},
		{ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("route")},
	} {
		for _, state := range []schemaext.KnowledgeState{schemaext.Absent, schemaext.Defaulted} {
			t.Run(string(family.kind)+"/"+string(state), func(t *testing.T) {
				c := qt.New(t)
				known := must.Must(ydbworkload.Coverage(family.kind, schemaext.Desired,
					schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{
						Kind: family.kind, Subject: family.ref, Knowledge: schemaext.Knowledge{State: state},
					}}))
				files, err := goschematogo.Render(t.Context(), &schemamodel.Database{FeatureCoverage: known}, goschematogo.Options{SingleFile: true, Dialect: "ydb"})
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(files, qt.IsNil)
			})
		}
	}
}

// Export cannot grant a source authority over a namespace it did not inspect.
// A complete neighboring namespace is a control against marking every family
// unknown, while the per-table layout checks the declaration holder is emitted.
func TestGoExportPreservesUnenrolledNamespaces(t *testing.T) {
	for _, layout := range []struct {
		name   string
		single bool
	}{{"single file", true}, {"per table", false}} {
		for _, test := range []struct {
			name     string
			coverage schemaext.Coverage
			pools    schemaext.KnowledgeState
			tables   []schemamodel.Table
			fields   []schemamodel.Field
		}{
			{name: "no enrollment", pools: schemaext.Uninspected},
			{name: "only pools enrolled", coverage: must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Desired,
				schemaext.Knowledge{State: schemaext.Complete}, nil)), pools: schemaext.Complete},
			{name: "table alongside coverage", pools: schemaext.Uninspected,
				tables: []schemamodel.Table{{Name: "records", StructName: "Records"}},
				fields: []schemamodel.Field{{StructName: "Records", Name: "id", FieldName: "ID", Type: "Int64"}}},
		} {
			t.Run(layout.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				files, err := goschematogo.Render(t.Context(), &schemamodel.Database{FeatureCoverage: test.coverage, Tables: test.tables, Fields: test.fields},
					goschematogo.Options{SingleFile: layout.single, Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				source := fstest.MapFS{}
				for _, file := range files {
					source[file.Name] = &fstest.MapFile{Data: file.Data}
				}
				parsed, err := goschema.ParseFS(source, ".")
				c.Assert(err, qt.IsNil)
				c.Assert(parsed.FeatureObjects.Len(), qt.Equals, 0)
				c.Assert(parsed.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("missing")).State, qt.Equals, test.pools)
				for _, ref := range []objectidentity.ID{ydbworkload.ClassifierRef("missing"), ydbcoordination.Ref("", "missing"), ydbstreaming.Ref("", "missing"), ydbsecret.Ref("", "missing")} {
					c.Assert(parsed.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref).State, qt.Equals, schemaext.Uninspected)
				}
				again, err := goschematogo.Render(t.Context(), parsed, goschematogo.Options{SingleFile: layout.single, Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				c.Assert(again, qt.DeepEquals, files)
			})
		}
	}
}

func TestGoExportPreservesAuthoredSubjectLimitsInEveryFile(t *testing.T) {
	for _, single := range []bool{true, false} {
		t.Run(fmt.Sprintf("single=%t", single), func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("source.go", `package entities
//ptah:schema:notdescribed kind="coordination_node" name="/locks.v1"
//ptah:schema:notdescribed kind="streaming_query" name="app.v1/copy"
//ptah:schema:notdescribed kind="resource_pool" name="Batch.jobs"
//ptah:schema:notdescribed kind="resource_pool_classifier" name="route.jobs"
//ptah:schema:notdescribed kind="secret" name="ext/pg.pw"
//ptah:schema:notdescribed kind="secret" name="pg.pw"
type Limits struct{}
`)
			c.Assert(err, qt.IsNil)
			db.Enums = []schemamodel.Enum{{Name: "mood", Values: []string{"ok"}}}
			db.Tables = []schemamodel.Table{{Name: "records", StructName: "Records"}}
			db.Fields = []schemamodel.Field{{StructName: "Records", Name: "id", FieldName: "ID", Type: "Int64"}}
			files, err := goschematogo.Render(t.Context(), &db, goschematogo.Options{SingleFile: single, Dialect: "ydb"})
			c.Assert(err, qt.IsNil)
			c.Assert(len(files) > 0, qt.IsTrue)
			for _, file := range files {
				parsed, err := goschema.ParseSource(file.Name, file.Data)
				c.Assert(err, qt.IsNil)
				c.Assert(parsed.FeatureCoverage.Equal(db.FeatureCoverage), qt.IsTrue, qt.Commentf("file %s", file.Name))
			}
		})
	}
}

func TestGoExportPartialFilesKeepUnknownNamespaces(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Enums:  []schemamodel.Enum{{Name: "mood", Values: []string{"ok"}}},
		Tables: []schemamodel.Table{{Name: "records", StructName: "Records"}},
		Fields: []schemamodel.Field{{StructName: "Records", Name: "id", FieldName: "ID", Type: "Int64"}},
	}
	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{PerTable: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 3)
	for _, file := range files {
		parsed, err := goschema.ParseSource(file.Name, file.Data)
		c.Assert(err, qt.IsNil)
		for _, kind := range []schemaext.Kind{ydbcoordination.Kind, ydbstreaming.Kind, ydbworkload.PoolKind, ydbworkload.ClassifierKind} {
			c.Assert(parsed.FeatureCoverage.Lookup(kind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected, qt.Commentf("%s in %s", kind, file.Name))
		}
	}
}

func secretExportSource(c *qt.C, knowledge schemaext.Knowledge) *schemamodel.Database {
	c.Helper()
	known, err := ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbsecret.Kind, Subject: ydbsecret.Ref("ext", "pg.pw"), Knowledge: knowledge}})
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureCoverage: known}
}

// TestGoExport_CarriesASecretAReadLeftUnmanaged writes a secret that a read
// of a line without the secrets capability left unmanaged as a limit on that
// secret, so the exported source leaves it unmanaged as well.
func TestGoExport_CarriesASecretAReadLeftUnmanaged(t *testing.T) {
	c := qt.New(t)
	db := secretExportSource(c, schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbsecret.UnsupportedReason})

	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Contains, `//ptah:schema:notdescribed kind="secret" name="ext/pg.pw"`)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("ext", "pg.pw")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("ext", "other")).State, qt.Equals, schemaext.Complete)
}

// TestGoExport_RefusesASecretLeftUnknownForAnotherReason refuses to write a
// secret whose record says something a limit cannot: a directive would turn a
// read failure into an authored decision.
func TestGoExport_RefusesASecretLeftUnknownForAnotherReason(t *testing.T) {
	c := qt.New(t)
	db := secretExportSource(c, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the read failed"})

	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})

	c.Assert(err, qt.ErrorMatches, `.*secrets object .* cannot be exported without losing its coverage record.*`)
	c.Assert(files, qt.IsNil)
}
