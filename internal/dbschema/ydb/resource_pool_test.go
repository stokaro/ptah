package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/migration/schemadiff"
)

// poolSource is a database holding one table, the pool default, a pool of its
// own and a classifier, as .sys reports them.
func poolSource() fakeSource {
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": plainTable()},
		pools: ydbschema.ResourcePools{
			Database: "/local",
			Objects: []schemaext.Object{
				ydbworkload.ObservedPoolObject("default", ydbworkload.PoolSpec{}),
				ydbworkload.ObservedPoolObject("batch", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(10))}),
				ydbworkload.ObservedClassifierObject("etl_users", ydbworkload.ClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}),
			},
		},
	}
}

// On a cluster whose EnableResourcePools flag is on, a read of the database
// describes every pool, default included, and every classifier, whatever
// directories it was scoped to: they belong to the database.
func TestReadSchema_ResourcePools_HappyPath(t *testing.T) {
	for _, schemas := range [][]string{nil, {""}} {
		t.Run(fmt.Sprintf("schemas %q", schemas), func(t *testing.T) {
			c := qt.New(t)
			reader := ydbschema.NewReaderFromSource(poolSource(), "/local",
				capability.YDB262().With(capability.ResourcePools, true))
			reader.SetSchemas(schemas)

			db, err := reader.ReadSchemaContext(context.Background())

			c.Assert(err, qt.IsNil)
			objects, err := db.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.ContentEquals, poolSource().pools.Objects)
			c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("missing")).State, qt.Equals, schemaext.Complete)
			c.Assert(db.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("missing")).State, qt.Equals, schemaext.Complete)
		})
	}
}

// Listing a pool cannot establish its settings. A successful system-view row
// is the control: the later listing must preserve that complete observation.
func TestReadSchemaRetainsPoolsMissingFromSystemViews(t *testing.T) {
	for _, test := range []struct {
		name      string
		objects   []schemaext.Object
		readErr   error
		want      schemaext.KnowledgeState
		namespace schemaext.KnowledgeState
	}{
		{name: "described pool", objects: poolSource().pools.Objects, want: schemaext.Complete, namespace: schemaext.Complete},
		{name: "listed but missing row", want: schemaext.Unrepresentable, namespace: schemaext.Complete},
		{name: "refused system view", readErr: ydbschema.ErrResourcePoolsRefused, want: schemaext.Unrepresentable, namespace: schemaext.Uninspected},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := poolSource()
			source.directories["/local"] = append(source.directories["/local"], entry("batch", Ydb_Scheme.Entry_RESOURCE_POOL))
			source.pools.Objects, source.poolsErr = test.objects, test.readErr
			reader := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.ResourcePools, true))
			reader.SetSchemas([]string{"app"})
			db, err := reader.ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("batch")).State, qt.Equals, test.want)
			c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("absent")).State, qt.Equals, test.namespace)
		})
	}
}

// Unavailable enumeration never means absence, including an empty view on a
// disabled cluster. Positive observations remain available independently of
// whether the target currently permits workload DDL.
func TestReadSchema_ResourcePools_Recorded(t *testing.T) {
	refused := poolSource()
	refused.poolsErr = fmt.Errorf("%w: ABORTED: AccessDenied", ydbschema.ErrResourcePoolsRefused)
	empty := poolSource()
	empty.pools.Objects = nil
	for _, test := range []struct {
		name   string
		source fakeSource
		root   string
		caps   capability.Capabilities
		want   []schemaext.Object
	}{
		{name: "disabled with positive observations", source: poolSource(), root: "/local", caps: capability.YDB251(), want: poolSource().pools.Objects},
		{name: "disabled with empty views", source: empty, root: "/local", caps: capability.YDB251(), want: make([]schemaext.Object, 0)},
		{name: "a dev realm", source: realmSource(), root: "/local/ptah_dev/r1", caps: capability.YDB262().With(capability.ResourcePools, true), want: make([]schemaext.Object, 0)},
		{name: "an account that may not read .sys", source: refused, root: "/local", caps: capability.YDB262().With(capability.ResourcePools, true), want: make([]schemaext.Object, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := ydbschema.NewReaderFromSource(test.source, test.root, test.caps).ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			objects, err := db.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.ContentEquals, test.want)
			for _, ref := range []objectidentity.ID{ydbworkload.PoolRef("missing"), ydbworkload.ClassifierRef("missing")} {
				knowledge := db.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref)
				c.Assert(knowledge.State, qt.Equals, schemaext.Uninspected)
				c.Assert(knowledge.Reason, qt.Not(qt.Equals), "")
			}
			for _, object := range objects {
				c.Assert(db.FeatureCoverage.Lookup(object.Value.Kind(), object.Ref).State, qt.Equals, schemaext.Complete)
			}
		})
	}
}

func TestReadSchema_ResourcePools_UnsupportedSettingsStayUnknown(t *testing.T) {
	c := qt.New(t)
	source := poolSource()
	source.pools.Objects = append(source.pools.Objects,
		ydbworkload.ObservedPoolObject("broken", ydbworkload.PoolSpec{ResourceWeight: new(-2.0)}),
		ydbworkload.ObservedClassifierObject("broken", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: -1}),
	)
	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.ResourcePools, true)).ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 3)
	for _, ref := range []objectidentity.ID{ydbworkload.PoolRef("broken"), ydbworkload.ClassifierRef("broken")} {
		c.Assert(db.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref).State, qt.Equals, schemaext.Unrepresentable)
	}
}

// realmSource is poolSource read through a dev realm: the walk is of the
// realm's directory, and the pools are the database's.
func realmSource() fakeSource {
	source := poolSource()
	source.directories = map[string][]*Ydb_Scheme.Entry{"/local/ptah_dev/r1": {entry("t", Ydb_Scheme.Entry_TABLE)}}
	source.tables = map[string]*Ydb_Table.DescribeTableResult{"/local/ptah_dev/r1/t": plainTable()}
	return source
}

func TestReadRehearsalSchemaPreservesObservedWorkloadEnvironment(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			c := qt.New(t)
			reader := ydbschema.NewReaderFromSource(realmSource(), "/local/ptah_dev/r1", capability.YDB262().With(capability.ResourcePools, enabled))
			ordinary, err := reader.ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(ordinary.FeatureObjects.Len(), qt.Equals, 0)
			environment, err := reader.ReadRehearsalSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(environment.FeatureObjects.All()), qt.ContentEquals, poolSource().pools.Objects)
			c.Assert(environment.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("default")).State, qt.Equals, schemaext.Complete)
			c.Assert(environment.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("missing")).State, qt.Equals,
				map[bool]schemaext.KnowledgeState{true: schemaext.Complete, false: schemaext.Uninspected}[enabled])
			again, err := reader.ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(again.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(again.FeatureCoverage, qt.DeepEquals, ordinary.FeatureCoverage)
		})
	}
}

func TestRehearsalBaselineMatchesObservedDefaultPoolWithoutDDL(t *testing.T) {
	c := qt.New(t)
	targetSource := poolSource()
	targetSource.pools.Objects = targetSource.pools.Objects[:1]
	devSource := realmSource()
	devSource.pools.Objects = devSource.pools.Objects[:1]
	caps := capability.YDB262()
	target, err := ydbschema.NewReaderFromSource(targetSource, "/local", caps).ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	dev, err := ydbschema.NewReaderFromSource(devSource, "/local/ptah_dev/r1", caps).ReadRehearsalSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	runtime := must.Must(builtin.New())
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), target, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	ordinary, err := ydbschema.NewReaderFromSource(devSource, "/local/ptah_dev/r1", caps).ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	unresolved, err := schemadiff.CompareWithDialect(t.Context(), desired, ordinary, "ydb", runtime)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "the current workload object or its absence was not established")
	c.Assert(unresolved, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, dev, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestRehearsalReadPreservesUnknownAndRefusesUnrelatedEnvironment(t *testing.T) {
	c := qt.New(t)
	source := realmSource()
	source.poolsErr = ydbschema.ErrResourcePoolsRefused
	db, err := ydbschema.NewReaderFromSource(source, "/local/ptah_dev/r1", capability.YDB262()).ReadRehearsalSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(db.FeatureCoverage.Lookup(ydbworkload.PoolKind, ydbworkload.PoolRef("default")).State, qt.Equals, schemaext.Uninspected)
	source.poolsErr = nil
	source.pools.Database = "/local2"
	db, err = ydbschema.NewReaderFromSource(source, "/local/ptah_dev/r1", capability.YDB262()).ReadRehearsalSchemaContext(t.Context())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(db, qt.IsNil)
}

// Any failure other than a refusal fails the read, rather than describing a
// database without pools.
func TestReadSchema_ResourcePools_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := poolSource()
	source.poolsErr = errors.New("connection reset")

	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).
		ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `read the YDB resource pools: connection reset`)
	c.Assert(db, qt.IsNil)
}
