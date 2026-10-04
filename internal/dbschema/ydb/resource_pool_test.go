package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// poolSource is a database holding one table, the pool default, a pool of its
// own and a classifier, as .sys reports them.
func poolSource() fakeSource {
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": plainTable()},
		pools: ydbschema.ResourcePools{
			Database: "/local",
			Pools: []catalog.ResourcePool{
				{Name: "default"},
				{Name: "batch", Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(10))}},
			},
			Classifiers: []catalog.ResourcePoolClassifier{{Name: "etl_users",
				Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}}},
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
			c.Assert(db.ResourcePools, qt.DeepEquals, poolSource().pools.Pools)
			c.Assert(db.ResourcePoolClassifiers, qt.DeepEquals, poolSource().pools.Classifiers)
			c.Assert(db.NotDescribed.Describes(coverage.ResourcePool), qt.IsTrue)
		})
	}
}

// What a read records rather than describes: on a line whose flag is off,
// each pool and classifier by name, so a plan meets none it would refuse; on
// a dev realm, both kinds whole, since its pools are its database's; and on a
// database that refuses .sys to the account, both kinds as refused.
func TestReadSchema_ResourcePools_Recorded(t *testing.T) {
	observed := func(kind coverage.Kind, name string) coverage.Object {
		return coverage.Object{Kind: kind, Name: name, Reason: coverage.Unsupported, Provenance: coverage.Observed}
	}
	refused := poolSource()
	refused.poolsErr = fmt.Errorf("%w: ABORTED: AccessDenied", ydbschema.ErrResourcePoolsRefused)
	tests := []struct {
		name   string
		source fakeSource
		root   string
		caps   capability.Capabilities
		want   coverage.Set
	}{
		{
			name: "a line whose flag is off", source: poolSource(), root: "/local", caps: capability.YDB251(),
			want: coverage.Set{}.With(observed(coverage.ResourcePool, "default"), observed(coverage.ResourcePool, "batch"),
				observed(coverage.ResourcePoolClassifier, "etl_users")),
		},
		{
			name: "a dev realm", source: realmSource(), root: "/local/ptah_dev/r1",
			caps: capability.YDB262().With(capability.ResourcePools, true),
			want: coverage.Set{}.With(
				coverage.Object{Kind: coverage.ResourcePool, Reason: coverage.OutsideScope, Provenance: coverage.Observed},
				coverage.Object{Kind: coverage.ResourcePoolClassifier, Reason: coverage.OutsideScope,
					Provenance: coverage.Observed}),
		},
		{
			name: "an account that may not read .sys", source: refused, root: "/local",
			caps: capability.YDB262().With(capability.ResourcePools, true),
			want: coverage.Set{}.With(coverage.Refused(coverage.ResourcePool),
				coverage.Refused(coverage.ResourcePoolClassifier)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := ydbschema.NewReaderFromSource(test.source, test.root, test.caps).
				ReadSchemaContext(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(db.ResourcePools, qt.HasLen, 0)
			c.Assert(db.ResourcePoolClassifiers, qt.HasLen, 0)
			c.Assert(db.NotDescribed, qt.DeepEquals, test.want)
		})
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
