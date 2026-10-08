//go:build integration

package ydb_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/ydbpool"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// poolSchemas is the directory the resource pool tests scope their reads to.
// A pool belongs to the database, so a read scoped to any directory describes
// every pool; scoping keeps the plan away from the tables other tests make.
var poolSchemas = []string{"ptah_ydb_pools"}

// poolNames are the pools and classifiers one run creates. They belong to the
// database rather than to a directory, so each run takes names of its own and
// removes them, whatever the run ended with. The ranks are drawn per run too,
// because YDB keeps one classifier per rank.
type poolNames struct {
	batch, idle, toBatch, toIdle, member string
	rank                                 int64
}

func newPoolNames(c *qt.C) poolNames {
	c.Helper()
	raw := make([]byte, 8)
	_, err := rand.Read(raw)
	c.Assert(err, qt.IsNil)
	suffix := hex.EncodeToString(raw[:4])
	return poolNames{
		batch: "ptahrpb" + suffix, idle: "ptahrpi" + suffix,
		toBatch: "ptahrcb" + suffix, toIdle: "ptahrci" + suffix,
		member: "ptahrpu" + suffix,
		rank:   1_000_000 + int64(binary.BigEndian.Uint32(raw[4:])),
	}
}

// removePools drops the run's classifiers and pools, and returns the pool
// default's weight to unset. A cleanup runs after the test's context is done,
// so it takes a context of its own, and a statement for an object the run did
// not get to create fails, which is fine.
func removePools(conn *dbschema.DatabaseConnection, names poolNames) {
	ctx := context.Background()
	for _, statement := range []string{
		ydbpool.DropClassifierStatement(names.toBatch), ydbpool.DropClassifierStatement(names.toIdle),
		ydbpool.DropPoolStatement(names.batch), ydbpool.DropPoolStatement(names.idle),
		"ALTER RESOURCE POOL default RESET (RESOURCE_WEIGHT);",
	} {
		_ = conn.Writer().ExecuteSQL(ctx, statement)
	}
}

// poolDeclaration is a pool with a limit, a queue and a fractional memory
// share, a pool with no setting, the pool default with a weight, and two
// classifiers sending a member's queries to the two pools at the ranks given.
// The member is a name no user holds, so neither classifier routes a query of
// another test.
func poolDeclaration(
	names poolNames,
	limit int32,
	queue *int32,
	memory *float64,
	batchRank, idleRank int64,
) *schemamodel.Database {
	return &schemamodel.Database{
		ResourcePools: []schemamodel.ResourcePool{
			{Name: names.batch, Spec: ast.ResourcePoolSpec{
				ConcurrentQueryLimit: new(limit), QueueSize: queue, QueryMemoryLimitPercentPerNode: memory,
			}},
			{Name: names.idle},
			{Name: ydbpool.DefaultPool, Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}},
		},
		ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{
			{Name: names.toBatch, Spec: ast.ResourcePoolClassifierSpec{
				ResourcePool: names.batch, MemberName: names.member, Rank: batchRank,
			}},
			{Name: names.toIdle, Spec: ast.ResourcePoolClassifierSpec{
				ResourcePool: names.idle, MemberName: names.member + "g", Rank: idleRank,
			}},
		},
	}
}

// poolsOf is what a read reports of the run's pools and classifiers, in
// declaration order, with the pool default.
func poolsOf(live *catalog.Database, names poolNames) ([]catalog.ResourcePool, []catalog.ResourcePoolClassifier) {
	var pools []catalog.ResourcePool
	for _, name := range []string{names.batch, names.idle, ydbpool.DefaultPool} {
		index := slices.IndexFunc(live.ResourcePools, func(pool catalog.ResourcePool) bool { return pool.Name == name })
		if index >= 0 {
			pools = append(pools, live.ResourcePools[index])
		}
	}
	var classifiers []catalog.ResourcePoolClassifier
	for _, name := range []string{names.toBatch, names.toIdle} {
		index := slices.IndexFunc(live.ResourcePoolClassifiers,
			func(classifier catalog.ResourcePoolClassifier) bool { return classifier.Name == name })
		if index >= 0 {
			classifiers = append(classifiers, live.ResourcePoolClassifiers[index])
		}
	}
	return pools, classifiers
}

// TestYDBResourcePools_RoundTrip applies a declaration of pools and
// classifiers, reads it back as declared and plans nothing after; then
// changes a pool's limit and resets two of its settings, and has the two
// classifiers trade ranks, which YDB refuses step by step and the plan makes
// by dropping and creating both, and again plans nothing after.
func TestYDBResourcePools_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			c.Assert(conn.Info().Capabilities.Has(capability.ResourcePools), qt.IsTrue)
			names := newPoolNames(c)
			c.Cleanup(func() { removePools(conn, names) })

			declared := poolDeclaration(names, 5, new(int32(4)), new(12.5), names.rank, names.rank+1)
			first := planAgainst(c, conn, declared, poolSchemas)
			c.Assert(first, qt.Contains, "CREATE RESOURCE POOL `"+names.batch+
				"` WITH (CONCURRENT_QUERY_LIMIT = 5, QUEUE_SIZE = 4, QUERY_MEMORY_LIMIT_PERCENT_PER_NODE = '12.5')")
			c.Assert(first, qt.Contains, "CREATE RESOURCE POOL `"+names.idle+"` WITH (CONCURRENT_QUERY_LIMIT = \"-1\")")
			apply(c, conn, first)

			pools, classifiers := poolsOf(readScoped(c, conn, poolSchemas), names)
			c.Assert(pools, qt.DeepEquals, []catalog.ResourcePool{
				{Name: names.batch, Spec: declared.ResourcePools[0].Spec},
				{Name: names.idle},
				{Name: ydbpool.DefaultPool, Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}},
			})
			c.Assert(classifiers, qt.DeepEquals, []catalog.ResourcePoolClassifier{
				{Name: names.toBatch, Spec: declared.ResourcePoolClassifiers[0].Spec},
				{Name: names.toIdle, Spec: declared.ResourcePoolClassifiers[1].Spec},
			})
			c.Assert(planAgainst(c, conn, declared, poolSchemas), qt.HasLen, 0)

			changed := poolDeclaration(names, 7, nil, nil, names.rank+1, names.rank)
			apply(c, conn, planAgainst(c, conn, changed, poolSchemas))

			pools, classifiers = poolsOf(readScoped(c, conn, poolSchemas), names)
			c.Assert(pools, qt.DeepEquals, []catalog.ResourcePool{
				{Name: names.batch, Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(7))}},
				{Name: names.idle},
				{Name: ydbpool.DefaultPool, Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}},
			})
			c.Assert(classifiers, qt.DeepEquals, []catalog.ResourcePoolClassifier{
				{Name: names.toBatch, Spec: changed.ResourcePoolClassifiers[0].Spec},
				{Name: names.toIdle, Spec: changed.ResourcePoolClassifiers[1].Spec},
			})
			c.Assert(planAgainst(c, conn, changed, poolSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBResourcePools_Rollback plans a declaration and its rollback through
// the generator, applies both, and finds the run's pools and classifiers gone
// and the pool default as the database held it.
func TestYDBResourcePools_Rollback(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newPoolNames(c)
			c.Cleanup(func() { removePools(conn, names) })
			info := conn.Info()
			current := readScoped(c, conn, poolSchemas)
			declared := poolDeclaration(names, 5, nil, nil, names.rank, names.rank+1)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, info, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Diff: diff, DesiredSchema: declared, CurrentSchema: current,
				Dialect: info.Dialect, Capabilities: info.Capabilities,
				Runtime: must.Must(builtin.New())})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)

			applyScript(c, conn, forward)
			c.Assert(planAgainst(c, conn, declared, poolSchemas), qt.HasLen, 0)
			applyScript(c, conn, reverse)

			pools, classifiers := poolsOf(readScoped(c, conn, poolSchemas), names)
			c.Assert(classifiers, qt.HasLen, 0)
			c.Assert(pools, qt.DeepEquals, []catalog.ResourcePool{
				current.ResourcePools[slices.IndexFunc(current.ResourcePools,
					func(pool catalog.ResourcePool) bool { return pool.Name == ydbpool.DefaultPool })],
			})
		})
	}
}

// A read of a dev realm leaves the pools out and records both kinds as
// outside its scope: a realm is a directory standing in for a database, and
// the pools a read would find are its database's.
func TestYDBResourcePools_ADevRealmLeavesThemOut(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			realm := connect(c, enterRealm(c, line))
			c.Assert(realm.Info().Capabilities.Has(capability.ResourcePools), qt.IsTrue)

			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), realm, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(live.ResourcePools, qt.HasLen, 0)
			c.Assert(live.ResourcePoolClassifiers, qt.HasLen, 0)
			c.Assert(live.NotDescribed.Describes(coverage.ResourcePool), qt.IsFalse)
			c.Assert(live.NotDescribed.Describes(coverage.ResourcePoolClassifier), qt.IsFalse)
		})
	}
}
