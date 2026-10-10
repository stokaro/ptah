//go:build integration

package ydb_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

func TestYDBDesiredYQL_CoordinationAndPools(t *testing.T) {
	const directory = "ptah_ydb_yql_resources"
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newPoolNames(c)
			c.Cleanup(func() {
				for _, statement := range []string{
					ydbworkload.DropClassifierStatement(names.toBatch),
					ydbworkload.DropPoolStatement(names.batch),
					"DROP COORDINATION NODE `" + directory + "/locks`",
				} {
					_ = conn.Writer().ExecuteSQL(context.Background(), statement)
				}
			})
			schemas := []string{directory}
			file := filepath.Join(c.TempDir(), "schema.sql")
			for _, source := range []string{
				fmt.Sprintf("CREATE COORDINATION NODE `%s/locks` WITH (attach_consistency_mode = 'relaxed'); CREATE RESOURCE POOL `%s` WITH (CONCURRENT_QUERY_LIMIT = 4, QUEUE_SIZE = 8); CREATE RESOURCE POOL CLASSIFIER `%s` WITH (RESOURCE_POOL = '%s', RANK = %d, MEMBER_NAME = '%s');", directory, names.batch, names.toBatch, names.batch, names.rank, names.member),
				fmt.Sprintf("CREATE COORDINATION NODE `%s/locks` WITH (attach_consistency_mode = 'strict'); CREATE RESOURCE POOL `%s` WITH (CONCURRENT_QUERY_LIMIT = 8); CREATE RESOURCE POOL CLASSIFIER `%s` WITH (RESOURCE_POOL = '%s', RANK = %d, MEMBER_NAME = '%s');", directory, names.batch, names.toBatch, names.batch, names.rank, names.member),
				"",
			} {
				c.Assert(os.WriteFile(file, []byte(source), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{file}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				statements := planAgainst(c, conn, desired, schemas)
				c.Assert(statements, qt.Not(qt.HasLen), 0)
				apply(c, conn, statements)
				c.Assert(planAgainst(c, conn, desired, schemas), qt.HasLen, 0)
			}
			live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, schemas)
			c.Assert(err, qt.IsNil)
			nodes := live.FeatureObjects.Select(func(ref objectidentity.ID) bool {
				return ref.Kind == objectidentity.Kind(ydbcoordination.Kind)
			})
			c.Assert(nodes.Len(), qt.Equals, 0)
			pools, classifiers := poolsOf(c, live, names)
			c.Assert(pools, qt.HasLen, 2)
			c.Assert(pools[0], qt.DeepEquals, ydbworkload.ObservedPoolObject(names.batch, ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(8))}))
			c.Assert(classifiers, qt.DeepEquals, []schemaext.Object{
				ydbworkload.ObservedClassifierObject(names.toBatch, ydbworkload.ClassifierSpec{ResourcePool: names.batch, MemberName: names.member, Rank: names.rank}),
			})
		})
	}
}
