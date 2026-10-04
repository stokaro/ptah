//go:build integration

package ydb_test

import (
	"context"
	"path"
	"testing"

	qt "github.com/frankban/quicktest"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydbcoordination"
)

// coordinationSchema is the directory the coordination node tests write into.
const coordinationSchema = "ptah_ydb_coordination"

var coordinationSchemas = []string{coordinationSchema}

// coordinationDeclaration is a table and two coordination nodes in the test
// directory: limits with the settings given, and plain with none.
func coordinationDeclaration(limits ast.CoordinationNodeSpec) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Job", Name: "jobs", Schema: coordinationSchema}},
		Fields: []schemamodel.Field{{StructName: "Job", Name: "id", Type: "BIGINT", Primary: true}},
		CoordinationNodes: []schemamodel.CoordinationNode{
			{Schema: coordinationSchema, Name: "limits", Spec: limits},
			{Schema: coordinationSchema, Name: "plain"},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// coordinationDriver opens an SDK driver of the test's own on the line's
// database, which asks the coordination service what Ptah's reader is not
// trusted to say.
func coordinationDriver(c *qt.C, line ydbLine) *ydbsdk.Driver {
	c.Helper()
	driver, err := ydbsdk.Open(context.Background(), dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver
}

// nodeConfig describes the node at relative, a path below the database root,
// through the SDK. A path that holds no node answers SCHEME_ERROR.
func nodeConfig(c *qt.C, driver *ydbsdk.Driver, relative string) (coordination.NodeConfig, error) {
	c.Helper()
	_, config, err := driver.Coordination().DescribeNode(c.Context(), path.Join(driver.Name(), relative))
	if config == nil {
		return coordination.NodeConfig{}, err
	}
	return *config, err
}

// noNode is what the coordination service answers about a path that holds no
// node.
const noNode = `(?s).*SCHEME_ERROR.*`

// dropCoordinationDirectory removes the test directory with every node and
// table in it.
func dropCoordinationDirectory(c *qt.C, conn *dbschema.DatabaseConnection, dir string) {
	c.Helper()
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(ctx context.Context, dir string) error
	})
	c.Assert(ok, qt.IsTrue)
	if err := dropper.DropDirectory(context.Background(), dir); err != nil {
		c.Logf("drop %s: %v", dir, err)
	}
}

// TestYDBCoordinationNodes_RoundTrip creates coordination nodes from a
// declaration, reads them back through Ptah's reader and through the
// coordination service, and plans nothing after; the same declaration applied
// again plans nothing too. A change names only the settings that differ, and
// the node keeps every other one. A setting declared at YDB's default and one
// left out describe the same node. A node the declaration leaves out is
// dropped.
func TestYDBCoordinationNodes_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			driver := coordinationDriver(c, line)
			dropCoordinationDirectory(c, conn, coordinationSchema)
			c.Cleanup(func() { dropCoordinationDirectory(c, conn, coordinationSchema) })

			limits := ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 2500, SessionGracePeriodMillis: 15000,
				ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
			}
			declared := coordinationDeclaration(limits)
			first := planAgainst(c, conn, declared, coordinationSchemas)
			c.Assert(first, qt.DeepEquals, []string{
				"CREATE TABLE `ptah_ydb_coordination/jobs` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n)",
				"CREATE COORDINATION NODE `ptah_ydb_coordination/limits` WITH (self_check_period = Interval('PT2.5S'), " +
					"session_grace_period = Interval('PT15S'), read_consistency_mode = 'strict', " +
					"attach_consistency_mode = 'relaxed', rate_limiter_counters_mode = 'detailed')",
				"CREATE COORDINATION NODE `ptah_ydb_coordination/plain`",
			})
			apply(c, conn, first)
			c.Assert(planAgainst(c, conn, declared, coordinationSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, coordinationSchemas))
			c.Assert(planAgainst(c, conn, declared, coordinationSchemas), qt.HasLen, 0)

			c.Assert(readScoped(c, conn, coordinationSchemas).CoordinationNodes, qt.DeepEquals, []catalog.CoordinationNode{
				{Schema: coordinationSchema, Name: "limits", Spec: limits},
				{Schema: coordinationSchema, Name: "plain"},
			})
			served, err := nodeConfig(c, driver, coordinationSchema+"/limits")
			c.Assert(err, qt.IsNil)
			c.Assert(served, qt.DeepEquals, coordination.NodeConfig{
				SelfCheckPeriodMillis: 2500, SessionGracePeriodMillis: 15000,
				ReadConsistencyMode: coordination.ConsistencyModeStrict, AttachConsistencyMode: coordination.ConsistencyModeRelaxed,
				RatelimiterCountersMode: coordination.RatelimiterCountersModeDetailed,
			})

			// The plain node's settings at their defaults, written out.
			atDefaults := coordinationDeclaration(limits)
			atDefaults.CoordinationNodes[1].Spec = ydbcoordination.Defaults()
			c.Assert(planAgainst(c, conn, atDefaults, coordinationSchemas), qt.HasLen, 0)

			changed := limits
			changed.SelfCheckPeriodMillis, changed.ReadConsistencyMode = 3000, "relaxed"
			declared = coordinationDeclaration(changed)
			change := planAgainst(c, conn, declared, coordinationSchemas)
			c.Assert(change, qt.DeepEquals, []string{
				"ALTER COORDINATION NODE `ptah_ydb_coordination/limits` SET (self_check_period = Interval('PT3S'), " +
					"read_consistency_mode = 'relaxed')",
			})
			apply(c, conn, change)
			c.Assert(planAgainst(c, conn, declared, coordinationSchemas), qt.HasLen, 0)
			served, err = nodeConfig(c, driver, coordinationSchema+"/limits")
			c.Assert(err, qt.IsNil)
			c.Assert(served, qt.DeepEquals, coordination.NodeConfig{
				SelfCheckPeriodMillis: 3000, SessionGracePeriodMillis: 15000,
				ReadConsistencyMode: coordination.ConsistencyModeRelaxed, AttachConsistencyMode: coordination.ConsistencyModeRelaxed,
				RatelimiterCountersMode: coordination.RatelimiterCountersModeDetailed,
			})

			declared.CoordinationNodes = declared.CoordinationNodes[:1]
			drop := planAgainst(c, conn, declared, coordinationSchemas)
			c.Assert(drop, qt.DeepEquals, []string{"DROP COORDINATION NODE `ptah_ydb_coordination/plain`"})
			apply(c, conn, drop)
			c.Assert(planAgainst(c, conn, declared, coordinationSchemas), qt.HasLen, 0)
			_, err = nodeConfig(c, driver, coordinationSchema+"/plain")
			c.Assert(err, qt.ErrorMatches, noNode)
		})
	}
}

// TestYDBCoordinationNodes_ConnectionRefuses holds the refusals Ptah's
// connection makes before the coordination service changes anything: Ptah's
// lock node, a creation of a node that exists, which the service would answer
// with success and leave as it was, a change the node would not run with as
// written, and a statement inside a transaction, which a rollback could not
// undo. The node each refusal is about is read back unchanged.
func TestYDBCoordinationNodes_ConnectionRefuses(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			driver := coordinationDriver(c, line)
			dropCoordinationDirectory(c, conn, coordinationSchema)
			c.Cleanup(func() { dropCoordinationDirectory(c, conn, coordinationSchema) })
			apply(c, conn, []string{
				"CREATE COORDINATION NODE `ptah_ydb_coordination/n` WITH (self_check_period = Interval('PT2S'))",
			})
			// The lock node exists once a run took a lock, and Ptah creates it
			// with the service's defaults.
			c.Assert(driver.Coordination().CreateNode(c.Context(), path.Join(driver.Name(), ydbcoordination.LockNode),
				coordination.NodeConfig{}), qt.IsNil)

			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "DROP COORDINATION NODE ptah_locks"), qt.ErrorMatches,
				`(?s).*coordination node ptah_locks at the database root holds Ptah's own locks.*`)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"CREATE COORDINATION NODE `ptah_ydb_coordination/n` WITH (self_check_period = Interval('PT3S'))"),
				qt.ErrorMatches, `(?s).*create YDB coordination node /\w+/ptah_ydb_coordination/n: the node already exists.*`)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(),
				"ALTER COORDINATION NODE `ptah_ydb_coordination/n` SET (session_grace_period = Interval('PT2.5S'))"),
				qt.ErrorMatches, `(?s).*session_grace_period PT2.5S: YDB runs a node with a grace period from the self-check `+
					`period plus PT1S \(PT3S here\).*`)
			tx, err := conn.BeginTx(c.Context(), nil)
			c.Assert(err, qt.IsNil)
			_, inTransaction := tx.ExecContext(c.Context(),
				"ALTER COORDINATION NODE `ptah_ydb_coordination/n` SET (read_consistency_mode = 'strict')")
			c.Assert(tx.Rollback(), qt.IsNil)
			c.Assert(inTransaction, qt.ErrorMatches, `(?s).*a coordination node statement runs outside a transaction.*`)

			_, locksErr := nodeConfig(c, driver, ydbcoordination.LockNode)
			served, servedErr := nodeConfig(c, driver, coordinationSchema+"/n")
			c.Assert(locksErr, qt.IsNil)
			c.Assert(servedErr, qt.IsNil)
			c.Assert(served, qt.DeepEquals, coordination.NodeConfig{SelfCheckPeriodMillis: 2000})
		})
	}
}

// TestYDBCoordinationNodes_ReaderLeavesTheLockNodeOut reads the database root
// holding Ptah's lock node and a node of the same name in a directory: the
// first is Ptah's and left out, the second is an ordinary node.
func TestYDBCoordinationNodes_ReaderLeavesTheLockNodeOut(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			driver := coordinationDriver(c, line)
			dropCoordinationDirectory(c, conn, coordinationSchema)
			c.Cleanup(func() { dropCoordinationDirectory(c, conn, coordinationSchema) })
			c.Assert(driver.Coordination().CreateNode(c.Context(), path.Join(driver.Name(), ydbcoordination.LockNode),
				coordination.NodeConfig{}), qt.IsNil)
			apply(c, conn, []string{"CREATE COORDINATION NODE `ptah_ydb_coordination/ptah_locks`"})

			live := readScoped(c, conn, []string{"", coordinationSchema})

			c.Assert(live.CoordinationNodes, qt.DeepEquals, []catalog.CoordinationNode{
				{Schema: coordinationSchema, Name: "ptah_locks"},
			})
		})
	}
}

// TestYDBCoordinationNodes_InADevRealm creates a node through a dev realm's
// connection, whose relative paths land under the realm, and resets the
// realm, which drops it.
func TestYDBCoordinationNodes_InADevRealm(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			realmURL := enterRealm(c, line)
			realm := connect(c, realmURL)
			driver := coordinationDriver(c, line)
			relative := path.Join(realmPath(c, realmURL), "app/locks")

			apply(c, realm, []string{"CREATE COORDINATION NODE `app/locks` WITH (attach_consistency_mode = 'relaxed')"})
			served, err := nodeConfig(c, driver, relative)
			c.Assert(err, qt.IsNil)
			c.Assert(served, qt.DeepEquals, coordination.NodeConfig{AttachConsistencyMode: coordination.ConsistencyModeRelaxed})
			inRealm, readErr := dbschema.ReadSchemaWithSchemasContext(c.Context(), realm, nil)
			c.Assert(readErr, qt.IsNil)
			c.Assert(inRealm.CoordinationNodes, qt.DeepEquals, []catalog.CoordinationNode{
				{Schema: "app", Name: "locks", Spec: ast.CoordinationNodeSpec{AttachConsistencyMode: "relaxed"}},
			})

			resetter, ok := realm.SchemaWriter().(interface {
				DropDatabaseRealm(ctx context.Context) error
			})
			c.Assert(ok, qt.IsTrue)
			c.Assert(resetter.DropDatabaseRealm(c.Context()), qt.IsNil)
			_, err = nodeConfig(c, driver, relative)
			c.Assert(err, qt.ErrorMatches, noNode)
		})
	}
}
