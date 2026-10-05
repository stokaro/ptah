//go:build integration

package ydb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_DynamicConfig_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_DynamicConfig"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydbflags"
)

// moveIndexSchema is the directory the EnableMoveIndex tests write into.
const moveIndexSchema = "ptah_ydb_move_index_off"

var moveIndexSchemas = []string{moveIndexSchema}

// clusterFlagOff turns one feature flag off for the whole cluster through its
// dynamic configuration; see [setClusterFlags].
//
// YDB_FEATURE_FLAGS can only turn a flag on, so a flag that is on by default
// is turned off the way an operator turns it off on a running cluster.
func clusterFlagOff(c *qt.C, line ydbLine, yamlName, pageName string) {
	c.Helper()
	setClusterFlags(c, line, clusterFlag{yaml: yamlName, page: pageName})
}

// clusterFlag is one feature flag a test sets for the whole cluster: its name
// in the configuration, its name on the monitoring page, and its value.
type clusterFlag struct {
	yaml string
	page string
	on   bool
}

// setClusterFlags sets each flag for the whole cluster through its dynamic
// configuration, waits until the monitoring endpoint the line's URL names
// reports them, and drops the configuration again when the test ends.
//
// The configuration's feature_flags section replaces the flags the server
// started with: measured on 25.1.4.7 and 26.2.1.14, a cluster started with
// YDB_FEATURE_FLAGS=enable_external_data_sources reports the flag off once a
// configuration that names only another flag is in place. A test therefore
// names every flag it needs. The configuration belongs to the cluster, so a
// cluster that already carries one is refused rather than overwritten, and the
// tests of this package run one at a time, so no other test sees the flags.
func setClusterFlags(c *qt.C, line ydbLine, flags ...clusterFlag) {
	c.Helper()
	driver, err := ydbsdk.Open(c.Context(), dbtarget.DriverDSN(c, line.engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = driver.Close(context.Background()) })
	client := Ydb_DynamicConfig_V1.NewDynamicConfigServiceClient(ydbsdk.GRPCConn(driver))

	before := dynamicConfig(c.Context(), c, client)
	c.Assert(before.GetConfig(), qt.Equals, "",
		qt.Commentf("the cluster carries a dynamic configuration of its own, which this test would replace"))
	_, target := monitoredTarget(c, line)
	original, err := ydbflags.Read(c.Context(), target.Monitoring, target.Database, "")
	c.Assert(err, qt.IsNil)
	var section strings.Builder
	set, restored := ydbflags.Flags{}, ydbflags.Flags{}
	for _, flag := range flags {
		fmt.Fprintf(&section, "    %s: %t\n", flag.yaml, flag.on)
		set[flag.page], restored[flag.page] = flag.on, original[flag.page]
	}
	config := fmt.Sprintf("---\nmetadata:\n  kind: MainConfig\n  cluster: %q\n  version: %d\n"+
		"config:\n  feature_flags:\n%sallowed_labels: {}\nselector_config: []\n",
		before.GetIdentity().GetCluster(), before.GetIdentity().GetVersion(), section.String())
	replaced, err := client.ReplaceConfig(c.Context(), &Ydb_DynamicConfig.ReplaceConfigRequest{Config: config})
	c.Assert(err, qt.IsNil)
	assertOperation(c, replaced.GetOperation())
	c.Cleanup(func() {
		// The test's context has ended by now.
		ctx := context.Background()
		dropped, err := client.DropConfig(ctx, &Ydb_DynamicConfig.DropConfigRequest{
			Identity: dynamicConfig(ctx, c, client).GetIdentity(),
		})
		c.Assert(err, qt.IsNil)
		assertOperation(c, dropped.GetOperation())
		awaitFlags(ctx, c, line, restored)
	})
	awaitFlags(c.Context(), c, line, set)
}

// dynamicConfig reads the cluster's dynamic configuration: empty, with the
// identity a first one must name, on a cluster that carries none.
func dynamicConfig(
	ctx context.Context,
	c *qt.C,
	client Ydb_DynamicConfig_V1.DynamicConfigServiceClient,
) *Ydb_DynamicConfig.GetConfigResult {
	c.Helper()
	response, err := client.GetConfig(ctx, &Ydb_DynamicConfig.GetConfigRequest{})
	c.Assert(err, qt.IsNil)
	assertOperation(c, response.GetOperation())
	var result Ydb_DynamicConfig.GetConfigResult
	c.Assert(response.GetOperation().GetResult().UnmarshalTo(&result), qt.IsNil)
	return &result
}

// assertOperation holds an operation the dynamic configuration service
// answered to success.
func assertOperation(c *qt.C, operation *Ydb_Operations.Operation) {
	c.Helper()
	c.Assert(operation.GetStatus(), qt.Equals, Ydb.StatusIds_SUCCESS, qt.Commentf("%v", operation.GetIssues()))
}

// awaitFlags waits until the monitoring endpoint the line's URL names lists
// each flag want names with the value want gives it: the cluster hands a new
// configuration to its nodes in the background.
func awaitFlags(ctx context.Context, c *qt.C, line ydbLine, want ydbflags.Flags) {
	c.Helper()
	_, target := monitoredTarget(c, line)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		flags, err := ydbflags.Read(ctx, target.Monitoring, target.Database, "")
		c.Assert(err, qt.IsNil)
		if listsEvery(flags, want) {
			return
		}
		select {
		case <-ctx.Done():
			c.Fatalf("the monitoring endpoint does not list %v", want)
		case <-time.After(time.Second):
		}
	}
}

// listsEvery reports whether flags gives every flag want names the value want
// gives it.
func listsEvery(flags, want ydbflags.Flags) bool {
	for name, value := range want {
		if flags[name] != value {
			return false
		}
	}
	return true
}

// moveIndexDeclaration is a table with one synchronous index named index.
func moveIndexDeclaration(index string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Schema: moveIndexSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "kind", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "Item", Name: index, Fields: []string{"kind"}}},
	}
	schemamodel.Finalize(db)
	return db
}

// A cluster that turned EnableMoveIndex off refuses ALTER TABLE ... RENAME
// INDEX. A connection that reads the cluster's flags learns that index_rename
// is off, and an index the declaration renames is planned as a drop and an add,
// which the cluster takes. TestYDBGlobalIndexes_RenameRoundTrip is the control:
// with the flag at its default the same rename is one RENAME INDEX.
func TestYDBConnection_ClusterWithoutMoveIndex_HappyPath(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			clusterFlagOff(c, line, "enable_move_index", "EnableMoveIndex")
			conn := openYDB(c, line)
			dropTables(c, conn, moveIndexSchemas)
			c.Cleanup(func() { dropTables(c, conn, moveIndexSchemas) })
			apply(c, conn, planAgainst(c, conn, moveIndexDeclaration("idx_items_kind"), moveIndexSchemas))

			renamed := moveIndexDeclaration("idx_items_by_kind")
			rebuild := planAgainst(c, conn, renamed, moveIndexSchemas)
			apply(c, conn, rebuild)

			c.Assert(conn.Info().Capabilities.Has(capability.IndexRename), qt.IsFalse)
			c.Assert(rebuild, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_move_index_off/items` DROP INDEX `idx_items_kind`",
				"ALTER TABLE `ptah_ydb_move_index_off/items` ADD INDEX `idx_items_by_kind` GLOBAL SYNC ON (`kind`)",
			})
			c.Assert(planAgainst(c, conn, renamed, moveIndexSchemas), qt.HasLen, 0)
			c.Assert(indexNamed(c, readScoped(c, conn, moveIndexSchemas), "idx_items_by_kind").Columns,
				qt.DeepEquals, []string{"kind"})
		})
	}
}

// A RENAME INDEX that reaches a cluster with EnableMoveIndex off, as one does
// from a plan made without the cluster's flags, fails with an error that names
// the capability and the flag rather than the server's text alone.
func TestYDBWriter_ClusterWithoutMoveIndex_FailurePath(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			clusterFlagOff(c, line, "enable_move_index", "EnableMoveIndex")
			conn := openYDB(c, line)
			dropTables(c, conn, moveIndexSchemas)
			c.Cleanup(func() { dropTables(c, conn, moveIndexSchemas) })
			apply(c, conn, planAgainst(c, conn, moveIndexDeclaration("idx_items_kind"), moveIndexSchemas))

			err := conn.Writer().ExecuteSQL(c.Context(),
				"ALTER TABLE `ptah_ydb_move_index_off/items` RENAME INDEX `idx_items_kind` TO `idx_items_by_kind`")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s)ydb: SQL execution failed: capability index_rename is off on this YDB `+
				`cluster, which runs with feature flag EnableMoveIndex off: .*Move index is not supported yet.*`)
			c.Assert(indexNamed(c, readScoped(c, conn, moveIndexSchemas), "idx_items_kind").Columns,
				qt.DeepEquals, []string{"kind"})
		})
	}
}
