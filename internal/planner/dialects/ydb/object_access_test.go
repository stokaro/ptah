package ydb_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// objectGrant is one permission entry of reader on the scheme object name of
// kind, as a comparison names it.
func objectGrant(kind, name string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: kind, ObjectName: name}
}

// ownedPlanCaps is a line that holds every kind of scheme object a grant
// names.
func ownedPlanCaps() capability.Capabilities {
	return externalPlanCaps(false).With(capability.StreamingQueries, true).With(capability.ResourcePools, true)
}

// streamingChange is a change of the streaming query name at the database
// root from before to after; a nil side is the query's absence.
func streamingChange(name string, before *ydbstreaming.Observed, after *ydbstreaming.Desired) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbstreaming.Ref("", name), Value: &ydbdiff.StreamingQuery{Before: before, After: after}}
}

// copyQuery is a stopped streaming query that copies one topic to another.
var copyQuery = ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM src;", Run: new(false)}

// ingestTransfer is a transfer of a changefeed into a table.
var ingestTransfer = ydbreplication.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: lambda}

// nodeChange is a change of the coordination node name in the directory
// schema from before to after; a nil side is the node's absence.
func nodeChange(schema, name string, before *ydbcoordination.Observed, after *ydbcoordination.Desired) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbcoordination.Ref(schema, name),
		Value: &ydbdiff.CoordinationNode{Before: before, After: after}}
}

// TestGenerateMigrationAST_ObjectPermissions_GoWithADroppedObject pins that a
// plan writes no REVOKE for an entry on a scheme object it drops, of every
// kind a grant names: the drop takes the object's access list with it, and a
// REVOKE after it would name an object that is gone. The drop is planned.
func TestGenerateMigrationAST_ObjectPermissions_GoWithADroppedObject(t *testing.T) {
	tests := []struct {
		name     string
		change   schemaext.ChangeRecord
		grant    difftypes.GrantRef
		wantDrop string
	}{
		{name: "a topic", change: topicChange("events", &ydbtopic.Spec{}, nil),
			grant: objectGrant("TOPIC", "events"), wantDrop: "DROP TOPIC `events`;"},
		{name: "a coordination node", change: nodeChange("app", "locks", &ydbcoordination.Observed{}, nil),
			grant: objectGrant("COORDINATION NODE", "app.locks"), wantDrop: "app/locks"},
		{name: "a secret", change: secretDropped("ext", "pw"),
			grant: objectGrant("SECRET", "ext.pw"), wantDrop: "DROP SECRET `ext/pw`;"},
		{name: "an external data source", change: sourceChange("ext", "bucket", &plannedBucket, nil),
			grant: objectGrant("EXTERNAL DATA SOURCE", "ext.bucket"), wantDrop: "DROP EXTERNAL DATA SOURCE `ext/bucket`;"},
		{name: "an external table", change: tableChange("ext", "events", &plannedEvents, nil),
			grant: objectGrant("EXTERNAL TABLE", "ext.events"), wantDrop: "DROP EXTERNAL TABLE `ext/events`;"},
		{name: "an async replication", change: replicationDropped("mirror", replicationOf("src", "copy"), ydbreplication.StateRunning),
			grant: objectGrant("ASYNC REPLICATION", "mirror"), wantDrop: "DROP ASYNC REPLICATION `mirror`"},
		{name: "a transfer", change: transferDropped("ingest", ingestTransfer),
			grant: objectGrant("TRANSFER", "ingest"), wantDrop: "DROP TRANSFER `ingest`;"},
		{name: "a streaming query", change: streamingChange("copy", &ydbstreaming.Observed{Spec: copyQuery}, nil),
			grant: objectGrant("STREAMING QUERY", "copy"), wantDrop: "DROP STREAMING QUERY `copy`;"},
		{name: "a resource pool", change: poolChange("batch", &ydbworkload.ObservedPool{}, nil),
			grant: objectGrant("RESOURCE POOL", "batch"), wantDrop: "DROP RESOURCE POOL `batch`;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{test.change},
				GrantsRemoved: []difftypes.GrantRef{test.grant}, CurrentDatabasePath: "/local"}

			sql := render(c, ownedPlanCaps(), diff)

			c.Assert(sql, qt.Contains, test.wantDrop)
			c.Assert(sql, qt.Not(qt.Contains), "REVOKE")
		})
	}
}

// TestGenerateMigrationAST_ObjectPermissions_FollowTheObject pins where a
// GRANT and a REVOKE on a scheme object an owner manages go: a GRANT after
// the statement that creates the object, on every kind, and by the object's
// absolute path where the line resolves no relative one; a REVOKE on an
// object the plan changes in place, which keeps its entries, after the
// change.
func TestGenerateMigrationAST_ObjectPermissions_FollowTheObject(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "a topic the plan creates", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{topicChange("events", nil, &ydbtopic.Spec{})},
				GrantsAdded: []difftypes.GrantRef{objectGrant("TOPIC", "events")}},
			want: []string{"CREATE TOPIC `events`;", "GRANT 'ydb.granular.select_row' ON `events` TO `reader`;"}},
		{name: "a root topic on a line that takes it by its absolute path", caps: capability.YDB251(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{topicChange("events", nil, &ydbtopic.Spec{})},
				GrantsAdded: []difftypes.GrantRef{objectGrant("TOPIC", "events")}, CurrentDatabasePath: "/local"},
			want: []string{"CREATE TOPIC `events`;", "GRANT 'ydb.granular.select_row' ON `/local/events` TO `reader`;"}},
		{name: "a secret the plan creates", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{secretCreated("ext", "pw", "PTAH_SECRET_PW")},
				GrantsAdded: []difftypes.GrantRef{objectGrant("SECRET", "ext.pw")}},
			want: []string{"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);",
				"GRANT 'ydb.granular.select_row' ON `ext/pw` TO `reader`;"}},
		{name: "an external table the plan creates", caps: externalPlanCaps(false),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{tableChange("ext", "events", nil, &plannedEvents)},
				GrantsAdded: []difftypes.GrantRef{objectGrant("EXTERNAL TABLE", "ext.events")}},
			want: []string{"CREATE EXTERNAL TABLE `ext/events`", "GRANT 'ydb.granular.select_row' ON `ext/events` TO `reader`;"}},
		{name: "a coordination node the plan creates, which is not placed early", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{nodeChange("app", "locks", nil, &ydbcoordination.Desired{})},
				GrantsAdded: []difftypes.GrantRef{objectGrant("COORDINATION NODE", "app.locks")}},
			want: []string{"CREATE COORDINATION NODE `app/locks`;", "GRANT 'ydb.granular.select_row' ON `app/locks` TO `reader`;"}},
		{name: "an async replication the plan creates", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{replicationCreated("mirror", replicationOf("src", "copy"))},
				GrantsAdded: []difftypes.GrantRef{objectGrant("ASYNC REPLICATION", "mirror")}},
			want: []string{"CREATE ASYNC REPLICATION `mirror` FOR `src` AS `copy` WITH (CONNECTION_STRING = " +
				"'grpc://primary:2136/?database=/prod');",
				"GRANT 'ydb.granular.select_row' ON `mirror` TO `reader`;"}},
		{name: "a transfer the plan creates", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{transferCreated("ingest", ingestTransfer)},
				GrantsAdded: []difftypes.GrantRef{objectGrant("TRANSFER", "ingest")}},
			want: []string{"CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING " + lambda + ";",
				"GRANT 'ydb.granular.select_row' ON `ingest` TO `reader`;"}},
		{name: "a streaming query the plan creates", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{streamingChange("copy", nil,
				&ydbstreaming.Desired{Spec: copyQuery})}, GrantsAdded: []difftypes.GrantRef{objectGrant("STREAMING QUERY", "copy")}},
			want: []string{"CREATE STREAMING QUERY `copy` WITH (RUN = FALSE, RESOURCE_POOL = `default`) AS DO BEGIN",
				"GRANT 'ydb.granular.select_row' ON `copy` TO `reader`;"}},
		{name: "a running streaming query the plan changes, its GRANT after the restart", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{streamingChange("copy",
				&ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: copyQuery.Text, Run: new(true)}},
				&ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "INSERT INTO dst SELECT * FROM next;", Run: new(true)}, AllowStateReset: true})},
				GrantsAdded: []difftypes.GrantRef{objectGrant("STREAMING QUERY", "copy")},
				RolesAdded:  difftypes.RoleChanges{{Name: "reader", Login: true}}},
			want: []string{"ALTER STREAMING QUERY `copy`", "CREATE USER `reader`;", "ALTER STREAMING QUERY `copy`",
				"GRANT 'ydb.granular.select_row' ON `copy` TO `reader`;"}},
		{name: "a GRANT on a streaming query the plan leaves alone", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{objectGrant("STREAMING QUERY", "copy")},
				RolesAdded: difftypes.RoleChanges{{Name: "reader", Login: true}}, RolesRemoved: difftypes.RoleChanges{{Name: "old"}}},
			want: []string{"CREATE USER `reader`;", "GRANT 'ydb.granular.select_row' ON `copy` TO `reader`;", "DROP USER IF EXISTS `old`;"}},
		{name: "a resource pool the plan creates, by the path YDB gives it", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{poolChange("batch", nil, &ydbworkload.DesiredPool{})},
				GrantsAdded: []difftypes.GrantRef{objectGrant("RESOURCE POOL", "batch")}},
			want: []string{"CREATE RESOURCE POOL `batch` WITH (CONCURRENT_QUERY_LIMIT = \"-1\");",
				"GRANT 'ydb.granular.select_row' ON `.metadata/workload_manager/pools/batch` TO `reader`;"}},
		{name: "a resource pool the plan changes in place", caps: ownedPlanCaps(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{poolChange("batch", &ydbworkload.ObservedPool{},
				&ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(5))}})},
				GrantsRemoved: []difftypes.GrantRef{objectGrant("RESOURCE POOL", "batch")}},
			want: []string{"ALTER RESOURCE POOL `batch`",
				"REVOKE 'ydb.granular.select_row' ON `.metadata/workload_manager/pools/batch` FROM `reader`;"}},
		{name: "a topic the plan changes in place", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{topicChange("events", &ydbtopic.Spec{}, &ydbtopic.Spec{RetentionPeriod: "PT2H"})},
				GrantsRemoved: []difftypes.GrantRef{objectGrant("TOPIC", "events")}},
			want: []string{"ALTER TOPIC `events`", "REVOKE 'ydb.granular.select_row' ON `events` FROM `reader`;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(statementAndGrantHeads(render(c, test.caps, test.diff)), qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ObjectPermissions_RecreatedObjectGetsItsEntriesBack
// pins that an external object the plan drops and creates again gets back the
// permission entries the old one held, after the statement that creates it:
// the drop takes the entries, and the new object holds none. An object the
// plan replaces in place with CREATE OR REPLACE keeps its entries, so none is
// granted again (measured on 26.2.1.14). An entry the plan revokes is not
// given back, and no REVOKE is written for it; on an object replaced in place
// it is revoked.
func TestGenerateMigrationAST_ObjectPermissions_RecreatedObjectGetsItsEntriesBack(t *testing.T) {
	changedEvents := plannedEvents.Clone()
	changedEvents.Location = "f/"
	// Sorted as a comparison sorts the grants a read reports: by object type,
	// then by object.
	held := []difftypes.GrantRef{
		objectGrant("EXTERNAL DATA SOURCE", "ext.bucket"),
		objectGrant("EXTERNAL TABLE", "ext.events"),
		{Role: "writer", Privilege: "YDB.GENERIC.LIST", ObjectType: "EXTERNAL TABLE", ObjectName: "ext.events"},
	}
	revoked := []difftypes.GrantRef{
		{Role: "writer", Privilege: "YDB.GENERIC.LIST", ObjectType: "EXTERNAL TABLE", ObjectName: "ext.events"},
	}
	tests := []struct {
		name    string
		replace bool
		changes []schemaext.ChangeRecord
		want    []string
	}{
		{name: "an external table dropped and created again", replace: false,
			changes: []schemaext.ChangeRecord{tableChange("ext", "events", &plannedEvents, &changedEvents)},
			want: []string{"DROP EXTERNAL TABLE `ext/events`;", "CREATE EXTERNAL TABLE `ext/events`",
				"GRANT 'ydb.granular.select_row' ON `ext/events` TO `reader`;"}},
		{name: "a data source dropped and created again with its table", replace: false,
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, movedBucket()),
				tableChange("ext", "events", &plannedEvents, &plannedEvents)},
			want: []string{"DROP EXTERNAL TABLE `ext/events`;", "DROP EXTERNAL DATA SOURCE `ext/bucket`;",
				"CREATE EXTERNAL DATA SOURCE `ext/bucket`", "CREATE EXTERNAL TABLE `ext/events`",
				"GRANT 'ydb.granular.select_row' ON `ext/bucket` TO `reader`;",
				"GRANT 'ydb.granular.select_row' ON `ext/events` TO `reader`;"}},
		{name: "an external table replaced in place", replace: true,
			changes: []schemaext.ChangeRecord{tableChange("ext", "events", &plannedEvents, &changedEvents)},
			want: []string{"CREATE OR REPLACE EXTERNAL TABLE `ext/events`",
				"REVOKE 'ydb.generic.list' ON `ext/events` FROM `writer`;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: test.changes, CurrentGrants: held, GrantsRemoved: revoked}
			c.Assert(statementAndGrantHeads(render(c, externalPlanCaps(test.replace), diff)), qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ObjectPermissions_FailurePath refuses, before any
// node is returned, a permission on a kind of object the target does not
// hold, and a grant on an object the plan drops.
func TestGenerateMigrationAST_ObjectPermissions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "a grant on an external table with the flag off", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{objectGrant("EXTERNAL TABLE", "ext.events")}},
			wantErr: "the plan changes a permission on external table ext.events, which requires target capability " +
				"external_data_sources, unavailable on this ydb target"},
		{name: "a revoke on a secret on a line without secrets", caps: capability.YDB251(),
			diff: &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{objectGrant("SECRET", "ext.pw")}},
			wantErr: "the plan changes a permission on secret ext.pw, which requires target capability secrets, " +
				"unavailable on this ydb target"},
		{name: "a grant on a resource pool without the key", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{objectGrant("RESOURCE POOL", "batch")}},
			wantErr: "the plan changes a permission on resource pool batch, which requires target capability " +
				"resource_pools, unavailable on this ydb target"},
		{name: "a grant on a topic the plan drops", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{topicChange("events", &ydbtopic.Spec{}, nil)},
				GrantsAdded: []difftypes.GrantRef{objectGrant("TOPIC", "events")}},
			wantErr: "granting YDB.GRANULAR.SELECT_ROW on topic events to reader: the plan drops the object; " +
				"declare it, or leave the grant out"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(context.Background(), must.Must(builtin.New()), test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// statementAndGrantHeads is statementHeads with each ALTER, GRANT and REVOKE
// of sql in its place, an ALTER cut before the settings it sets.
func statementAndGrantHeads(sql string) []string {
	var heads []string
	for line := range strings.SplitSeq(sql, "\n") {
		if !slices.ContainsFunc([]string{"ALTER ", "CREATE ", "DROP ", "GRANT ", "REVOKE "}, func(prefix string) bool {
			return strings.HasPrefix(line, prefix)
		}) {
			continue
		}
		line, _, _ = strings.Cut(line, " SET (")
		line = strings.TrimSuffix(line, " WITH (")
		heads = append(heads, strings.TrimSuffix(line, " ("))
	}
	return heads
}

// viewGrant is one permission entry of reader on the view name, which a
// comparison names as a grant on a table.
func viewGrant(name string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "TABLE", ObjectName: name}
}

// TestGenerateMigrationAST_ViewPermissions_HappyPath pins how a plan treats
// the permission entries of a view. YDB has no CREATE OR REPLACE VIEW, so a
// view whose query changed is dropped and created again, which takes its
// entries: the plan grants them again after the new view, writes no REVOKE
// for an entry it removes there, and none for an entry on a view it drops. A
// view the plan keeps has its entry revoked, and one it creates gets its
// GRANT after the CREATE VIEW.
func TestGenerateMigrationAST_ViewPermissions_HappyPath(t *testing.T) {
	changed := difftypes.ViewDiff{ViewName: "app.active", Changes: map[string]string{"body": "SELECT 1 AS a -> SELECT 2 AS a"},
		Desired: schemamodel.View{Name: "app.active", Body: "SELECT 2 AS a"}, PreviousBody: "SELECT 1 AS a"}
	written := difftypes.GrantRef{Role: "writer", Privilege: "YDB.GENERIC.LIST", ObjectType: "TABLE", ObjectName: "app.active"}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "a view whose query changed", diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{changed},
			CurrentGrants: []difftypes.GrantRef{viewGrant("app.active"), written}, GrantsRemoved: []difftypes.GrantRef{written}},
			want: []string{"DROP VIEW `app/active`;", "CREATE VIEW `app/active` WITH (security_invoker = TRUE) AS",
				"GRANT 'ydb.granular.select_row' ON `app/active` TO `reader`;"}},
		{name: "a view the plan drops", diff: &difftypes.SchemaDiff{
			ViewsRemoved: difftypes.ViewChanges{{Name: "app.active", Body: "SELECT 1 AS a"}}, GrantsRemoved: []difftypes.GrantRef{viewGrant("app.active")}},
			want: []string{"DROP VIEW `app/active`;"}},
		{name: "a view the plan keeps", diff: &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{viewGrant("app.active")}},
			want: []string{"REVOKE 'ydb.granular.select_row' ON `app/active` FROM `reader`;"}},
		{name: "a view the plan creates", diff: &difftypes.SchemaDiff{
			ViewsAdded: difftypes.ViewChanges{{Name: "app.active", Body: "SELECT 1 AS a"}}, GrantsAdded: []difftypes.GrantRef{viewGrant("app.active")}},
			want: []string{"CREATE VIEW `app/active` WITH (security_invoker = TRUE) AS",
				"GRANT 'ydb.granular.select_row' ON `app/active` TO `reader`;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(statementAndGrantHeads(render(c, capability.YDB262(), test.diff)), qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_ViewPermissions_FailurePath refuses a grant on a
// view the plan drops, which no GRANT can reach, before any node is returned.
func TestGenerateMigrationAST_ViewPermissions_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "app.active", Body: "SELECT 1 AS a"}},
		GrantsAdded: []difftypes.GrantRef{viewGrant("app.active")}}

	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.ErrorMatches, "granting YDB.GRANULAR.SELECT_ROW on view app.active to reader: the plan drops the view; "+
		"declare it, or leave the grant out")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(nodes, qt.IsNil)
}
