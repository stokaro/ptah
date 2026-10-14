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
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// objectGrant is one permission entry of reader on the scheme object name of
// kind, as a comparison names it.
func objectGrant(kind, name string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: kind, ObjectName: name}
}

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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{test.change},
				GrantsRemoved: []difftypes.GrantRef{test.grant}, CurrentDatabasePath: "/local"}

			sql := render(c, externalPlanCaps(false), diff)

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
