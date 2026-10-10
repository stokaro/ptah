package ydb_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

func coordinationCreated(schema, name string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbcoordination.Ref(schema, name), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}
}

func coordinationDropped(schema, name string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbcoordination.Ref(schema, name), Value: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}
}

// TestGenerateMigrationAST_ObjectsBelowAPathFollowItsDrop pins the order of a
// standalone object created below a path whose occupant the plan drops, and
// of one dropped below a path where the plan creates an object: YDB needs
// every directory above an object to be a directory. A coordination node
// orders itself against a common table the way a secret does, and two owners
// are ordered against each other, though neither sees the other's statements.
func TestGenerateMigrationAST_ObjectsBelowAPathFollowItsDrop(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{name: "a coordination node below a dropped table",
			diff: &difftypes.SchemaDiff{
				TablesRemoved:  difftypes.TableRemovals{{Name: "ext", Current: observedFeeds(t, "", "ext")}},
				FeatureChanges: []schemaext.ChangeRecord{coordinationCreated("ext", "lock")},
			},
			want: "DROP TABLE `ext`;\nCREATE COORDINATION NODE `ext/lock`;\n"},
		{name: "a secret below a dropped coordination node",
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				coordinationDropped("", "ext"), secretCreated("ext", "pw", "PTAH_SECRET_PW"),
			}},
			want: "DROP COORDINATION NODE `ext`;\nCREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n"},
		{name: "a coordination node below a dropped secret",
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				secretDropped("", "ext"), coordinationCreated("ext", "lock"),
			}},
			want: "DROP SECRET `ext`;\nCREATE COORDINATION NODE `ext/lock`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.YDB262(), test.diff), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_OwnersTradeAPath hands one path from an object of
// one owner to an object of another, which neither owner sees: the drop runs
// first, so the creation finds the path free.
func TestGenerateMigrationAST_OwnersTradeAPath(t *testing.T) {
	empty := ydbtopic.Spec{}
	tests := []struct {
		name    string
		changes []schemaext.ChangeRecord
		want    string
	}{
		{name: "a coordination node where a secret was",
			changes: []schemaext.ChangeRecord{coordinationCreated("", "ext"), secretDropped("", "ext")},
			want:    "DROP SECRET `ext`;\nCREATE COORDINATION NODE `ext`;\n"},
		{name: "a secret where a topic was",
			changes: []schemaext.ChangeRecord{secretCreated("", "ext", "PTAH_SECRET_EXT"), topicChange("ext", &empty, nil)},
			want:    "DROP TOPIC `ext`;\nCREATE SECRET `ext` WITH (value = $PTAH_SECRET_EXT);\n"},
		{name: "a topic where a coordination node was",
			changes: []schemaext.ChangeRecord{topicChange("ext", nil, &empty), coordinationDropped("", "ext")},
			want:    "DROP COORDINATION NODE `ext`;\nCREATE TOPIC `ext`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(render(c, capability.YDB262(), &difftypes.SchemaDiff{FeatureChanges: test.changes}), qt.Equals, test.want)
		})
	}
}

// TestGenerateMigrationAST_CreationOverAnEmptiedDirectory_FailurePath pins
// the refusal of a plan that drops every object below a path and creates an
// object at the path.
//
// YDB keeps the directory after its last object is dropped and YQL has no
// statement that removes one, so the creation failed at apply with "Path is
// not a table or topic" after the drops had run, and the next plan planned the
// same creation again (stokaro/ptah#4282).
func TestGenerateMigrationAST_CreationOverAnEmptiedDirectory_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{name: "a table over a dropped coordination node",
			diff: &difftypes.SchemaDiff{
				TablesAdded:    difftypes.TableChanges{{Name: "ext", Table: schemamodel.Table{StructName: "S", Name: "ext"}, Fields: []schemamodel.Field{keyField("id")}}},
				FeatureChanges: []schemaext.ChangeRecord{coordinationDropped("ext", "lock")},
			},
			want: "drops ext/lock and creates an object at ext,"},
		{name: "a table over a dropped table",
			diff: &difftypes.SchemaDiff{
				TablesAdded:   difftypes.TableChanges{{Name: "ext", Table: schemamodel.Table{StructName: "S", Name: "ext"}, Fields: []schemamodel.Field{keyField("id")}}},
				TablesRemoved: difftypes.TableRemovals{{Name: "ext.t", Current: observedFeeds(t, "ext", "t")}},
			},
			want: "drops ext/t and creates an object at ext,"},
		{name: "a secret over a coordination node two levels down",
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				secretCreated("", "ext", "PTAH_SECRET_EXT"), coordinationDropped("ext/locks", "lock"),
			}},
			want: "drops ext/locks/lock and creates an object at ext,"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(t.Context(), must.Must(builtin.New()), test.diff)
			c.Assert(err, qt.ErrorAs, new(*schemavalidation.RefusalError))
			c.Assert(err, qt.ErrorMatches, `(?s).*this plan `+regexp.QuoteMeta(test.want)+` which is a directory in the database.*`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
