package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
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
