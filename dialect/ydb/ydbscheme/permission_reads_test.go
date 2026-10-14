package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TestCommonEffects_PermissionsReadTheObjectTheyName pins that a GRANT and a
// REVOKE on a scheme object an owner manages read that object, by its owner's
// identity, so the owner's statements order them: by a reference, and by the
// absolute path a plan writes for an object at the database root.
func TestCommonEffects_PermissionsReadTheObjectTheyName(t *testing.T) {
	tests := []struct {
		name string
		root string
		node ast.Node
		want objectidentity.ID
	}{
		{name: "a grant on a topic", node: ast.NewGrantPrivilege("app", "TOPIC", "app.events", []string{"SELECT ROW"}),
			want: ydbtopic.Ref("app", "events")},
		{name: "a revoke on a secret", node: ast.NewRevokePrivilege("app", "SECRET", "ext.pw", []string{"SELECT ROW"}),
			want: ydbsecret.Ref("ext", "pw")},
		{name: "a grant on a root external table by its absolute path", root: "/local",
			node: ast.NewGrantPrivilege("app", "EXTERNAL TABLE", "/local/events", []string{"SELECT ROW"}),
			want: ydbexternal.TableRef("", "events")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects, err := ydbscheme.CommonEffects(objectidentity.NewBuilder(identifier.ForDialect("ydb")), test.root, test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(effects, qt.DeepEquals, []plangraph.Effect{{Subject: test.want, Action: plangraph.Read}})
		})
	}
}

// TestCommonEffects_PermissionsOnOtherObjectsReadNothing pins that a GRANT on
// the database, a directory or a table, and one on an absolute path with no
// database to read it against, carry no footprint: the host orders those
// itself.
func TestCommonEffects_PermissionsOnOtherObjectsReadNothing(t *testing.T) {
	for _, node := range []ast.Node{
		ast.NewGrantPrivilege("app", "TABLE", "app.events", []string{"SELECT ROW"}),
		ast.NewRevokePrivilege("app", "SCHEMA", "app", []string{"LIST"}),
		ast.NewGrantPrivilege("app", "DATABASE", "/local", []string{"CONNECT"}),
		ast.NewGrantPrivilege("app", "TOPIC", "/local/events", []string{"SELECT ROW"}),
	} {
		c := qt.New(t)
		effects, err := ydbscheme.CommonEffects(objectidentity.NewBuilder(identifier.ForDialect("ydb")), "", node)
		c.Assert(err, qt.IsNil)
		c.Assert(effects, qt.IsNil)
	}
}
