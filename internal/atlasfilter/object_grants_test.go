package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/atlasfilter"
)

// objectGrantSides is a declaration and a read that each hold a topic at the
// root and one in the directory ext, and a grant on each.
func objectGrantSides() (*schemamodel.Database, *catalog.Database) {
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{}), ydbtopic.DesiredObject("ext", "queue", "", ydbtopic.Spec{}))),
		Grants: []schemamodel.Grant{
			{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "events", ObjectKind: "TOPIC"},
			{Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "ext.queue", ObjectKind: "TOPIC"},
		},
	}
	held := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbtopic.ObservedObject("", "events", ydbtopic.Spec{}), ydbtopic.ObservedObject("ext", "queue", ydbtopic.Spec{}))),
		Grants: []catalog.Grant{
			{Role: "app", Privilege: "ydb.granular.select_row", ObjectType: "TOPIC", ObjectName: "events"},
			{Role: "app", Privilege: "ydb.granular.select_row", ObjectType: "TOPIC", Schema: "ext", ObjectName: "queue"},
		},
	}
	return declared, held
}

// TestScope_ObjectGrantsRideTheirObject keeps a grant on a scheme object
// exactly where the selection keeps the object, on both sides of a
// comparison, so a selection plans no grant or revoke on an object it left
// out.
func TestScope_ObjectGrantsRideTheirObject(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "include", scope: atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"events[type=topic]"}}, want: []string{"events"}},
		{name: "schema", scope: atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"ext"}}, want: []string{"ext.queue"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, held := objectGrantSides()

			generated, err := atlasfilter.ScopeGenerated(declared, test.scope)
			c.Assert(err, qt.IsNil)
			live, err := atlasfilter.ScopeDatabase(held, test.scope)
			c.Assert(err, qt.IsNil)

			c.Assert(declaredGrantTargets(generated.Grants), qt.DeepEquals, test.want)
			c.Assert(readGrantTargets(live.Grants), qt.DeepEquals, test.want)
		})
	}
}

// TestExclude_ObjectGrantsLeaveWithTheirObject drops a grant on an excluded
// scheme object, from the declaration and from the read alike.
func TestExclude_ObjectGrantsLeaveWithTheirObject(t *testing.T) {
	c := qt.New(t)
	declared, held := objectGrantSides()

	generated, err := atlasfilter.ExcludeGenerated(ownerSelections(), declared, []string{"ext.*[type=topic]"})
	c.Assert(err, qt.IsNil)
	live, err := atlasfilter.ExcludeDatabase(ownerSelections(), held, []string{"ext.*[type=topic]"})
	c.Assert(err, qt.IsNil)

	c.Assert(declaredGrantTargets(generated.Grants), qt.DeepEquals, []string{"events"})
	c.Assert(readGrantTargets(live.Grants), qt.DeepEquals, []string{"events"})
}

func declaredGrantTargets(grants []schemamodel.Grant) []string {
	var targets []string
	for _, grant := range grants {
		targets = append(targets, grant.OnObject)
	}
	return targets
}

func readGrantTargets(grants []catalog.Grant) []string {
	var targets []string
	for _, grant := range grants {
		targets = append(targets, grant.QualifiedTarget())
	}
	return targets
}
