package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestCompare_YDBObjectGrantsFollowTheDeclaredObject compares a declaration
// that manages the role reader with a database where reader holds entries on
// two topics. The entry on the topic the declaration declares, and does not
// grant, is revoked; the entry on the topic it leaves unmanaged is left as the
// database holds it, since the declaration makes no claim about that topic,
// as an HCL document, which cannot name a topic, makes none. Neither topic is
// changed.
func TestCompare_YDBObjectGrantsFollowTheDeclaredObject(t *testing.T) {
	c := qt.New(t)
	unmanaged := []schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "audit"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "left unmanaged"}}}
	desired := &schemamodel.Database{
		Roles:           []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{}))),
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, unmanaged)),
	}
	current := &catalog.Database{
		Roles:           []catalog.Role{{Name: "reader", Login: true, Inherit: true}},
		FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbtopic.ObservedObject("app", "events", ydbtopic.Spec{}), ydbtopic.ObservedObject("app", "audit", ydbtopic.Spec{}))),
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TOPIC", Schema: "app", ObjectName: "events"},
			{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TOPIC", Schema: "app", ObjectName: "audit"},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
		catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262()}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "reader", Privilege: "YDB.GENERIC.READ", ObjectType: "TOPIC", ObjectName: "app.events"},
	})
	c.Assert(diff.GrantsAdded, qt.HasLen, 0)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
}
