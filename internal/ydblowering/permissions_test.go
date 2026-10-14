package ydblowering_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/ydblowering"
)

// objectGrantsDesired declares the topic app.events, a grant on it, and a
// revoke of an entry on the topic app.audit, which it does not declare.
func objectGrantsDesired() *schemamodel.Database {
	return &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{}))),
		Roles:          []schemamodel.Role{{Name: "reader"}},
		Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"},
			OnObject: "app.events", ObjectKind: "TOPIC"}},
		RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"YDB.GENERIC.WRITE"},
			OnObject: "app.audit", ObjectKind: "TOPIC"}},
	}
}

// TestAdoptObjectGrants_HappyPath adopts every entry the database holds on a
// scheme object the declaration does not declare, so a comparison leaves it
// in place: an HCL document, which cannot name a topic, applied back keeps
// the entries its roles hold on one. An entry on a declared object, an entry
// the declaration revokes, an entry YDB gave a secret it created, and an
// entry on a table are not adopted. The declaration is not modified.
func TestAdoptObjectGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	desired := objectGrantsDesired()
	current := &catalog.Database{Grants: []catalog.Grant{
		{Role: "reader", Privilege: "ydb.generic.list", ObjectType: "TOPIC", Schema: "app", ObjectName: "events"},
		{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TOPIC", Schema: "app", ObjectName: "audit"},
		{Role: "reader", Privilege: "ydb.generic.write", ObjectType: "TOPIC", Schema: "app", ObjectName: "audit"},
		{Role: "METADATA-READERS", Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET", Schema: "app",
			ObjectName: "pw", Implicit: true},
		{Role: "reader", Privilege: "ydb.generic.use", ObjectType: "COORDINATION NODE", ObjectName: "locks"},
		{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TABLE", Schema: "app", ObjectName: "orders"},
	}}

	lowered := ydblowering.AdoptObjectGrants(desired, current)

	c.Assert(lowered.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "reader", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}, OnObject: "app.events", ObjectKind: "TOPIC"},
		{Role: "reader", Privileges: []string{"YDB.GENERIC.READ"}, OnObject: "app.audit", ObjectKind: "TOPIC"},
		{Role: "reader", Privileges: []string{"YDB.GENERIC.USE"}, OnObject: "locks", ObjectKind: "COORDINATION NODE"},
	})
	c.Assert(desired, qt.DeepEquals, objectGrantsDesired())
}

// TestAdoptObjectGrants_NothingToAdopt returns the declaration itself when
// there is no read, and when the read holds nothing to adopt.
func TestAdoptObjectGrants_NothingToAdopt(t *testing.T) {
	tests := []struct {
		name    string
		current *catalog.Database
	}{
		{name: "no read"},
		{name: "a read of entries on declared objects only", current: &catalog.Database{Grants: []catalog.Grant{
			{Role: "reader", Privilege: "ydb.generic.list", ObjectType: "TOPIC", Schema: "app", ObjectName: "events"},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := objectGrantsDesired()
			c.Assert(ydblowering.AdoptObjectGrants(desired, test.current), qt.Equals, desired)
		})
	}
}
