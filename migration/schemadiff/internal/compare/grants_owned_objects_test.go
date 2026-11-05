package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// topicKind is the owner model a TOPIC grant names in these tests.
const topicKind schemaext.Kind = "example.org/owner/topic"

// topicModel is the desired model of topicKind, as a source enrolls it.
var topicModel = schemaext.CodecIdentity{Owner: "example.org/owner", Kind: topicKind, Representation: schemaext.Desired, Version: 1, Definition: "topic-v1"}

// eventsTopic is the topic app.events.
var eventsTopic = objectidentity.ID{Kind: objectidentity.Kind(topicKind),
	Schema: objectidentity.Part{Source: "app", Normalized: "app"}, Name: objectidentity.Part{Source: "events", Normalized: "events"}}

// topicValue is a desired topic, which declares nothing beyond its identity.
type topicValue struct{}

func (topicValue) Kind() schemaext.Kind { return topicKind }

func (topicValue) Clone() schemaext.Value { return topicValue{} }

func (topicValue) Equal(other schemaext.Value) bool { return other == schemaext.Value(topicValue{}) }

// grantObjectNames names an owner's object by the object type and the name a
// grant spells, as a scope selection's grant object type does.
type grantObjectNames map[[2]string]objectidentity.ID

func (n grantObjectNames) GrantObject(objectType, objectName string) (objectidentity.ID, bool) {
	ref, named := n[[2]string{objectType, objectName}]
	return ref, named
}

// topicGrants names app.events as the topic a TOPIC grant on it is on.
var topicGrants = grantObjectNames{{"TOPIC", "app.events"}: eventsTopic}

// heldTopicGrant is the database's entry for reader on app.events.
var heldTopicGrant = catalog.Grant{Role: "reader", Privilege: "ydb.granular.select_row", ObjectType: "TOPIC", Schema: "app", ObjectName: "events"}

// revokedTopicGrant is the removal of heldTopicGrant.
var revokedTopicGrant = difftypes.GrantRef{Role: "reader", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "TOPIC", ObjectName: "app.events"}

// ownedObjectsTest is a desired source's account of the owner's objects: the
// objects it declares and the knowledge it claims.
type ownedObjectsTest struct {
	name     string
	declared []schemaext.Object
	kinds    []schemaext.KindCoverage
	subjects []schemaext.SubjectCoverage
}

func (test ownedObjectsTest) objects() compare.OwnedObjects {
	return compare.OwnedObjects{Names: topicGrants, Desired: schemaext.FeatureState{
		Objects:  must.Must(schemaext.NewObjects(test.declared...)),
		Coverage: must.Must(schemaext.NewCoverage(schemaext.Desired, test.kinds, test.subjects)),
	}}
}

// uninspectedTopics enrolls topicKind with no claim, as a source does whose
// header says it does not describe topics.
var uninspectedTopics = []schemaext.KindCoverage{{Model: topicModel,
	Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the description does not describe topic"})}}

// A source that makes no claim about an owner's object leaves the grants on
// it as the database holds them, though it manages the role that holds one:
// it does not declare the object and does not describe its kind, by a
// not-described record or by claiming nothing about it.
func TestGrantsWithObjects_AnUndescribedObjectKeepsItsGrants(t *testing.T) {
	for _, test := range []ownedObjectsTest{
		{name: "no claim about the kind"},
		{name: "the kind not described", kinds: uninspectedTopics},
		{name: "the object unrepresentable", kinds: uninspectedTopics, subjects: []schemaext.SubjectCoverage{{Kind: topicKind, Subject: eventsTopic,
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the source cannot spell it"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
			database := &catalog.Database{Roles: []catalog.Role{{Name: "reader"}}, Grants: []catalog.Grant{heldTopicGrant}}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithObjects(desired, database, diff, identifier.ForDialect("ydb"), test.objects())

			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		})
	}
}

// A source that declares an owner's object, or claims to know its state,
// manages the grants on it: an entry it leaves out of a managed role is
// revoked. Where the object is described and not declared, the plan drops it,
// and its owner accounts for the revoke in the drop.
func TestGrantsWithObjects_ADescribedObjectLosesAGrantLeftOut(t *testing.T) {
	for _, test := range []ownedObjectsTest{
		{name: "the object declared", declared: []schemaext.Object{{Ref: eventsTopic, Value: topicValue{}}}},
		{name: "the kind described", kinds: []schemaext.KindCoverage{{Model: topicModel, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}}},
		{name: "the object asserted absent", kinds: uninspectedTopics, subjects: []schemaext.SubjectCoverage{{Kind: topicKind, Subject: eventsTopic,
			Knowledge: schemaext.Knowledge{State: schemaext.Absent}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
			database := &catalog.Database{Roles: []catalog.Role{{Name: "reader"}}, Grants: []catalog.Grant{heldTopicGrant}}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithObjects(desired, database, diff, identifier.ForDialect("ydb"), test.objects())

			c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{revokedTopicGrant})
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		})
	}
}

// A revoke the source writes out is planned on an object it does not
// describe: the revoke is its own claim about that one entry.
func TestGrantsWithObjects_ARevokeOnAnUndescribedObjectIsPlanned(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}},
		RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"ydb.granular.select_row"}, OnObject: "app.events", ObjectKind: "TOPIC"}}}
	database := &catalog.Database{Roles: []catalog.Role{{Name: "reader"}}, Grants: []catalog.Grant{heldTopicGrant}}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithObjects(desired, database, diff, identifier.ForDialect("ydb"), ownedObjectsTest{kinds: uninspectedTopics}.objects())

	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{revokedTopicGrant})
}

// A grant on an object no owner names is compared as it is without owners: a
// table's entry a managed role holds and the source leaves out is revoked,
// whatever the source claims about the owner's kinds.
func TestGrantsWithObjects_ACommonObjectIsNotAnOwnersObject(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
	database := &catalog.Database{Roles: []catalog.Role{{Name: "reader"}},
		Grants: []catalog.Grant{{Role: "reader", Privilege: "ydb.granular.select_row", ObjectType: "TABLE", Schema: "app", ObjectName: "events"}}}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithObjects(desired, database, diff, identifier.ForDialect("ydb"), ownedObjectsTest{kinds: uninspectedTopics}.objects())

	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{{Role: "reader", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "TABLE", ObjectName: "app.events"}})
}
