package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
)

func managedFeed(attributes map[string]string) *Ydb_Table.ChangefeedDescription {
	return &Ydb_Table.ChangefeedDescription{Name: "ordinary_name", Mode: Ydb_Table.ChangefeedMode_MODE_UPDATES,
		Format: Ydb_Table.ChangefeedFormat_FORMAT_JSON, State: Ydb_Table.ChangefeedDescription_STATE_ENABLED, Attributes: attributes}
}

func TestRead_ReplicationBindingDoesNotDependOnAStreamNameOrLocalOwner(t *testing.T) {
	c := qt.New(t)
	db := readFrom(c, changefeedSource(managedFeed(map[string]string{"__async_replication": `{"path":"/remote/replica","id":"7","supports_topic_autopartitioning":false}`}), plainTopic()))
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	value := objects[0].Value.(*ydbschema.ObservedChangefeed)
	c.Assert(value.Spec.Name, qt.Equals, "ordinary_name")
	c.Assert(value.Replication, qt.DeepEquals, &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "7"})
	c.Assert(db.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, objects[0].Ref).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(ydbreplication.ReplicationKind)
	}).Len(), qt.Equals, 0)
}

func TestRead_MalformedReplicationMetadataStaysUnrepresentable(t *testing.T) {
	for _, attributes := range []map[string]string{
		{"__async_replication": `{"path":"/replica","id":"1"}`},
		{"__async_replication": `{"path":"/replica","id":"1","supports_topic_autopartitioning":false,"unknown":true}`},
		{"__async_replication": `{"path":"/replica","id":"0","supports_topic_autopartitioning":false}`},
		{"__async_replication": `{"path":"/replica","id":"1","supports_topic_autopartitioning":false}`, "other": "unknown"},
	} {
		t.Run(attributes["__async_replication"], func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, changefeedSource(managedFeed(attributes), plainTopic()))
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
			c.Assert(db.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, ydbschema.ChangefeedRef("app", "t", "ordinary_name")).State, qt.Equals, schemaext.Unrepresentable)
		})
	}
}

func TestRead_GeneratedLookingNameDoesNotAssertReplicationOwnership(t *testing.T) {
	c := qt.New(t)
	feed := managedFeed(nil)
	feed.Name = "2b1f0c5e-0d1c-4b1a-9c3e-5d6f7a8b9c0d"
	db := readFrom(c, changefeedSource(feed, plainTopic()))
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value.(*ydbschema.ObservedChangefeed).Replication, qt.IsNil)
}
