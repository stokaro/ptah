package ydbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestReplicationBindingSurvivesRepresentationAndCodecRoundTrips(t *testing.T) {
	c := qt.New(t)
	observed := &ydbschema.ObservedChangefeed{
		Spec:        ydbschema.ChangefeedSpec{Name: "stream", Mode: "UPDATES", Format: "JSON"},
		Replication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/destination", ItemID: "7", SupportsTopicAutopartitioning: true},
	}
	desired := observed.Desired()
	projected := desired.Observed()
	c.Assert(projected.Equal(observed), qt.IsTrue)
	c.Assert(desired.RetainedReplication, qt.DeepEquals, observed.Replication)
	desired.RetainedReplication.ItemID = "8"
	c.Assert(observed.Replication.ItemID, qt.Equals, "7")
	c.Assert(projected.Replication.ItemID, qt.Equals, "7")
	for _, test := range []struct {
		name  string
		codec int
		value schemaext.Value
	}{
		{"observed", 1, observed},
		{"retained", 0, observed.Desired()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := ydbschema.Codecs()[test.codec]
			encoded, err := codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			decoded, err := codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(test.value.Equal(decoded.(schemaext.Value)), qt.IsTrue)
			canonical, err := codec.Canonical(test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(canonical), qt.Contains, `"destination_path":"/remote/destination"`)
			c.Assert(string(canonical), qt.Contains, `"item_id":"7"`)
		})
	}
}

func TestReplicationBindingRejectsAmbiguousWireValues(t *testing.T) {
	for _, data := range []string{
		`{"destination_path":"/replica","item_id":"1"}`,
		`{"destination_path":"/replica","item_id":"1","supports_topic_autopartitioning":null}`,
		`{"destination_path":"replica","item_id":"1","supports_topic_autopartitioning":false}`,
		`{"destination_path":"/replica","item_id":"01","supports_topic_autopartitioning":false}`,
		`{"destination_path":"/replica","item_id":"0","supports_topic_autopartitioning":false}`,
		`{"destination_path":"/replica","item_id":"1","supports_topic_autopartitioning":false,"unknown":true}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			_, err := schemaext.DecodeJSON[*ydbschema.ReplicationBinding]([]byte(data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestReplicationBindingParticipatesInEqualityAndCloning(t *testing.T) {
	c := qt.New(t)
	value := &ydbschema.ObservedChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "stream", Mode: "UPDATES", Format: "JSON"},
		Replication: &ydbschema.ReplicationBinding{DestinationPath: "/replica", ItemID: "1"}}
	clone := value.Clone().(*ydbschema.ObservedChangefeed)
	clone.Replication.DestinationPath = "/other"
	c.Assert(value.Equal(clone), qt.IsFalse)
	c.Assert(value.Replication.DestinationPath, qt.Equals, "/replica")
	c.Assert(value.Equal(&ydbschema.ObservedChangefeed{Spec: value.Spec}), qt.IsFalse)
}
