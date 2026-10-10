package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/sqlschema"
)

const feedTable = "CREATE TABLE events (id Int64 NOT NULL, PRIMARY KEY (id));"
const feedDeclaration = "ALTER TABLE events ADD CHANGEFEED updates WITH (mode = 'NEW_AND_OLD_IMAGES', format = 'JSON');"

func TestReadYQLChangefeedSettings(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(feedTable+`ALTER TABLE events ADD CHANGEFEED updates WITH (
 mode = 'NEW_AND_OLD_IMAGES', format = 'JSON', virtual_timestamps = TRUE,
 resolved_timestamps = Interval('PT1S'), initial_scan = TRUE, user_sids = TRUE,
 schema_changes = TRUE, topic_min_active_partitions = 2, topic_auto_partitioning = 'ENABLED', retention_period = Interval('PT12H'));
 ALTER TOPIC `+"`events/updates`"+` ADD CONSUMER audit WITH (important = TRUE, supported_codecs = 'raw,gzip');`), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(database.FeatureObjects, database.Tables[0].Schema, database.Tables[0].Name)), qt.DeepEquals, []ydbschema.ChangefeedSpec{{
		Name: "updates", Mode: "NEW_AND_OLD_IMAGES", Format: "JSON", VirtualTimestamps: true,
		ResolvedTimestamps: "PT1S", InitialScan: true, UserSIDs: true, SchemaChanges: true,
		TopicMinActivePartitions: 2, TopicAutoPartitioning: true, RetentionPeriod: "PT12H",
		Consumers: []ydbtopic.ConsumerSpec{{Name: "audit", Important: true, SupportedCodecs: []string{"raw", "gzip"}}},
	}})
}

func TestReadYQLChangefeedIdentityAcrossFiles(t *testing.T) {
	c := qt.New(t)
	base, _, err := sqlschema.Read([]byte("CREATE TABLE `app.events` (id Int64 NOT NULL, PRIMARY KEY (id)); CREATE TABLE `app/events` (id Int64 NOT NULL, PRIMARY KEY (id)); CREATE TOPIC `app.events/audit`;"), "ydb")
	c.Assert(err, qt.IsNil)
	document := sqlschema.NewDocument(&base)
	addition, _, err := sqlschema.ReadOnto([]byte("ALTER TABLE `app.events` ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON'); ALTER TABLE `app/events` ADD CHANGEFEED updates WITH (mode='KEYS_ONLY', format='JSON');"), "ydb", document)
	c.Assert(err, qt.IsNil)
	merged, err := schemamodel.Merge(&base, &addition)
	c.Assert(err, qt.IsNil)
	base = *merged
	_, _, err = sqlschema.ReadOnto([]byte("ALTER TOPIC `app.events/updates` ADD CONSUMER dotted; ALTER TOPIC `app/events/updates` ADD CONSUMER nested; ALTER TOPIC `app.events/audit` ADD CONSUMER ordinary;"), "ydb", document)
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(base.FeatureObjects, base.Tables[0].Schema, base.Tables[0].Name))[0].Consumers, qt.DeepEquals, []ydbtopic.ConsumerSpec{{Name: "dotted"}})
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(base.FeatureObjects, base.Tables[1].Schema, base.Tables[1].Name))[0].Consumers, qt.DeepEquals, []ydbtopic.ConsumerSpec{{Name: "nested"}})
	topic, found, err := base.FeatureObjects.Get(ydbtopic.Ref("app.events", "audit"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(topic.Value, qt.DeepEquals, &ydbtopic.Desired{Spec: ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "ordinary"}}}})
}

func TestReadYQLChangefeedRefusals(t *testing.T) {
	for _, suffix := range []string{
		"ALTER TABLE events ADD CHANGEFEED updates;",
		"ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON', name='other');",
		"ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON', retention_period='PT1H');",
		"ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON', resolved_timestamps=Interval('PT0.5S'));",
		"ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON', topic_auto_partitioning=TRUE);",
		"ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON', topic_min_active_partitions=0);",
		"ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UNKNOWN', format='JSON');",
		"ALTER TABLE events DROP CHANGEFEED updates;",
		feedDeclaration + feedDeclaration,
		"ALTER TABLE absent ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON');",
		feedDeclaration + "ALTER TOPIC `absent/updates` ADD CONSUMER worker;",
		feedDeclaration + "ALTER TOPIC `events/absent` ADD CONSUMER worker;",
		feedDeclaration + "ALTER TOPIC `events/updates` ADD CONSUMER worker; ALTER TOPIC `events/updates` ADD CONSUMER worker;",
		"CREATE TOPIC queue (CONSUMER worker); ALTER TOPIC queue ADD CONSUMER worker;",
		"ALTER TOPIC queue ADD CONSUMER worker; CREATE TOPIC queue;",
	} {
		t.Run(suffix, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(feedTable+suffix), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}
