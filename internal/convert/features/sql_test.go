package features_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/sqlschema"
)

func TestSQLFeatureObjects_AcrossFilesRetainStructuredParents(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	files := map[string]string{
		"01-tables.sql":    "CREATE TABLE `app.events` (id Int64 NOT NULL, PRIMARY KEY (id)); CREATE TABLE `app/events` (id Int64 NOT NULL, PRIMARY KEY (id));",
		"02-streams.sql":   "ALTER TABLE `app.events` ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON'); ALTER TABLE `app/events` ADD CHANGEFEED updates WITH (mode='KEYS_ONLY', format='JSON');",
		"03-consumers.sql": "ALTER TOPIC `app.events/updates` ADD CONSUMER dotted; ALTER TOPIC `app/events/updates` ADD CONSUMER nested;",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	database, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 2)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 2)
	for _, test := range []struct{ schema, table, consumer string }{
		{table: "app.events", consumer: "dotted"}, {schema: "app", table: "events", consumer: "nested"},
	} {
		feeds, err := ydbschema.DesiredChangefeeds(database.FeatureObjects, test.schema, test.table)
		c.Assert(err, qt.IsNil)
		c.Assert(feeds, qt.HasLen, 1)
		c.Assert(feeds[0].Consumers, qt.HasLen, 1)
		c.Assert(feeds[0].Consumers[0].Name, qt.Equals, test.consumer)
		c.Assert(database.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, ydbschema.ChangefeedRef(test.schema, test.table, "updates")).State, qt.Equals, schemaext.Complete)
	}
}

func TestSQLFeatureObjects_RefuseDuplicateStreamAndConsumer(t *testing.T) {
	const table = "CREATE TABLE events (id Int64 NOT NULL, PRIMARY KEY (id));"
	const feed = "ALTER TABLE events ADD CHANGEFEED updates WITH (mode='UPDATES', format='JSON');"
	const consumer = "ALTER TOPIC `events/updates` ADD CONSUMER audit;"
	for _, suffix := range []string{feed + feed, feed + consumer + consumer} {
		t.Run(suffix, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(table+suffix), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestSourceMerge_UnknownFirstSourceStaysUnknown(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	hcl := filepath.Join(dir, "unknown.hcl")
	sql := filepath.Join(dir, "known.sql")
	c.Assert(os.WriteFile(hcl, []byte(""), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(sql, []byte("CREATE TABLE events (id Int64 NOT NULL, PRIMARY KEY (id));"), 0o600), qt.IsNil)
	for _, sources := range [][]schemafile.Source{
		{{URL: hcl}, {URL: sql}}, {{URL: sql}, {URL: hcl}},
	} {
		database, err := schemafile.LoadSources(sources, schemafile.Options{Dialect: "ydb"})
		c.Assert(err, qt.IsNil)
		c.Assert(database.FeatureCoverage.Representation(), qt.Equals, schemaext.Desired)
		c.Assert(database.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, ydbschema.ChangefeedRef("", "events", "updates")).State, qt.Equals, schemaext.Uninspected)
	}
}
