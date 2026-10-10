package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TestParse_YDBChangefeed_HappyPath reads a table's changefeeds in YAML, keyed
// by name in the order written, each option and consumer under the key the
// annotation reads.
func TestParse_YDBChangefeed_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(ydbYAMLOwners, []byte(`
tables:
  items:
    columns:
      id:
        type: bigint
        primary: true
    changefeeds:
      updates:
        mode: updates
        format: JSON
        virtual_timestamps: true
        retention_period: PT12H
        consumers:
          audit:
            important: true
          late:
            supported_codecs: [raw, GZIP]
            read_from: 2026-01-01T03:00:00+03:00
      keys:
        mode: KEYS_ONLY
        format: JSON
        topic_min_active_partitions: 2
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(db.FeatureObjects, db.Tables[0].Schema, db.Tables[0].Name)), qt.DeepEquals, []ydbschema.ChangefeedSpec{
		{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON", TopicMinActivePartitions: 2},
		{Name: "updates", Mode: "UPDATES", Format: "JSON", VirtualTimestamps: true, RetentionPeriod: "PT12H",
			Consumers: []ydbtopic.ConsumerSpec{
				{Name: "audit", Important: true},
				{Name: "late", SupportedCodecs: []string{"raw", "gzip"}, ReadFrom: "2026-01-01T00:00:00Z"},
			}},
	})
}

// TestParse_YDBChangefeed_FailurePath refuses a value YDB would refuse, an
// empty one included, and a key no changefeed or consumer has.
func TestParse_YDBChangefeed_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		changefeed string
		wantErr    string
	}{
		{name: "no format", changefeed: "mode: UPDATES",
			wantErr: `table "items": changefeed "feed": invalid format "": takes one of JSON, DEBEZIUM_JSON`},
		{name: "an empty retention", changefeed: "mode: UPDATES\n        format: JSON\n        retention_period: ''",
			wantErr: `table "items": changefeed "feed": invalid retention_period "": interval "" is not an ISO 8601 duration YDB takes .*`},
		{name: "a consumer of both kinds", changefeed: "mode: UPDATES\n        format: JSON\n        consumers:\n" +
			"          c:\n            important: true\n            availability_period: PT1H",
			wantErr: `table "items": changefeed "feed": consumer "c": invalid availability_period "PT1H": .*`},
		{name: "an unknown key", changefeed: "mode: UPDATES\n        format: JSON\n        barriers: PT1S",
			wantErr: `(?s)parse YAML schema: .*field barriers not found.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(ydbYAMLOwners, []byte(`
tables:
  items:
    columns:
      id:
        type: bigint
        primary: true
    changefeeds:
      feed:
        `+test.changefeed+`
`))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
