package ydbchangefeed_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbchangefeed"
)

func TestParseDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ast.ChangefeedSpec
	}{
		{
			name:   "the two options YDB requires, folded to capitals",
			values: map[string]string{"name": "updates", "mode": "updates", "format": "json"},
			want:   ast.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"},
		},
		{
			name: "every option",
			values: map[string]string{
				"name": "feed", "mode": "NEW_AND_OLD_IMAGES", "format": "DEBEZIUM_JSON",
				"virtual_timestamps": "true", "resolved_timestamps": "PT10S", "initial_scan": "TRUE",
				"user_sids": "true", "schema_changes": "true", "topic_min_active_partitions": "4",
				"topic_auto_partitioning": "true", "retention_period": "P1D",
			},
			want: ast.ChangefeedSpec{
				Name: "feed", Mode: "NEW_AND_OLD_IMAGES", Format: "DEBEZIUM_JSON", VirtualTimestamps: true,
				ResolvedTimestamps: "PT10S", InitialScan: true, UserSIDs: true, SchemaChanges: true,
				TopicMinActivePartitions: 4, TopicAutoPartitioning: true, RetentionPeriod: "P1D",
			},
		},
		{
			name:   "a switch set to false declares nothing",
			values: map[string]string{"name": "f", "mode": "KEYS_ONLY", "format": "JSON", "initial_scan": "false"},
			want:   ast.ChangefeedSpec{Name: "f", Mode: "KEYS_ONLY", Format: "JSON"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbchangefeed.ParseDeclaration(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParseDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "no name", values: map[string]string{"mode": "UPDATES", "format": "JSON"},
			wantErr: `invalid name "": a changefeed needs a name`},
		{name: "no mode", values: map[string]string{"name": "f", "format": "JSON"},
			wantErr: `invalid mode "": takes one of KEYS_ONLY, UPDATES, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES`},
		{name: "no format", values: map[string]string{"name": "f", "mode": "UPDATES"},
			wantErr: `invalid format "": takes one of JSON, DEBEZIUM_JSON`},
		{name: "the document-table format", values: map[string]string{"name": "f", "mode": "UPDATES",
			"format": "dynamodb_streams_json"}, wantErr: `invalid format "dynamodb_streams_json": YDB writes .*`},
		{name: "a switch that is not a boolean", values: map[string]string{"name": "f", "mode": "UPDATES",
			"format": "JSON", "virtual_timestamps": "yes"}, wantErr: `invalid virtual_timestamps "yes": takes true or false`},
		{name: "a fraction of a second", values: map[string]string{"name": "f", "mode": "UPDATES",
			"format": "JSON", "retention_period": "PT1.5S"},
			wantErr: `invalid retention_period "PT1.5S": interval "PT1.5S" has a fraction of a second, .*`},
		{name: "a zero interval", values: map[string]string{"name": "f", "mode": "UPDATES",
			"format": "JSON", "resolved_timestamps": "PT0S"},
			wantErr: `invalid resolved_timestamps "PT0S": interval "PT0S" is no time at all, and YDB takes only a positive interval`},
		{name: "zero partitions", values: map[string]string{"name": "f", "mode": "UPDATES",
			"format": "JSON", "topic_min_active_partitions": "0"},
			wantErr: `invalid topic_min_active_partitions "0": takes a count of at least 1 .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbchangefeed.ParseDeclaration(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, ast.ChangefeedSpec{})
		})
	}
}

func TestParseConsumer_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ast.TopicConsumerSpec
	}{
		{name: "a name alone", values: map[string]string{"name": "audit", "changefeed": "feed"},
			want: ast.TopicConsumerSpec{Name: "audit"}},
		{
			name: "every setting, the time kept in UTC and the codecs in lower case",
			values: map[string]string{"name": "c", "important": "true",
				"read_from": "2026-01-01T03:00:00+03:00", "supported_codecs": "RAW, gzip"},
			want: ast.TopicConsumerSpec{Name: "c", Important: true, ReadFrom: "2026-01-01T00:00:00Z",
				SupportedCodecs: []string{"raw", "gzip"}},
		},
		{name: "an availability period", values: map[string]string{"name": "c", "availability_period": "PT1H"},
			want: ast.TopicConsumerSpec{Name: "c", AvailabilityPeriod: "PT1H"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbchangefeed.ParseConsumer(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParseConsumer_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "no name", values: make(map[string]string), wantErr: `invalid name "": a consumer needs a name`},
		{name: "a slash", values: map[string]string{"name": "shared/c"},
			wantErr: `invalid name "shared/c": a consumer's name cannot hold a slash .*`},
		{name: "a fraction of a second", values: map[string]string{"name": "c", "read_from": "2026-01-01T00:00:00.5Z"},
			wantErr: `invalid read_from "2026-01-01T00:00:00.5Z": YDB keeps whole seconds, and drops the fraction of this one`},
		{name: "not a time", values: map[string]string{"name": "c", "read_from": "yesterday"},
			wantErr: `invalid read_from "yesterday": takes an RFC 3339 time such as 2026-01-01T00:00:00Z`},
		{name: "an unknown codec", values: map[string]string{"name": "c", "supported_codecs": "raw,snappy"},
			wantErr: `invalid supported_codecs "raw,snappy": takes a comma-separated list of raw, gzip, lzop, zstd, custom`},
		{name: "a codec twice", values: map[string]string{"name": "c", "supported_codecs": "raw,RAW"},
			wantErr: `invalid supported_codecs "raw,RAW": names codec raw twice`},
		{name: "important and limited", values: map[string]string{"name": "c", "important": "true",
			"availability_period": "PT1H"}, wantErr: `invalid availability_period "PT1H": YDB keeps every unread .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbchangefeed.ParseConsumer(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, ast.TopicConsumerSpec{})
		})
	}
}

// TestSeconds_HappyPath reads an interval in YDB's grammar, which
// ydbttl.IntervalSeconds holds, in any case.
func TestSeconds_HappyPath(t *testing.T) {
	tests := []struct {
		text string
		want uint64
	}{
		{text: "PT1S", want: 1},
		{text: "PT90M", want: 5400},
		{text: "P1DT2H30M5S", want: 95405},
		{text: "P2W3D", want: 17 * 86400},
		{text: "pt12h", want: 43200},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbchangefeed.Seconds(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestSeconds_FailurePath refuses what ydbttl.IntervalSeconds refuses, and an
// interval of no length, which a TTL takes and a changefeed does not.
func TestSeconds_FailurePath(t *testing.T) {
	tests := []struct {
		text    string
		wantErr string
	}{
		{text: "PT0S", wantErr: `interval "PT0S" is no time at all, and YDB takes only a positive interval`},
		{text: "PT0.5S", wantErr: `interval "PT0.5S" has a fraction of a second, .*`},
		{text: "P1M", wantErr: `interval "P1M" is not an ISO 8601 duration YDB takes .*`},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbchangefeed.Seconds(test.text)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, uint64(0))
		})
	}
}

// TestCheck_HappyPath holds that a changefeed the target can write passes, the
// options a line lacks included where the line has them.
func TestCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		spec ast.ChangefeedSpec
		caps capability.Capabilities
	}{
		{name: "the plainest changefeed on the oldest line", caps: capability.YDB251(),
			spec: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON"}},
		{name: "every option on the newest line", caps: capability.YDB262(),
			spec: ast.ChangefeedSpec{Name: "f", Mode: "NEW_IMAGE", Format: "JSON", UserSIDs: true,
				SchemaChanges: true, TopicAutoPartitioning: true, ResolvedTimestamps: "PT1S", RetentionPeriod: "P31D",
				Consumers: []ast.TopicConsumerSpec{{Name: "a", AvailabilityPeriod: "PT1H"}, {Name: "b", Important: true}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.Check("app.items", test.spec, test.caps), qt.IsNil)
		})
	}
}

// TestCheck_RefusesWhatTheTargetLacks pins each option to the key that gates
// it, on the newest line that lacks it.
func TestCheck_RefusesWhatTheTargetLacks(t *testing.T) {
	plain := ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON"}
	withSIDs, withSchema, withAuto, withAvailability := plain, plain, plain, plain
	withSIDs.UserSIDs = true
	withSchema.SchemaChanges = true
	withAuto.TopicAutoPartitioning = true
	withAvailability.Consumers = []ast.TopicConsumerSpec{{Name: "c", AvailabilityPeriod: "PT1H"}}
	tests := []struct {
		name string
		spec ast.ChangefeedSpec
		caps capability.Capabilities
		want ydbchangefeed.Refusal
	}{
		{name: "any changefeed off YDB", spec: plain, caps: capability.Postgres17(),
			want: ydbchangefeed.Refusal{Subject: `changefeed "f" of table "app.items"`, Key: capability.Changefeeds}},
		{name: "USER_SIDS on 25.4", spec: withSIDs, caps: capability.YDB254(),
			want: ydbchangefeed.Refusal{Subject: `changefeed "f" of table "app.items" takes USER_SIDS`,
				Key: capability.ChangefeedUserSIDs}},
		{name: "SCHEMA_CHANGES on 25.2", spec: withSchema, caps: capability.YDB252(),
			want: ydbchangefeed.Refusal{Subject: `changefeed "f" of table "app.items" takes SCHEMA_CHANGES`,
				Key: capability.ChangefeedSchemaChanges}},
		{name: "an auto-partitioned topic on 25.1", spec: withAuto, caps: capability.YDB251(),
			want: ydbchangefeed.Refusal{Subject: `changefeed "f" of table "app.items" takes TOPIC_AUTO_PARTITIONING`,
				Key: capability.ChangefeedTopicAutoPartitioning}},
		{name: "an availability period on 25.3", spec: withAvailability, caps: capability.YDB253(),
			want: ydbchangefeed.Refusal{Subject: `consumer "c" of changefeed "f" of table "app.items" takes availability_period`,
				Key: capability.TopicConsumerAvailabilityPeriod}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbchangefeed.Check("app.items", test.spec, test.caps)
			c.Assert(got, qt.IsNotNil)
			c.Assert(*got, qt.DeepEquals, test.want)
		})
	}
}

// TestCheck_RefusesWhatYDBRefusesEverywhere pins the shapes YDB refuses on
// every line, with no key to name.
func TestCheck_RefusesWhatYDBRefusesEverywhere(t *testing.T) {
	tests := []struct {
		name       string
		spec       ast.ChangefeedSpec
		wantReason string
	}{
		{name: "Debezium in UPDATES mode", spec: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "DEBEZIUM_JSON"},
			wantReason: "YDB writes DEBEZIUM_JSON in every mode but UPDATES .*"},
		{name: "Debezium with virtual timestamps", spec: ast.ChangefeedSpec{Name: "f", Mode: "NEW_IMAGE", Format: "DEBEZIUM_JSON",
			VirtualTimestamps: true}, wantReason: "YDB writes DEBEZIUM_JSON with no virtual timestamps, .*"},
		{name: "Debezium with resolved timestamps", spec: ast.ChangefeedSpec{Name: "f", Mode: "NEW_IMAGE", Format: "DEBEZIUM_JSON",
			ResolvedTimestamps: "PT1S"}, wantReason: "YDB writes DEBEZIUM_JSON with no virtual timestamps, .*"},
		{name: "Debezium with schema changes", spec: ast.ChangefeedSpec{Name: "f", Mode: "NEW_IMAGE", Format: "DEBEZIUM_JSON",
			SchemaChanges: true}, wantReason: "YDB writes DEBEZIUM_JSON with no virtual timestamps, .*"},
		{name: "a disabled changefeed", spec: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON", Disabled: true},
			wantReason: "YDB has no statement that disables a changefeed .*"},
		{name: "an unknown mode", spec: ast.ChangefeedSpec{Name: "f", Mode: "ALL", Format: "JSON"},
			wantReason: `its mode "ALL" is none of .*`},
		{name: "a slash in the name", spec: ast.ChangefeedSpec{Name: "a/b", Mode: "UPDATES", Format: "JSON"},
			wantReason: "a changefeed needs a name without a slash .*"},
		{name: "a fractional retention", spec: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON",
			RetentionPeriod: "PT1.5S"}, wantReason: `its retention_period "PT1.5S": interval "PT1.5S" has a fraction of a second, .*`},
		{name: "two consumers of one name", spec: ast.ChangefeedSpec{Name: "f", Mode: "UPDATES", Format: "JSON",
			Consumers: []ast.TopicConsumerSpec{{Name: "c"}, {Name: "c"}}},
			wantReason: `two of its consumers are named "c", .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbchangefeed.Check("t", test.spec, capability.YDB262())
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.Key, qt.Equals, capability.Capability(""))
			c.Assert(got.Reason, qt.Matches, test.wantReason)
		})
	}
}

func TestNameRefusal(t *testing.T) {
	feed := ast.ChangefeedSpec{Name: "feed"}
	tests := []struct {
		name        string
		changefeeds []ast.ChangefeedSpec
		indexes     []string
		want        string
	}{
		{name: "distinct names", changefeeds: []ast.ChangefeedSpec{feed}, indexes: []string{"ix"}, want: ""},
		{name: "an index's name", changefeeds: []ast.ChangefeedSpec{feed}, indexes: []string{"feed"},
			want: `changefeed "feed" has the name of one of its indexes, and YDB keeps both under the table's path`},
		{name: "two changefeeds", changefeeds: []ast.ChangefeedSpec{feed, feed},
			want: `two of its changefeeds are named "feed"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.NameRefusal(test.changefeeds, test.indexes), qt.Equals, test.want)
		})
	}
}

func TestKeyRefusal(t *testing.T) {
	two := ast.ChangefeedSpec{Name: "f", TopicMinActivePartitions: 2}
	tests := []struct {
		name    string
		spec    ast.ChangefeedSpec
		keyType string
		want    string
	}{
		{name: "two partitions on a Uint64 key", spec: two, keyType: "Uint64", want: ""},
		{name: "two partitions on a Uint32 key", spec: two, keyType: "Uint32", want: ""},
		{name: "one partition on a Utf8 key", spec: ast.ChangefeedSpec{TopicMinActivePartitions: 1}, keyType: "Utf8", want: ""},
		{name: "two partitions on a Utf8 key", spec: two, keyType: "Utf8",
			want: "its topic starts with 2 partitions, which YDB splits by the first key column, and takes that " +
				"column only as Uint32 or Uint64, not Utf8"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbchangefeed.KeyRefusal(test.spec, test.keyType), qt.Equals, test.want)
		})
	}
}

func TestCodecName(t *testing.T) {
	c := qt.New(t)
	for number, want := range map[int32]string{1: "raw", 2: "gzip", 3: "lzop", 4: "zstd", 10000: "custom"} {
		got, ok := ydbchangefeed.CodecName(number)
		c.Check(ok, qt.IsTrue)
		c.Check(got, qt.Equals, want)
	}
	got, ok := ydbchangefeed.CodecName(5)
	c.Assert(ok, qt.IsFalse)
	c.Assert(got, qt.Equals, "")
}
