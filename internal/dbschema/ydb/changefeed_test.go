package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// changefeedSource is a table app/t carrying feed, whose topic topic
// describes.
func changefeedSource(feed *Ydb_Table.ChangefeedDescription, topic *Ydb_Topic.DescribeTopicResult) fakeSource {
	table := plainTable()
	table.Changefeeds = []*Ydb_Table.ChangefeedDescription{feed}
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local":     {entry("app", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/app": {entry("t", Ydb_Scheme.Entry_TABLE)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{"/local/app/t": table},
		topics: map[string]*Ydb_Topic.DescribeTopicResult{"/local/app/t/" + feed.GetName(): topic},
	}
}

// plainTopic is a changefeed's topic as YDB creates it for a changefeed
// declaring nothing: one partition, no auto partitioning, records kept for 24
// hours, and no consumer. Measured on 25.1.4.7 and 26.2.1.14.
func plainTopic(consumers ...*Ydb_Topic.Consumer) *Ydb_Topic.DescribeTopicResult {
	return &Ydb_Topic.DescribeTopicResult{
		PartitioningSettings: &Ydb_Topic.PartitioningSettings{
			MinActivePartitions: 1, MaxActivePartitions: 1,
			AutoPartitioningSettings: &Ydb_Topic.AutoPartitioningSettings{
				Strategy: Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_DISABLED,
			},
		},
		RetentionPeriod: durationpb.New(24 * time.Hour),
		Consumers:       consumers,
	}
}

// consumer is a consumer as YDB reports one ALTER TOPIC created: from the
// start of the epoch, with the attribute YDB gives every consumer.
func consumer(name string) *Ydb_Topic.Consumer {
	return &Ydb_Topic.Consumer{Name: name, ReadFrom: timestamppb.New(time.Unix(0, 0)),
		Attributes: map[string]string{"_service_type": "data-streams"}}
}

// withUnknownBool appends a boolean field the pinned protocol buffers do not
// model, as a newer server sends it.
func withUnknownBool(feed *Ydb_Table.ChangefeedDescription, number protowire.Number) *Ydb_Table.ChangefeedDescription {
	raw := protowire.AppendTag(nil, number, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)
	feed.ProtoReflect().SetUnknown(raw)
	return feed
}

// withConsumerType appends the consumer_type arm number of a consumer, an
// empty message, as 26.2.1.14 sends it and the pinned protocol buffers do not
// model it.
func withConsumerType(consumer *Ydb_Topic.Consumer, number protowire.Number) *Ydb_Topic.Consumer {
	raw := protowire.AppendTag(nil, number, protowire.BytesType)
	raw = protowire.AppendBytes(raw, nil)
	consumer.ProtoReflect().SetUnknown(raw)
	return consumer
}

// TestReader_Changefeed_HappyPath reads a changefeed as the catalog carries
// it: its options from DescribeTable, and its retention, partitions and
// consumers from DescribeTopic on its path, each as YDB reports what the
// matching statement wrote on 25.1.4.7 and 26.2.1.14.
func TestReader_Changefeed_HappyPath(t *testing.T) {
	updates := func() *Ydb_Table.ChangefeedDescription {
		return &Ydb_Table.ChangefeedDescription{Name: "feed", Mode: Ydb_Table.ChangefeedMode_MODE_UPDATES,
			Format: Ydb_Table.ChangefeedFormat_FORMAT_JSON, State: Ydb_Table.ChangefeedDescription_STATE_ENABLED}
	}
	everything := updates()
	everything.Mode = Ydb_Table.ChangefeedMode_MODE_NEW_AND_OLD_IMAGES
	everything.VirtualTimestamps = true
	everything.ResolvedTimestampsInterval = durationpb.New(90 * time.Minute)
	everything.InitialScanProgress = &Ydb_Table.ChangefeedDescription_InitialScanProgress{PartsTotal: 1, PartsCompleted: 1}
	everything.SchemaChanges = true
	withUnknownBool(everything, 11)
	scanning := updates()
	scanning.State = Ydb_Table.ChangefeedDescription_STATE_INITIAL_SCAN
	scanning.InitialScanProgress = &Ydb_Table.ChangefeedDescription_InitialScanProgress{PartsTotal: 1}
	disabled := updates()
	disabled.State = Ydb_Table.ChangefeedDescription_STATE_DISABLED

	busyTopic := plainTopic(consumer("audit"), func() *Ydb_Topic.Consumer {
		late := consumer("late")
		late.Important = true
		late.ReadFrom = timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		late.SupportedCodecs = &Ydb_Topic.SupportedCodecs{Codecs: []int32{2, 1}}
		return late
	}(), func() *Ydb_Topic.Consumer {
		limited := consumer("limited")
		limited.AvailabilityPeriod = durationpb.New(time.Hour)
		return limited
	}())
	busyTopic.RetentionPeriod = durationpb.New(12 * time.Hour)
	busyTopic.PartitioningSettings.MinActivePartitions = 2
	busyTopic.PartitioningSettings.AutoPartitioningSettings.Strategy = Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_SCALE_UP

	tests := []struct {
		name  string
		feed  *Ydb_Table.ChangefeedDescription
		topic *Ydb_Topic.DescribeTopicResult
		want  ast.ChangefeedSpec
	}{
		{name: "a changefeed declaring nothing but its mode and format", feed: updates(), topic: plainTopic(),
			want: ast.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}},
		{
			name: "every option and a topic holding consumers", feed: everything, topic: busyTopic,
			want: ast.ChangefeedSpec{
				Name: "feed", Mode: "NEW_AND_OLD_IMAGES", Format: "JSON", VirtualTimestamps: true,
				ResolvedTimestamps: "PT1H30M", InitialScan: true, UserSIDs: true, SchemaChanges: true,
				TopicMinActivePartitions: 2, TopicAutoPartitioning: true, RetentionPeriod: "PT12H",
				Consumers: []ast.TopicConsumerSpec{
					{Name: "audit"},
					{Name: "late", Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"gzip", "raw"}},
					{Name: "limited", AvailabilityPeriod: "PT1H"},
				},
			},
		},
		{name: "a changefeed still scanning the table", feed: scanning, topic: plainTopic(),
			want: ast.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON", InitialScan: true}},
		{name: "a disabled changefeed", feed: disabled, topic: plainTopic(),
			want: ast.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON", Disabled: true}},
		{
			name: "streaming consumers as 26.2 reports them, in the order they were added",
			feed: updates(), topic: plainTopic(withConsumerType(consumer("zeta"), 9), withConsumerType(consumer("alpha"), 9)),
			want: ast.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON",
				Consumers: []ast.TopicConsumerSpec{{Name: "alpha"}, {Name: "zeta"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, changefeedSource(test.feed, test.topic))
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].Changefeeds, qt.DeepEquals, []ast.ChangefeedSpec{test.want})
			c.Assert(db.NotDescribed.Objects, qt.Not(qt.Contains), changefeedRecord)
		})
	}
}

// changefeedRecord is the record a changefeed Ptah does not model leaves.
var changefeedRecord = coverage.Object{Kind: coverage.Changefeed, Name: "app.t/feed",
	Reason: coverage.Unsupported, Provenance: coverage.Observed}

// TestReader_Changefeed_RecordsWhatItDoesNotModel records, rather than reads,
// a changefeed holding anything Ptah does not model, so a plan neither drops
// nor changes it. Read as known, each would lose that setting on the next
// plan that recreated it.
func TestReader_Changefeed_RecordsWhatItDoesNotModel(t *testing.T) {
	feed := func(edit func(*Ydb_Table.ChangefeedDescription)) *Ydb_Table.ChangefeedDescription {
		described := &Ydb_Table.ChangefeedDescription{Name: "feed", Mode: Ydb_Table.ChangefeedMode_MODE_UPDATES,
			Format: Ydb_Table.ChangefeedFormat_FORMAT_JSON, State: Ydb_Table.ChangefeedDescription_STATE_ENABLED}
		edit(described)
		return described
	}
	topic := func(edit func(*Ydb_Topic.DescribeTopicResult)) *Ydb_Topic.DescribeTopicResult {
		described := plainTopic(consumer("c"))
		edit(described)
		return described
	}
	plain := func(*Ydb_Table.ChangefeedDescription) {}
	untouched := func(*Ydb_Topic.DescribeTopicResult) {}
	tests := []struct {
		name  string
		feed  *Ydb_Table.ChangefeedDescription
		topic *Ydb_Topic.DescribeTopicResult
	}{
		{name: "the document-table format", topic: topic(untouched), feed: feed(func(f *Ydb_Table.ChangefeedDescription) {
			f.Format = Ydb_Table.ChangefeedFormat_FORMAT_DYNAMODB_STREAMS_JSON
		})},
		{name: "no mode", topic: topic(untouched), feed: feed(func(f *Ydb_Table.ChangefeedDescription) {
			f.Mode = Ydb_Table.ChangefeedMode_MODE_UNSPECIFIED
		})},
		{name: "an unknown state", topic: topic(untouched), feed: feed(func(f *Ydb_Table.ChangefeedDescription) {
			f.State = Ydb_Table.ChangefeedDescription_STATE_UNSPECIFIED
		})},
		{name: "an AWS region", topic: topic(untouched), feed: feed(func(f *Ydb_Table.ChangefeedDescription) { f.AwsRegion = "eu-1" })},
		{name: "attributes", topic: topic(untouched), feed: feed(func(f *Ydb_Table.ChangefeedDescription) {
			f.Attributes = map[string]string{"k": "v"}
		})},
		{name: "trace identifiers", topic: topic(untouched), feed: withUnknownBool(feed(plain), 12)},
		{name: "a field nobody models", topic: topic(untouched), feed: withUnknownBool(feed(plain), 13)},
		{name: "a fraction of a second", topic: topic(untouched), feed: feed(func(f *Ydb_Table.ChangefeedDescription) {
			f.ResolvedTimestampsInterval = durationpb.New(1500 * time.Millisecond)
		})},
		{name: "a topic splitting and merging", feed: feed(plain), topic: topic(func(d *Ydb_Topic.DescribeTopicResult) {
			d.PartitioningSettings.AutoPartitioningSettings.Strategy = Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN
		})},
		{name: "a consumer attribute", feed: feed(plain), topic: topic(func(d *Ydb_Topic.DescribeTopicResult) {
			d.Consumers[0].Attributes["owner"] = "team"
		})},
		{name: "a consumer codec nobody names", feed: feed(plain), topic: topic(func(d *Ydb_Topic.DescribeTopicResult) {
			d.Consumers[0].SupportedCodecs = &Ydb_Topic.SupportedCodecs{Codecs: []int32{5}}
		})},
		{name: "a shared consumer", feed: feed(plain), topic: topic(func(d *Ydb_Topic.DescribeTopicResult) {
			withConsumerType(d.Consumers[0], 10)
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, changefeedSource(test.feed, test.topic))
			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].Changefeeds, qt.IsNil)
			c.Assert(db.NotDescribed.Objects, qt.Contains, changefeedRecord)
		})
	}
}

// TestReader_Changefeed_FailurePath fails a read whose changefeed's topic the
// server will not describe, rather than reading the changefeed without its
// retention and consumers.
func TestReader_Changefeed_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := changefeedSource(&Ydb_Table.ChangefeedDescription{Name: "feed", Mode: Ydb_Table.ChangefeedMode_MODE_UPDATES,
		Format: Ydb_Table.ChangefeedFormat_FORMAT_JSON, State: Ydb_Table.ChangefeedDescription_STATE_ENABLED}, plainTopic())
	source.topics = nil

	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `YDB table /local/app/t: changefeed "feed": described topic /local/app/t/feed, which the fixture does not hold`)
	c.Assert(db, qt.IsNil)
}
