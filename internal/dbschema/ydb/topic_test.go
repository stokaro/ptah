package ydb_test

import (
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// standaloneTopic is a topic described as YDB describes one created without
// settings, measured on 26.2.1.14 and 25.1.4.7, server attributes included.
func standaloneTopic() *Ydb_Topic.DescribeTopicResult {
	return &Ydb_Topic.DescribeTopicResult{
		Self: &Ydb_Scheme.Entry{Name: "events", Type: Ydb_Scheme.Entry_TOPIC},
		PartitioningSettings: &Ydb_Topic.PartitioningSettings{
			MinActivePartitions: 1, MaxActivePartitions: 1,
			AutoPartitioningSettings: &Ydb_Topic.AutoPartitioningSettings{
				Strategy: Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_DISABLED,
				PartitionWriteSpeed: &Ydb_Topic.AutoPartitioningWriteSpeedStrategy{
					StabilizationWindow: durationpb.New(300 * time.Second), UpUtilizationPercent: 80, DownUtilizationPercent: 20,
				},
			},
		},
		RetentionPeriod:                   durationpb.New(24 * time.Hour),
		PartitionWriteSpeedBytesPerSecond: 1048576,
		PartitionWriteBurstBytes:          1048576,
		Attributes:                        map[string]string{"_timestamp_type": "CreateTime", "__max_partition_message_groups_seqno_stored": "6000000"},
	}
}

// streaming marks consumer the way 26.1.1.22 and 26.2.1.14 describe every
// consumer YQL creates: an empty field 9, the streaming consumer type the
// pinned protocol buffers do not know.
func streaming(consumer *Ydb_Topic.Consumer) *Ydb_Topic.Consumer {
	consumer.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 9, protowire.BytesType), nil))
	return consumer
}

// readTopic reads a directory holding one topic described as described.
func readTopic(c *qt.C, described *Ydb_Topic.DescribeTopicResult) (*catalog.Database, error) {
	c.Helper()
	source := fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("app", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/app": {entry("events", Ydb_Scheme.Entry_TOPIC)}},
		topics: map[string]*Ydb_Topic.DescribeTopicResult{"/local/app/events": described},
	}
	return ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchema()
}

// A topic is described with what the server holds: each setting, the
// auto-partitioning ones only while it is enabled, and its consumers in the
// order the server lists them. Server attributes are left out, and so is the
// streaming consumer type a newer server reports.
func TestReader_Topic_HappyPath(t *testing.T) {
	scaled := standaloneTopic()
	scaled.PartitioningSettings = &Ydb_Topic.PartitioningSettings{
		MinActivePartitions: 2, MaxActivePartitions: 6,
		AutoPartitioningSettings: &Ydb_Topic.AutoPartitioningSettings{
			Strategy: Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_SCALE_UP,
			PartitionWriteSpeed: &Ydb_Topic.AutoPartitioningWriteSpeedStrategy{
				StabilizationWindow: durationpb.New(2 * time.Minute), UpUtilizationPercent: 70, DownUtilizationPercent: 10,
			},
		},
	}
	scaled.RetentionPeriod = durationpb.New(36 * time.Hour)
	scaled.SupportedCodecs = &Ydb_Topic.SupportedCodecs{Codecs: []int32{1, 2}}
	scaled.Consumers = []*Ydb_Topic.Consumer{
		streaming(&Ydb_Topic.Consumer{Name: "billing", Important: true, ReadFrom: timestamppb.New(time.Unix(0, 0)),
			Attributes: map[string]string{"_service_type": "data-streams"}}),
		{Name: "audit", ReadFrom: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			SupportedCodecs: &Ydb_Topic.SupportedCodecs{Codecs: []int32{4, 10000}}, AvailabilityPeriod: durationpb.New(2 * time.Hour)},
	}
	paused := standaloneTopic()
	paused.PartitioningSettings.AutoPartitioningSettings.Strategy = Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_PAUSED
	paused.PartitioningSettings.MaxActivePartitions = 4
	tests := []struct {
		name      string
		described *Ydb_Topic.DescribeTopicResult
		want      ast.TopicSpec
	}{
		{name: "created without settings", described: standaloneTopic(),
			want: ast.TopicSpec{MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "P1D",
				PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}},
		{name: "every setting and two consumers", described: scaled,
			want: ast.TopicSpec{MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
				AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
				AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "P1DT12H",
				PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576,
				SupportedCodecs: []string{"raw", "gzip"},
				Consumers: []ast.TopicConsumerSpec{
					{Name: "billing", Important: true},
					{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"zstd", "custom"}, AvailabilityPeriod: "PT2H"},
				}}},
		{name: "auto-partitioning paused", described: paused,
			want: ast.TopicSpec{MinActivePartitions: 1, MaxActivePartitions: 4, AutoPartitioningStrategy: "paused",
				AutoPartitioningUpUtilizationPercent: 80, AutoPartitioningDownUtilizationPercent: 20,
				AutoPartitioningStabilizationWindow: "PT5M", RetentionPeriod: "P1D",
				PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := readTopic(c, test.described)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Topics, qt.DeepEquals, []catalog.Topic{{Name: "events", Schema: "app", Spec: test.want}})
			c.Assert(db.NotDescribed.Describes(coverage.Topic, "app.events"), qt.IsTrue)
		})
	}
}

// A topic holding what Ptah does not read, or a value it cannot keep, is
// refused by name rather than read as the nearest topic Ptah models: read
// without it, a declaration made from the read would recreate the topic
// without it in silence.
func TestReader_Topic_FailurePath(t *testing.T) {
	withUnknown := func(described *Ydb_Topic.DescribeTopicResult) *Ydb_Topic.DescribeTopicResult {
		described.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
		return described
	}
	withConsumer := func(consumer *Ydb_Topic.Consumer) *Ydb_Topic.DescribeTopicResult {
		described := standaloneTopic()
		described.Consumers = []*Ydb_Topic.Consumer{consumer}
		return described
	}
	consumerWithField := func(number protowire.Number, payload []byte) *Ydb_Topic.DescribeTopicResult {
		consumer := &Ydb_Topic.Consumer{Name: "c"}
		consumer.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), payload))
		return withConsumer(consumer)
	}
	storage := standaloneTopic()
	storage.RetentionStorageMb = 1024
	limit := standaloneTopic()
	limit.PartitioningSettings.PartitionCountLimit = 10 //nolint:staticcheck // SA1019: the deprecated field is what the read refuses
	metering := standaloneTopic()
	metering.MeteringMode = Ydb_Topic.MeteringMode_METERING_MODE_REQUEST_UNITS
	totalRead := standaloneTopic()
	totalRead.PartitionTotalReadSpeedBytesPerSecond = 1
	consumerRead := standaloneTopic()
	consumerRead.PartitionConsumerReadSpeedBytesPerSecond = 1
	metrics := standaloneTopic()
	metrics.MetricsLevel = new(uint32(2))
	codec := standaloneTopic()
	codec.SupportedCodecs = &Ydb_Topic.SupportedCodecs{Codecs: []int32{1, 5}}
	strategy := standaloneTopic()
	strategy.PartitioningSettings.AutoPartitioningSettings.Strategy = 7
	fraction := standaloneTopic()
	fraction.RetentionPeriod = durationpb.New(1500 * time.Millisecond)
	const prefix = `YDB topic /local/app/events: `
	tests := []struct {
		name      string
		described *Ydb_Topic.DescribeTopicResult
		wantErr   string
	}{
		{name: "a field the pinned buffers do not know", described: withUnknown(standaloneTopic()),
			wantErr: "its description carries field 99, which this build of Ptah does not read"},
		{name: "a storage limit", described: storage,
			wantErr: "it holds retention_storage_mb, which Ptah does not model and no YQL statement sets; read a schema that does not hold the topic"},
		{name: "a partition count limit", described: limit,
			wantErr: "it holds partition_count_limit, which Ptah does not model and no YQL statement sets; read a schema that does not hold the topic"},
		{name: "a metering mode", described: metering,
			wantErr: "it holds metering_mode, which Ptah does not model and no YQL statement sets; read a schema that does not hold the topic"},
		{name: "a total read speed", described: totalRead,
			wantErr: "it holds partition_total_read_speed_bytes_per_second, which Ptah does not model and no YQL statement sets; read a schema that does not hold the topic"},
		{name: "a consumer read speed", described: consumerRead,
			wantErr: "it holds partition_consumer_read_speed_bytes_per_second, which Ptah does not model and no YQL statement sets; read a schema that does not hold the topic"},
		{name: "a metrics level", described: metrics,
			wantErr: "it holds metrics_level, which Ptah does not model and no YQL statement sets; read a schema that does not hold the topic"},
		{name: "a codec Ptah does not declare", described: codec,
			wantErr: "its codec list names codec 5, which Ptah does not declare"},
		{name: "a strategy Ptah does not read", described: strategy,
			wantErr: "its auto-partitioning strategy 7 is not one this build of Ptah reads"},
		{name: "a retention that is no whole second", described: fraction,
			wantErr: "its retention period is 1.5s, which is not a whole number of seconds a declaration can hold"},
		{name: "a shared consumer", described: consumerWithField(10, nil),
			wantErr: `consumer "c": it is a shared consumer, which Ptah does not model`},
		{name: "a streaming consumer type with settings", described: consumerWithField(9, []byte{0x08, 0x01}),
			wantErr: `consumer "c": its streaming consumer type carries settings this build of Ptah does not read`},
		{name: "a consumer field the pinned buffers do not know", described: consumerWithField(11, nil),
			wantErr: `consumer "c": its description carries field 11, which this build of Ptah does not read`},
		{name: "a read_from that is no whole second", described: withConsumer(&Ydb_Topic.Consumer{Name: "c",
			ReadFrom: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 500, time.UTC))}),
			wantErr: `consumer "c": its read_from 2026-01-01T00:00:00.0000005Z is not a whole second`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := readTopic(c, test.described)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(prefix+test.wantErr))
			c.Assert(db, qt.IsNil)
		})
	}
}

// A topic of the older persistent queue kind is recorded, not read: no
// statement Ptah writes creates one, and the scheme service lists it apart
// from a topic YQL creates.
func TestReader_PersistentQueueIsRecorded(t *testing.T) {
	c := qt.New(t)
	source := fakeSource{directories: map[string][]*Ydb_Scheme.Entry{
		"/local": {entry("legacy", Ydb_Scheme.Entry_PERS_QUEUE_GROUP)},
	}}

	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(db.Topics, qt.HasLen, 0)
	c.Assert(db.NotDescribed.Describes(coverage.Topic, "legacy"), qt.IsFalse)
}
