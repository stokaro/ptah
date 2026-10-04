package ydb

import (
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/durationpb"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbchangefeed"
)

// changefeedModes and changefeedFormats spell the values DescribeTable
// reports as YQL writes them. A value outside them is one Ptah does not
// write -- DYNAMODB_STREAMS_JSON, which YDB writes only for a document table,
// or one a newer YDB added -- and the changefeed is recorded rather than read.
var (
	changefeedModes = map[Ydb_Table.ChangefeedMode_Mode]string{
		Ydb_Table.ChangefeedMode_MODE_KEYS_ONLY:          ydbchangefeed.ModeKeysOnly,
		Ydb_Table.ChangefeedMode_MODE_UPDATES:            ydbchangefeed.ModeUpdates,
		Ydb_Table.ChangefeedMode_MODE_NEW_IMAGE:          ydbchangefeed.ModeNewImage,
		Ydb_Table.ChangefeedMode_MODE_OLD_IMAGE:          ydbchangefeed.ModeOldImage,
		Ydb_Table.ChangefeedMode_MODE_NEW_AND_OLD_IMAGES: ydbchangefeed.ModeNewAndOldImages,
	}
	changefeedFormats = map[Ydb_Table.ChangefeedFormat_Format]string{
		Ydb_Table.ChangefeedFormat_FORMAT_JSON:          ydbchangefeed.FormatJSON,
		Ydb_Table.ChangefeedFormat_FORMAT_DEBEZIUM_JSON: ydbchangefeed.FormatDebeziumJSON,
	}
)

// The fields of ChangefeedDescription the pinned protocol buffers do not
// model, by the number ydb_table.proto gives each.
const (
	changefeedUserSIDsField protowire.Number = 11
	changefeedTraceIDsField protowire.Number = 12
)

// consumerServiceType is the attribute YDB gives every consumer it creates
// through ALTER TOPIC, measured on 25.1.4.7 and 26.2.1.14; any other
// attribute is one Ptah does not model.
var consumerServiceType = map[string]string{"_service_type": "data-streams"}

// changefeeds reads a table's changefeeds, each with the retention and the
// consumers of its topic, and records as not described every one holding
// something Ptah does not model, so a plan neither drops nor changes it.
func (r *Reader) changefeeds(
	ctx context.Context,
	source Source,
	schema, table string,
	described *Ydb_Table.DescribeTableResult,
) ([]ast.ChangefeedSpec, []coverage.Object, error) {
	var read []ast.ChangefeedSpec
	var records []coverage.Object
	for _, feed := range described.GetChangefeeds() {
		spec, modeled, err := r.changefeed(ctx, source, schema, table, feed)
		if err != nil {
			return nil, nil, fmt.Errorf("changefeed %q: %w", feed.GetName(), err)
		}
		if !modeled {
			records = append(records, coverage.Object{
				Kind:       coverage.Changefeed,
				Name:       tableref.Canonical(schema, table+"/"+feed.GetName()),
				Reason:     coverage.Unsupported,
				Provenance: coverage.Observed,
			})
			continue
		}
		read = append(read, spec)
	}
	slices.SortFunc(read, func(a, b ast.ChangefeedSpec) int { return strings.Compare(a.Name, b.Name) })
	return read, records, nil
}

// changefeed reads one changefeed and its topic. It reports false for one
// holding something Ptah does not model: a mode, a format or a state it does
// not know, an AWS region, attributes, trace identifiers, a field the pinned
// protocol buffers do not know, or a topic or consumer setting it does not
// read.
func (r *Reader) changefeed(
	ctx context.Context,
	source Source,
	schema, table string,
	feed *Ydb_Table.ChangefeedDescription,
) (ast.ChangefeedSpec, bool, error) {
	mode, knownMode := changefeedModes[feed.GetMode()]
	format, knownFormat := changefeedFormats[feed.GetFormat()]
	userSIDs, traceIDs, knownFields := changefeedUnknownFields(feed)
	if !knownMode || !knownFormat || !knownFields || traceIDs ||
		feed.GetAwsRegion() != "" || len(feed.GetAttributes()) > 0 {
		return ast.ChangefeedSpec{}, false, nil
	}
	spec := ast.ChangefeedSpec{
		Name:              feed.GetName(),
		Mode:              mode,
		Format:            format,
		VirtualTimestamps: feed.GetVirtualTimestamps(),
		// An initial scan leaves its progress on the changefeed for good,
		// measured on both lines: `initialScanProgress: {partsTotal: 1,
		// partsCompleted: 1}` once the scan ends, and nothing on a
		// changefeed that asked for none.
		InitialScan:   feed.GetInitialScanProgress() != nil,
		UserSIDs:      userSIDs,
		SchemaChanges: feed.GetSchemaChanges(),
	}
	switch feed.GetState() {
	case Ydb_Table.ChangefeedDescription_STATE_ENABLED, Ydb_Table.ChangefeedDescription_STATE_INITIAL_SCAN:
	case Ydb_Table.ChangefeedDescription_STATE_DISABLED:
		spec.Disabled = true
	default:
		return ast.ChangefeedSpec{}, false, nil
	}
	resolved, whole := durationSeconds(feed.GetResolvedTimestampsInterval())
	if !whole {
		return ast.ChangefeedSpec{}, false, nil
	}
	if resolved > 0 {
		spec.ResolvedTimestamps = ydbchangefeed.FormatSeconds(resolved)
	}
	topic, err := source.DescribeTopic(ctx, r.absolute(schema, path.Join(table, feed.GetName())))
	if err != nil {
		return ast.ChangefeedSpec{}, false, err
	}
	modeled := readTopic(&spec, topic)
	return spec, modeled, nil
}

// changefeedUnknownFields reads the fields of a changefeed description the
// pinned protocol buffers do not model: user_sids and trace_ids, booleans the
// description carries from 26.1 on. It reports false where a field it does not
// know arrives, or one of these in a form other than a boolean.
func changefeedUnknownFields(feed *Ydb_Table.ChangefeedDescription) (userSIDs, traceIDs, known bool) {
	unknown := feed.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		number, wireType, length := protowire.ConsumeTag(unknown)
		if length < 0 {
			return false, false, false
		}
		unknown = unknown[length:]
		if wireType != protowire.VarintType ||
			(number != changefeedUserSIDsField && number != changefeedTraceIDsField) {
			return false, false, false
		}
		value, valueLength := protowire.ConsumeVarint(unknown)
		if valueLength < 0 {
			return false, false, false
		}
		unknown = unknown[valueLength:]
		switch number {
		case changefeedUserSIDsField:
			userSIDs = value != 0
		default:
			traceIDs = value != 0
		}
	}
	return userSIDs, traceIDs, true
}

// readTopic reads the retention, the partitions and the consumers of a
// changefeed's topic into spec. It reports false where the topic holds
// something Ptah does not model.
//
// The retention is reported where it is not YDB's 24 hours, and a starting
// partition count where it is above one: a changefeed that declared none
// starts with one partition per partition of the table, which a table created
// without settings has one of.
func readTopic(spec *ast.ChangefeedSpec, topic *Ydb_Topic.DescribeTopicResult) bool {
	retention, whole := durationSeconds(topic.GetRetentionPeriod())
	if !whole {
		return false
	}
	if retention != 0 && retention != ydbchangefeed.DefaultRetentionSeconds {
		spec.RetentionPeriod = ydbchangefeed.FormatSeconds(retention)
	}
	partitioning := topic.GetPartitioningSettings()
	if minimum := partitioning.GetMinActivePartitions(); minimum > 1 {
		spec.TopicMinActivePartitions = uint64(minimum)
	}
	switch partitioning.GetAutoPartitioningSettings().GetStrategy() {
	case Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_UNSPECIFIED,
		Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_DISABLED:
	case Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_SCALE_UP:
		spec.TopicAutoPartitioning = true
	default:
		return false
	}
	for _, consumer := range topic.GetConsumers() {
		read, ok := readConsumer(consumer)
		if !ok {
			return false
		}
		spec.Consumers = append(spec.Consumers, read)
	}
	// By name, as the changefeeds are: the server lists consumers in the order
	// they were added, which a consumer dropped and added again changes.
	slices.SortFunc(spec.Consumers, func(a, b ast.TopicConsumerSpec) int { return strings.Compare(a.Name, b.Name) })
	return true
}

// readConsumer reads one consumer, and reports false for one holding an
// attribute, a codec or a field Ptah does not model.
func readConsumer(consumer *Ydb_Topic.Consumer) (ast.TopicConsumerSpec, bool) {
	attributes := consumer.GetAttributes()
	if !streamingConsumer(consumer) || (len(attributes) > 0 && !maps.Equal(attributes, consumerServiceType)) {
		return ast.TopicConsumerSpec{}, false
	}
	read := ast.TopicConsumerSpec{Name: consumer.GetName(), Important: consumer.GetImportant()}
	if from := consumer.GetReadFrom(); from != nil {
		read.ReadFrom = ydbchangefeed.FormatReadFrom(from.AsTime())
	}
	for _, number := range consumer.GetSupportedCodecs().GetCodecs() {
		name, known := ydbchangefeed.CodecName(number)
		if !known {
			return ast.TopicConsumerSpec{}, false
		}
		read.SupportedCodecs = append(read.SupportedCodecs, name)
	}
	availability, whole := durationSeconds(consumer.GetAvailabilityPeriod())
	if !whole {
		return ast.TopicConsumerSpec{}, false
	}
	if availability > 0 {
		read.AvailabilityPeriod = ydbchangefeed.FormatSeconds(availability)
	}
	return read, true
}

// consumerStreamingType is the field of Consumer the pinned protocol buffers
// do not model that marks an ordinary consumer: the `streaming_consumer_type`
// arm of its `consumer_type` oneof, an empty message. 26.2.1.14 sends it on
// every consumer ALTER TOPIC creates, and 25.1.4.7 sends none.
const consumerStreamingType protowire.Number = 9

// streamingConsumer reports whether the fields of consumer the pinned
// protocol buffers do not model say no more than that it is an ordinary,
// streaming consumer. Any other one -- a shared consumer, which 26.2.1.14
// keeps behind a flag (`shared consumers is disabled`), or a read speed limit
// -- is a setting Ptah does not model.
func streamingConsumer(consumer *Ydb_Topic.Consumer) bool {
	unknown := consumer.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		number, wireType, length := protowire.ConsumeTag(unknown)
		if length < 0 || number != consumerStreamingType || wireType != protowire.BytesType {
			return false
		}
		unknown = unknown[length:]
		value, valueLength := protowire.ConsumeBytes(unknown)
		if valueLength < 0 || len(value) > 0 {
			return false
		}
		unknown = unknown[valueLength:]
	}
	return true
}

// durationSeconds reads a duration in whole seconds, zero for none, and
// reports false for a negative one or one with a fraction, which no
// changefeed setting holds.
func durationSeconds(duration *durationpb.Duration) (uint64, bool) {
	if duration == nil {
		return 0, true
	}
	seconds := duration.GetSeconds()
	if seconds < 0 || duration.GetNanos() != 0 {
		return 0, false
	}
	return uint64(seconds), true
}
