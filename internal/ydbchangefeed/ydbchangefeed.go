// Package ydbchangefeed owns the rules of a YDB changefeed: what a declaration
// may say, what YDB refuses or keeps differently from how it was written, the
// statements that add, drop and change one, and when two descriptions of a
// changefeed are the same.
//
// A changefeed is YDB's stream of the changes made to one row table. YDB adds
// it with `ALTER TABLE ... ADD CHANGEFEED <name> WITH (...)` and keeps the
// stream in a topic at `<table>/<name>`, whose retention and consumers change
// in place through `ALTER TOPIC`. No option of the changefeed itself changes
// in place: measured on every YDB line from 25.1.4.7 to 26.2.1.14, `ALTER
// TABLE ... ALTER CHANGEFEED ... SET (MODE = ...)` answers `MODE alter is not
// supported`, and likewise for every other option. A change to one of them
// drops the changefeed and adds it again, and the stream restarts: the records
// nobody read are gone, and every consumer starts over.
//
// The annotation parser, the YAML reader, the renderer, the reader, the
// comparison and the planner each ask this package, so a declaration one of
// them accepts is one the others read the same way.
package ydbchangefeed

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// The attributes that declare a changefeed, spelled as YDB spells its
// options, in lower case. The annotation parser, the YAML reader and the
// annotation registry read these names.
const (
	AttributeName                     = "name"
	AttributeTable                    = "table"
	AttributeMode                     = "mode"
	AttributeFormat                   = "format"
	AttributeVirtualTimestamps        = "virtual_timestamps"
	AttributeResolvedTimestamps       = "resolved_timestamps"
	AttributeInitialScan              = "initial_scan"
	AttributeUserSIDs                 = "user_sids"
	AttributeSchemaChanges            = "schema_changes"
	AttributeTopicMinActivePartitions = "topic_min_active_partitions"
	AttributeTopicAutoPartitioning    = "topic_auto_partitioning"
	AttributeRetentionPeriod          = "retention_period"
)

// The attributes that declare a consumer of a changefeed's topic, spelled as
// YDB spells the consumer settings. AttributeChangefeed names the changefeed
// whose topic the consumer reads.
const (
	AttributeChangefeed         = "changefeed"
	AttributeImportant          = "important"
	AttributeReadFrom           = "read_from"
	AttributeSupportedCodecs    = "supported_codecs"
	AttributeAvailabilityPeriod = "availability_period"
)

// The modes a changefeed takes, as YQL spells them.
const (
	ModeKeysOnly         = "KEYS_ONLY"
	ModeUpdates          = "UPDATES"
	ModeNewImage         = "NEW_IMAGE"
	ModeOldImage         = "OLD_IMAGE"
	ModeNewAndOldImages  = "NEW_AND_OLD_IMAGES"
	FormatJSON           = "JSON"
	FormatDebeziumJSON   = "DEBEZIUM_JSON"
	formatDynamoDBStream = "DYNAMODB_STREAMS_JSON"
)

// Modes lists the modes a changefeed takes.
func Modes() []string {
	return []string{ModeKeysOnly, ModeUpdates, ModeNewImage, ModeOldImage, ModeNewAndOldImages}
}

// Formats lists the formats Ptah writes a changefeed in. YDB has a third,
// DYNAMODB_STREAMS_JSON, which it writes only for a document table: on a row
// table it answers `DYNAMODB_STREAMS_JSON format incompatible with
// non-document table`, measured on 25.1.4.7 and 26.2.1.14, and Ptah creates
// no document table.
func Formats() []string {
	return []string{FormatJSON, FormatDebeziumJSON}
}

// The codecs a topic consumer can declare, as YQL spells them. Codecs reads
// the numbers DescribeTopic reports them with.
var codecNumbers = map[string]int32{
	"raw":    1,
	"gzip":   2,
	"lzop":   3,
	"zstd":   4,
	"custom": 10000,
}

// Codecs lists the codec names a consumer takes, in the order YDB numbers
// them.
func Codecs() []string {
	return []string{"raw", "gzip", "lzop", "zstd", "custom"}
}

// CodecName is the name of the codec DescribeTopic reports as number, and
// false for a number no codec name Ptah declares carries.
func CodecName(number int32) (string, bool) {
	for name, value := range codecNumbers {
		if value == number {
			return name, true
		}
	}
	return "", false
}

// DefaultRetentionSeconds is the retention YDB gives a changefeed's topic
// when the declaration names none: 24 hours, measured on 25.1.4.7 and
// 26.2.1.14 as `retentionPeriod: 86400s`.
const DefaultRetentionSeconds = 24 * 60 * 60

// DeclarationError is an attribute whose value a declaration cannot carry.
type DeclarationError struct {
	// Attribute is the attribute's name.
	Attribute string
	// Value is the value it was given.
	Value string
	// Reason says what the attribute takes.
	Reason string
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Attribute, e.Value, e.Reason)
}

// ParseDeclaration reads one changefeed out of values, keyed by attribute
// name, and ignores every key it does not name. The changefeed it returns has
// no consumers; a consumer is declared on its own.
//
// Each value is checked for the form YDB takes, so a typo is refused where it
// was written: a name, a mode and a format are required, since YDB refuses a
// changefeed without one (`Invalid changefeed mode`); a mode or a format may
// be written in any case and is kept in capitals, as YDB folds them; a switch
// is `true` or `false`; an interval is an ISO 8601 duration of whole seconds;
// and a partition count is at least 1.
func ParseDeclaration(values map[string]string) (ydbschema.ChangefeedSpec, error) {
	spec := ydbschema.ChangefeedSpec{Name: strings.TrimSpace(values[AttributeName])}
	if spec.Name == "" {
		return ydbschema.ChangefeedSpec{}, &DeclarationError{Attribute: AttributeName, Reason: "a changefeed needs a name"}
	}
	var err error
	if spec.Mode, err = choice(values, AttributeMode, Modes()); err != nil {
		return ydbschema.ChangefeedSpec{}, err
	}
	if spec.Format, err = format(values); err != nil {
		return ydbschema.ChangefeedSpec{}, err
	}
	switches := []struct {
		attribute string
		target    *bool
	}{
		{AttributeVirtualTimestamps, &spec.VirtualTimestamps},
		{AttributeInitialScan, &spec.InitialScan},
		{AttributeUserSIDs, &spec.UserSIDs},
		{AttributeSchemaChanges, &spec.SchemaChanges},
		{AttributeTopicAutoPartitioning, &spec.TopicAutoPartitioning},
	}
	for _, sw := range switches {
		if *sw.target, err = boolean(values, sw.attribute); err != nil {
			return ydbschema.ChangefeedSpec{}, err
		}
	}
	if spec.ResolvedTimestamps, err = interval(values, AttributeResolvedTimestamps); err != nil {
		return ydbschema.ChangefeedSpec{}, err
	}
	if spec.RetentionPeriod, err = interval(values, AttributeRetentionPeriod); err != nil {
		return ydbschema.ChangefeedSpec{}, err
	}
	if raw, ok := present(values, AttributeTopicMinActivePartitions); ok {
		count, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil || count == 0 {
			return ydbschema.ChangefeedSpec{}, &DeclarationError{Attribute: AttributeTopicMinActivePartitions, Value: raw,
				Reason: "takes a count of at least 1 (`topic_min_active_partitions must be greater than 0`)"}
		}
		spec.TopicMinActivePartitions = count
	}
	return spec, nil
}

// ParseConsumer reads one consumer out of values, keyed by attribute name,
// and ignores every key it does not name, the changefeed it belongs to
// included.
//
// A codec may be written in any case and is kept in lower case, as YDB folds
// them; read_from is an RFC 3339 time of whole seconds, kept in UTC, because
// YDB keeps whole seconds in UTC (measured: `2026-01-01T00:00:00.5Z` reads
// back as `2026-01-01T00:00:00Z`, and `2026-01-01T03:00:00+03:00` as
// `2026-01-01T00:00:00Z`); and a consumer is not both important and limited
// by an availability period, which YDB refuses (`has both an important flag
// and a limited availability_period, which are mutually exclusive`).
func ParseConsumer(values map[string]string) (ydbtopic.ConsumerSpec, error) {
	consumer := ydbtopic.ConsumerSpec{Name: strings.TrimSpace(values[AttributeName])}
	if consumer.Name == "" {
		return ydbtopic.ConsumerSpec{}, &DeclarationError{Attribute: AttributeName, Reason: "a consumer needs a name"}
	}
	if strings.Contains(consumer.Name, "/") {
		return ydbtopic.ConsumerSpec{}, &DeclarationError{Attribute: AttributeName, Value: consumer.Name,
			Reason: "a consumer's name cannot hold a slash (`consumer ... has illegal symbols`)"}
	}
	var err error
	if consumer.Important, err = boolean(values, AttributeImportant); err != nil {
		return ydbtopic.ConsumerSpec{}, err
	}
	if raw, ok := present(values, AttributeReadFrom); ok {
		if consumer.ReadFrom, err = NormalizeReadFrom(raw); err != nil {
			return ydbtopic.ConsumerSpec{}, &DeclarationError{Attribute: AttributeReadFrom, Value: raw, Reason: err.Error()}
		}
	}
	if raw, ok := present(values, AttributeSupportedCodecs); ok {
		if consumer.SupportedCodecs, err = parseCodecs(raw); err != nil {
			return ydbtopic.ConsumerSpec{}, err
		}
	}
	if consumer.AvailabilityPeriod, err = interval(values, AttributeAvailabilityPeriod); err != nil {
		return ydbtopic.ConsumerSpec{}, err
	}
	if consumer.Important && consumer.AvailabilityPeriod != "" {
		return ydbtopic.ConsumerSpec{}, &DeclarationError{Attribute: AttributeAvailabilityPeriod,
			Value: consumer.AvailabilityPeriod, Reason: "YDB keeps every unread record for an important consumer, " +
				"so it takes no availability period as well (`has both an important flag and a limited " +
				"availability_period, which are mutually exclusive`)"}
	}
	return consumer, nil
}

// present returns the trimmed value of attribute and whether one was given.
func present(values map[string]string, attribute string) (string, bool) {
	raw, ok := values[attribute]
	if !ok {
		return "", false
	}
	return strings.TrimSpace(raw), true
}

// choice reads a required attribute that takes one of allowed, in any case.
func choice(values map[string]string, attribute string, allowed []string) (string, error) {
	raw, _ := present(values, attribute)
	upper := strings.ToUpper(raw)
	if !slices.Contains(allowed, upper) {
		return "", &DeclarationError{Attribute: attribute, Value: raw,
			Reason: "takes one of " + strings.Join(allowed, ", ")}
	}
	return upper, nil
}

// format reads the format, and names why DYNAMODB_STREAMS_JSON is refused.
func format(values map[string]string) (string, error) {
	raw, _ := present(values, AttributeFormat)
	if strings.EqualFold(raw, formatDynamoDBStream) {
		return "", &DeclarationError{Attribute: AttributeFormat, Value: raw,
			Reason: "YDB writes DYNAMODB_STREAMS_JSON only for a document table, which Ptah does not create " +
				"(`DYNAMODB_STREAMS_JSON format incompatible with non-document table`)"}
	}
	return choice(values, AttributeFormat, Formats())
}

// boolean reads an optional switch.
func boolean(values map[string]string, attribute string) (bool, error) {
	raw, ok := present(values, attribute)
	if !ok {
		return false, nil
	}
	switch strings.ToLower(raw) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, &DeclarationError{Attribute: attribute, Value: raw, Reason: "takes true or false"}
	}
}

// interval reads an optional ISO 8601 interval, checked with [Seconds].
func interval(values map[string]string, attribute string) (string, error) {
	raw, ok := present(values, attribute)
	if !ok {
		return "", nil
	}
	if _, err := Seconds(raw); err != nil {
		return "", &DeclarationError{Attribute: attribute, Value: raw, Reason: err.Error()}
	}
	return raw, nil
}

// parseCodecs reads a comma-separated codec list.
func parseCodecs(raw string) ([]string, error) {
	var codecs []string
	for part := range strings.SplitSeq(raw, ",") {
		codec := strings.ToLower(strings.TrimSpace(part))
		if _, known := codecNumbers[codec]; !known {
			return nil, &DeclarationError{Attribute: AttributeSupportedCodecs, Value: raw,
				Reason: "takes a comma-separated list of " + strings.Join(Codecs(), ", ")}
		}
		if slices.Contains(codecs, codec) {
			return nil, &DeclarationError{Attribute: AttributeSupportedCodecs, Value: raw,
				Reason: "names codec " + codec + " twice"}
		}
		codecs = append(codecs, codec)
	}
	return codecs, nil
}

// NormalizeReadFrom reads an RFC 3339 time and writes it the way YDB keeps
// it: in UTC, to the second. A time with a fraction of a second is refused,
// because YDB drops the fraction and the consumer would read from a moment
// other than the one declared.
func NormalizeReadFrom(raw string) (string, error) {
	instant, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("takes an RFC 3339 time such as 2026-01-01T00:00:00Z")
	}
	if instant.Nanosecond() != 0 {
		return "", fmt.Errorf("YDB keeps whole seconds, and drops the fraction of this one")
	}
	return FormatReadFrom(instant), nil
}

// FormatReadFrom writes an instant as a consumer's read_from: RFC 3339 in
// UTC, or empty for the start of the Unix epoch, which is what YDB reports
// for a consumer that declared none.
func FormatReadFrom(instant time.Time) string {
	if instant.Unix() == 0 {
		return ""
	}
	return instant.UTC().Format(time.RFC3339)
}

// TopicPath is the path of the topic changefeed name keeps for table, a
// canonical table reference, written as one quoted YDB path:
// `<directory>/<table>/<changefeed>`.
func TopicPath(table, name string) string {
	ref, ok := tableref.Parse(table)
	if !ok {
		return sqlident.Quote(platform.YDB, table+"/"+name)
	}
	return sqlident.Qualified(platform.YDB, ref.Schema, ref.Name+"/"+name)
}

// Refusal says why a changefeed cannot be written on a target: Key is the
// capability it needs and the target lacks, or empty when YDB refuses it
// whatever the target, and Reason then says why.
type Refusal struct {
	// Subject names what is refused.
	Subject string
	// Key is the capability the declaration needs, empty for a refusal YDB
	// makes on every line.
	Key capability.Capability
	// Reason is why YDB refuses it, for a refusal without a key.
	Reason string
}

// Check reports why spec cannot be written on a target holding caps, or nil
// when it can. table names the table in the subject.
//
// It holds what a declaration's parse cannot know: the options a line lacks,
// each through its capability key, and the shapes YDB refuses on every line,
// measured on 25.1.4.7 and 26.2.1.14 -- DEBEZIUM_JSON in UPDATES mode
// (`DEBEZIUM_JSON format incompatible with specified stream mode`), and with
// virtual or resolved timestamps or schema change records
// (`VIRTUAL_TIMESTAMPS incompatible with specified stream format`, and the
// same for RESOLVED_TIMESTAMPS and, on 26.2.1.14, SCHEMA_CHANGES), and a
// disabled changefeed, since no statement disables one. A spec built by hand
// is held to the parse's rules too, because nothing else checks it.
func Check(table string, spec ydbschema.ChangefeedSpec, caps capability.Capabilities) *Refusal {
	subject := fmt.Sprintf("changefeed %q of %s", spec.Name, tableref.Phrase(table))
	if !caps.Has(capability.Changefeeds) {
		return &Refusal{Subject: subject, Key: capability.Changefeeds}
	}
	if reason := shapeRefusal(spec); reason != "" {
		return &Refusal{Subject: subject, Reason: reason}
	}
	options := []struct {
		set  bool
		key  capability.Capability
		what string
	}{
		{spec.UserSIDs, capability.ChangefeedUserSIDs, "USER_SIDS"},
		{spec.SchemaChanges, capability.ChangefeedSchemaChanges, "SCHEMA_CHANGES"},
		{spec.TopicAutoPartitioning, capability.ChangefeedTopicAutoPartitioning, "TOPIC_AUTO_PARTITIONING"},
	}
	for _, option := range options {
		if option.set && !caps.Has(option.key) {
			return &Refusal{Subject: subject + " takes " + option.what, Key: option.key}
		}
	}
	names := make(map[string]bool, len(spec.Consumers))
	for _, consumer := range spec.Consumers {
		consumerSubject := fmt.Sprintf("consumer %q of %s", consumer.Name, subject)
		if names[consumer.Name] {
			return &Refusal{Subject: subject, Reason: fmt.Sprintf("two of its consumers are named %q, and YDB "+
				"names a consumer once per topic (`Duplicate consumer name`)", consumer.Name)}
		}
		names[consumer.Name] = true
		if reason := consumerRefusal(consumer); reason != "" {
			return &Refusal{Subject: consumerSubject, Reason: reason}
		}
		if consumer.AvailabilityPeriod != "" && !caps.Has(capability.TopicConsumerAvailabilityPeriod) {
			return &Refusal{Subject: consumerSubject + " takes availability_period",
				Key: capability.TopicConsumerAvailabilityPeriod}
		}
	}
	return nil
}

// shapeRefusal says why YDB refuses spec on every line, or is empty.
func shapeRefusal(spec ydbschema.ChangefeedSpec) string {
	switch {
	case strings.TrimSpace(spec.Name) == "" || strings.Contains(spec.Name, "/"):
		return "a changefeed needs a name without a slash (`symbol '/' is not allowed in the path part`)"
	case !slices.Contains(Modes(), strings.ToUpper(spec.Mode)):
		return fmt.Sprintf("its mode %q is none of %s", spec.Mode, strings.Join(Modes(), ", "))
	case !slices.Contains(Formats(), strings.ToUpper(spec.Format)):
		return fmt.Sprintf("its format %q is none of %s", spec.Format, strings.Join(Formats(), ", "))
	case strings.EqualFold(spec.Format, FormatDebeziumJSON) && strings.EqualFold(spec.Mode, ModeUpdates):
		return "YDB writes DEBEZIUM_JSON in every mode but UPDATES (`DEBEZIUM_JSON format incompatible with " +
			"specified stream mode`)"
	case strings.EqualFold(spec.Format, FormatDebeziumJSON) &&
		(spec.VirtualTimestamps || spec.ResolvedTimestamps != "" || spec.SchemaChanges):
		return "YDB writes DEBEZIUM_JSON with no virtual timestamps, resolved timestamps or schema change records " +
			"(`VIRTUAL_TIMESTAMPS incompatible with specified stream format`, and likewise for the other two)"
	case spec.Disabled:
		return "YDB has no statement that disables a changefeed (`ALTER CHANGEFEED ... DISABLE` answers " +
			"`Name not found: quote` on every measured line)"
	}
	for _, period := range []struct{ attribute, value string }{
		{AttributeResolvedTimestamps, spec.ResolvedTimestamps},
		{AttributeRetentionPeriod, spec.RetentionPeriod},
	} {
		if period.value == "" {
			continue
		}
		if _, err := Seconds(period.value); err != nil {
			return fmt.Sprintf("its %s %q: %v", period.attribute, period.value, err)
		}
	}
	return ""
}

// consumerRefusal says why YDB refuses consumer on every line, or is empty.
func consumerRefusal(consumer ydbtopic.ConsumerSpec) string {
	values := map[string]string{AttributeName: consumer.Name}
	if consumer.Important {
		values[AttributeImportant] = "true"
	}
	if consumer.ReadFrom != "" {
		values[AttributeReadFrom] = consumer.ReadFrom
	}
	if len(consumer.SupportedCodecs) > 0 {
		values[AttributeSupportedCodecs] = strings.Join(consumer.SupportedCodecs, ",")
	}
	if consumer.AvailabilityPeriod != "" {
		values[AttributeAvailabilityPeriod] = consumer.AvailabilityPeriod
	}
	if _, err := ParseConsumer(values); err != nil {
		return err.Error()
	}
	return ""
}

// NameRefusal says why the changefeeds and the indexes of one table cannot
// all be created, or is empty. A table's changefeeds and indexes are named in
// one namespace, the table's path: measured on 25.1.4.7 and 26.2.1.14, a
// changefeed named after an index answers `unexpected path type ...
// EPathTypeTableIndex`, and an index named after a changefeed the other way
// round.
func NameRefusal(changefeeds []ydbschema.ChangefeedSpec, indexes []string) string {
	seen := make(map[string]bool, len(changefeeds))
	for _, changefeed := range changefeeds {
		if seen[changefeed.Name] {
			return fmt.Sprintf("two of its changefeeds are named %q", changefeed.Name)
		}
		seen[changefeed.Name] = true
		if slices.Contains(indexes, changefeed.Name) {
			return fmt.Sprintf("changefeed %q has the name of one of its indexes, and YDB keeps both under the "+
				"table's path", changefeed.Name)
		}
	}
	return ""
}

// KeyRefusal says why YDB refuses spec on a table whose first key column has
// type firstKeyType, or is empty. A topic declared with more than one
// partition splits the stream by the first key column, which YDB takes only
// as Uint32 or Uint64: measured on 25.1.4.7 and 26.2.1.14,
// TOPIC_MIN_ACTIVE_PARTITIONS = 2 on a Utf8 key answers `Unsupported first key
// column type Utf8, only Uint32 and Uint64 are supported`.
func KeyRefusal(spec ydbschema.ChangefeedSpec, firstKeyType string) string {
	if spec.TopicMinActivePartitions <= 1 || firstKeyType == "" {
		return ""
	}
	if firstKeyType == "Uint32" || firstKeyType == "Uint64" {
		return ""
	}
	return fmt.Sprintf("its topic starts with %d partitions, which YDB splits by the first key column, and takes "+
		"that column only as Uint32 or Uint64, not %s", spec.TopicMinActivePartitions, firstKeyType)
}
