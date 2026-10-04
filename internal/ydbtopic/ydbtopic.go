// Package ydbtopic owns the rules of a standalone YDB topic and its
// consumers: what a declaration may say, what YDB keeps differently from how
// it was written, when two descriptions of a topic are the same, and the
// statements that create, change and drop one.
//
// A topic is a persistent message queue at a path of YDB's scheme tree, and a
// consumer is a named reader that keeps its own position in it. YDB creates a
// topic with its consumers and settings in one statement, `CREATE TOPIC <path>
// (CONSUMER ...) WITH (...)`, and changes both in place through `ALTER TOPIC`.
// Several of its answers shape the rules here, each measured on local-ydb
// 25.1.4.7 and 26.2.1.14 alike:
//
//   - YDB accepts some settings and keeps nothing: `retention_storage_mb`,
//     `partition_count_limit`, `metering_mode` outside a serverless database,
//     `max_active_partitions` while auto-partitioning is disabled, a codec it
//     does not know, and on 26.2 a consumer of `type = 'shared'`, which it
//     creates as an ordinary one. A declaration of any of them is refused.
//   - No setting can be reset: `RESET (...)` changes nothing on 26.2.1.14 and
//     is a parse error on 25.1.4.7. A statement that changes a topic names
//     the value it is to hold.
//   - A setting nobody declared takes a value that depends on how the topic
//     was made: the write burst equals the write speed a topic was created
//     with and does not follow a later change of it, and the auto-partitioning
//     thresholds are 90% and 30% for a topic created with a strategy and 80%
//     and 20% for one that was given a strategy later. So the comparison
//     resolves an undeclared setting to the value a new topic takes, and a
//     change names every setting of the topic.
//   - The partition count only grows (`Invalid total groups count specified`),
//     and auto-partitioning, once enabled, cannot be disabled (`Can't disable
//     auto partitioning.`). A change asking for either is refused.
//   - A consumer's codec list cannot be emptied (`unknown codec found or
//     codecs list is malformed`), so that change drops the consumer and adds
//     it again, and the consumer's position is lost.
//
// The annotation parser, the YAML reader, the renderer, the reader, the
// comparison and the planner each ask this package, so a declaration one of
// them accepts is one the others read the same way.
package ydbtopic

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbtype"
)

// The attributes that declare a topic, spelled as YDB spells its settings.
// The annotation parser, the YAML reader and the annotation registry read
// these names.
const (
	AttributeName                   = "name"
	AttributeSchema                 = "schema"
	AttributeMinActivePartitions    = "min_active_partitions"
	AttributeMaxActivePartitions    = "max_active_partitions"
	AttributeStrategy               = "auto_partitioning_strategy"
	AttributeUpUtilizationPercent   = "auto_partitioning_up_utilization_percent"
	AttributeDownUtilizationPercent = "auto_partitioning_down_utilization_percent"
	AttributeStabilizationWindow    = "auto_partitioning_stabilization_window"
	AttributeRetentionPeriod        = "retention_period"
	AttributeWriteSpeed             = "partition_write_speed_bytes_per_second"
	AttributeWriteBurst             = "partition_write_burst_bytes"
	AttributeSupportedCodecs        = "supported_codecs"
)

// The attributes that declare a consumer, spelled as YDB spells the consumer
// settings. AttributeTopic names the topic the consumer reads, in the
// directory AttributeSchema names.
const (
	AttributeTopic              = "topic"
	AttributeImportant          = "important"
	AttributeReadFrom           = "read_from"
	AttributeAvailabilityPeriod = "availability_period"
)

// The auto-partitioning strategies, as YQL spells them.
const (
	StrategyDisabled       = "disabled"
	StrategyScaleUp        = "scale_up"
	StrategyScaleUpAndDown = "scale_up_and_down"
	StrategyPaused         = "paused"
)

// Strategies lists the auto-partitioning strategies a topic takes.
func Strategies() []string {
	return []string{StrategyDisabled, StrategyScaleUp, StrategyScaleUpAndDown, StrategyPaused}
}

// The values YDB gives a new topic for a setting the statement does not name,
// measured on 25.1.4.7 and 26.2.1.14 by describing a topic created without
// it: one partition, auto-partitioning disabled, messages kept for 24 hours,
// a write quota of 1 MiB per second per partition with a burst of the same
// size, and, for a topic created with a strategy, a maximum equal to the
// minimum, splitting at 90%, merging at 30% and a stabilization window of
// five minutes.
const (
	DefaultMinActivePartitions        = 1
	DefaultRetentionSeconds           = 24 * 60 * 60
	DefaultWriteSpeed                 = 1 << 20
	DefaultUpUtilizationPercent       = 90
	DefaultDownUtilizationPercent     = 30
	DefaultStabilizationWindowSeconds = 5 * 60
)

// codecNumbers maps the codecs a topic or a consumer takes, as YQL spells
// them, to the numbers DescribeTopic reports them with. YDB has one more,
// kafka_batch, which `CREATE TOPIC ... WITH (supported_codecs =
// 'kafka_batch')` accepts and keeps as no codec at all, so it is not one a
// declaration can name.
var codecNumbers = map[string]int32{
	"raw":    1,
	"gzip":   2,
	"lzop":   3,
	"zstd":   4,
	"custom": 10000,
}

// Codecs lists the codec names a topic and a consumer take, in the order YDB
// numbers them.
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
	if e.Value == "" {
		return fmt.Sprintf("invalid %s: %s", e.Attribute, e.Reason)
	}
	return fmt.Sprintf("invalid %s %q: %s", e.Attribute, e.Value, e.Reason)
}

// ParseTopic reads a topic's settings out of values, keyed by attribute name,
// and ignores every key it does not name: the topic's name, its directory and
// its comment are the caller's.
//
// Each value is checked for the form YDB keeps, so a typo or a value YDB would
// change is refused where it was written rather than on the server. The
// settings that shape auto-partitioning -- the maximum, the thresholds and
// the window -- are refused without a strategy that uses them: YDB keeps
// `max_active_partitions` as nothing while auto-partitioning is disabled
// (measured: `max_active_partitions = 5` reads back as 1), and the others
// have no effect then.
func ParseTopic(values map[string]string) (ast.TopicSpec, error) {
	var spec ast.TopicSpec
	var err error
	counts := []struct {
		attribute string
		target    *uint64
	}{
		{AttributeMinActivePartitions, &spec.MinActivePartitions},
		{AttributeMaxActivePartitions, &spec.MaxActivePartitions},
		{AttributeWriteSpeed, &spec.PartitionWriteSpeedBytesPerSecond},
		{AttributeWriteBurst, &spec.PartitionWriteBurstBytes},
	}
	for _, count := range counts {
		if *count.target, err = positive(values, count.attribute); err != nil {
			return ast.TopicSpec{}, err
		}
	}
	if raw, ok := present(values, AttributeStrategy); ok {
		strategy := strings.ToLower(raw)
		if !slices.Contains(Strategies(), strategy) {
			return ast.TopicSpec{}, &DeclarationError{Attribute: AttributeStrategy, Value: raw,
				Reason: "takes one of " + strings.Join(Strategies(), ", ")}
		}
		spec.AutoPartitioningStrategy = strategy
	}
	if spec.AutoPartitioningUpUtilizationPercent, err = percent(values, AttributeUpUtilizationPercent); err != nil {
		return ast.TopicSpec{}, err
	}
	if spec.AutoPartitioningDownUtilizationPercent, err = percent(values, AttributeDownUtilizationPercent); err != nil {
		return ast.TopicSpec{}, err
	}
	if spec.AutoPartitioningStabilizationWindow, err = interval(values, AttributeStabilizationWindow); err != nil {
		return ast.TopicSpec{}, err
	}
	if spec.RetentionPeriod, err = interval(values, AttributeRetentionPeriod); err != nil {
		return ast.TopicSpec{}, err
	}
	if raw, ok := present(values, AttributeSupportedCodecs); ok {
		if spec.SupportedCodecs, err = parseCodecs(raw); err != nil {
			return ast.TopicSpec{}, err
		}
	}
	if reason := settingsRefusal(spec); reason != nil {
		return ast.TopicSpec{}, reason
	}
	return spec, nil
}

// settingsRefusal says why YDB would not keep spec's settings as written, or
// is nil.
func settingsRefusal(spec ast.TopicSpec) *DeclarationError {
	if !autoPartitioned(spec.AutoPartitioningStrategy) {
		shaping := []struct {
			attribute string
			set       bool
		}{
			{AttributeMaxActivePartitions, spec.MaxActivePartitions != 0},
			{AttributeUpUtilizationPercent, spec.AutoPartitioningUpUtilizationPercent != 0},
			{AttributeDownUtilizationPercent, spec.AutoPartitioningDownUtilizationPercent != 0},
			{AttributeStabilizationWindow, spec.AutoPartitioningStabilizationWindow != ""},
		}
		for _, setting := range shaping {
			if setting.set {
				return &DeclarationError{Attribute: setting.attribute,
					Reason: "it shapes auto-partitioning, which this topic does not enable; declare " +
						AttributeStrategy + " as " + StrategyScaleUp + ", " + StrategyScaleUpAndDown + " or " +
						StrategyPaused}
			}
		}
	}
	minimum := max(spec.MinActivePartitions, DefaultMinActivePartitions)
	if spec.MaxActivePartitions != 0 && spec.MaxActivePartitions < minimum {
		return &DeclarationError{Attribute: AttributeMaxActivePartitions,
			Value: strconv.FormatUint(spec.MaxActivePartitions, 10),
			Reason: fmt.Sprintf("it is below min_active_partitions, %d, which YDB refuses "+
				"(`Invalid total partition count specified`)", minimum)}
	}
	return nil
}

// autoPartitioned reports a strategy under which auto-partitioning is
// enabled, paused included.
func autoPartitioned(strategy string) bool {
	return strategy != "" && strategy != StrategyDisabled
}

// ParseConsumer reads one consumer out of values, keyed by attribute name,
// and ignores every key it does not name, the topic it belongs to included.
//
// A codec may be written in any case and is kept in lower case, as YDB folds
// them; read_from is an RFC 3339 time of whole seconds, kept in UTC, because
// YDB keeps whole seconds in UTC (measured: `read_from =
// Timestamp('2024-01-01T00:00:00.123456Z')` reads back as
// `2024-01-01T00:00:00Z`); and a consumer is not both important and limited
// by an availability period, which YDB refuses (`has both an important flag
// and a limited availability_period, which are mutually exclusive`).
func ParseConsumer(values map[string]string) (ast.TopicConsumerSpec, error) {
	consumer := ast.TopicConsumerSpec{Name: strings.TrimSpace(values[AttributeName])}
	if consumer.Name == "" {
		return ast.TopicConsumerSpec{}, &DeclarationError{Attribute: AttributeName, Reason: "a consumer needs a name"}
	}
	if strings.Contains(consumer.Name, "/") {
		return ast.TopicConsumerSpec{}, &DeclarationError{Attribute: AttributeName, Value: consumer.Name,
			Reason: "a consumer's name cannot hold a slash"}
	}
	var err error
	if consumer.Important, err = boolean(values, AttributeImportant); err != nil {
		return ast.TopicConsumerSpec{}, err
	}
	if raw, ok := present(values, AttributeReadFrom); ok {
		if consumer.ReadFrom, err = NormalizeReadFrom(raw); err != nil {
			return ast.TopicConsumerSpec{}, &DeclarationError{Attribute: AttributeReadFrom, Value: raw, Reason: err.Error()}
		}
	}
	if raw, ok := present(values, AttributeSupportedCodecs); ok {
		if consumer.SupportedCodecs, err = parseCodecs(raw); err != nil {
			return ast.TopicConsumerSpec{}, err
		}
	}
	if consumer.AvailabilityPeriod, err = interval(values, AttributeAvailabilityPeriod); err != nil {
		return ast.TopicConsumerSpec{}, err
	}
	if consumer.Important && consumer.AvailabilityPeriod != "" {
		return ast.TopicConsumerSpec{}, &DeclarationError{Attribute: AttributeAvailabilityPeriod,
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

// positive reads an optional count above zero. YDB reads a zero as no setting
// at all (measured: `min_active_partitions = 0` reads back as 1), so a zero
// declares nothing a reader could see and is refused.
func positive(values map[string]string, attribute string) (uint64, error) {
	raw, ok := present(values, attribute)
	if !ok {
		return 0, nil
	}
	count, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || count == 0 || count > 1<<62 {
		return 0, &DeclarationError{Attribute: attribute, Value: raw, Reason: "takes a whole number above zero"}
	}
	return count, nil
}

// percent reads an optional share of a partition's write speed, from 1 to
// 100. YDB refuses 101 (`Invalid total partition count specified: 0`) and
// reads 0 as no setting.
func percent(values map[string]string, attribute string) (uint32, error) {
	raw, ok := present(values, attribute)
	if !ok {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value == 0 || value > 100 {
		return 0, &DeclarationError{Attribute: attribute, Value: raw, Reason: "takes a whole percentage from 1 to 100"}
	}
	return uint32(value), nil
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
				Reason: "takes a comma-separated list of " + strings.Join(Codecs(), ", ") +
					"; YDB keeps a list naming any other codec as no list at all"}
		}
		if slices.Contains(codecs, codec) {
			return nil, &DeclarationError{Attribute: AttributeSupportedCodecs, Value: raw,
				Reason: "names codec " + codec + " twice"}
		}
		codecs = append(codecs, codec)
	}
	return codecs, nil
}

// Seconds reads an ISO 8601 duration the way YDB's Interval literal takes one
// and returns the whole seconds it denotes.
//
// A duration of no length, a negative one and one with a fraction of a second
// are refused: YDB keeps a topic's intervals in whole seconds and drops the
// fraction (measured: `retention_period = Interval('PT1.5S')` reads back as
// 1s), and a zero retention is read as no setting at all, giving the topic
// another retention than either.
func Seconds(text string) (uint64, error) {
	micros, ok := ydbtype.ParseInterval(text)
	switch {
	case !ok:
		return 0, errors.New("takes an ISO 8601 duration such as PT12H or P1D")
	case micros <= 0:
		return 0, errors.New("is no positive time, and YDB takes only a positive interval")
	case micros%int64(time.Second/time.Microsecond) != 0:
		return 0, errors.New("YDB keeps whole seconds, and drops the fraction of this one")
	}
	return uint64(micros / int64(time.Second/time.Microsecond)), nil
}

// FormatSeconds writes a number of seconds as the ISO 8601 duration YDB's
// Interval reads: 86400 is `P1D`, 129600 `P1DT12H`. It is how the reader
// writes an interval YDB reports in seconds, so a declaration read back from
// the database is one YDB takes.
func FormatSeconds(seconds uint64) string {
	const microsPerSecond = uint64(time.Second / time.Microsecond)
	seconds = min(seconds, math.MaxInt64/microsPerSecond)
	return ydbtype.IntervalText(int64(seconds * microsPerSecond)) // #nosec G115 -- clamped to what an Int64 of microseconds holds above
}

// NormalizeReadFrom reads an RFC 3339 time and writes it the way YDB keeps
// it: in UTC, to the second. A time with a fraction of a second is refused,
// because YDB drops the fraction and the consumer would read from a moment
// other than the one declared.
func NormalizeReadFrom(raw string) (string, error) {
	instant, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err != nil {
		return "", errors.New("takes an RFC 3339 time such as 2026-01-01T00:00:00Z")
	}
	if instant.Nanosecond() != 0 {
		return "", errors.New("YDB keeps whole seconds, and drops the fraction of this one")
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
