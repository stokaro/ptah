package ydbchangefeed

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbttl"
)

// TablePath writes table, a canonical table reference, as one quoted YDB
// path, the way the renderer names a table.
func TablePath(table string) string {
	ref, ok := tableref.Parse(table)
	if !ok {
		return sqlident.Quote(platform.YDB, table)
	}
	return sqlident.Qualified(platform.YDB, ref.Schema, ref.Name)
}

// AddStatements writes what adds spec to table: one `ALTER TABLE ... ADD
// CHANGEFEED`, since YDB adds one changefeed per statement (`Only one
// changefeed can be added by one operation`), and then one `ALTER TOPIC ...
// ADD CONSUMER` per consumer, since the topic exists only once the changefeed
// does.
//
// The WITH clause names MODE and FORMAT, which YDB requires, and each other
// option only where spec declares it. An interval is written as
// [ydbttl.FormatInterval] writes the seconds it denotes, a spelling YDB takes.
func AddStatements(table string, spec ydbschema.ChangefeedSpec) []string {
	options := []string{
		"MODE = " + quoteString(strings.ToUpper(spec.Mode)),
		"FORMAT = " + quoteString(strings.ToUpper(spec.Format)),
	}
	if spec.VirtualTimestamps {
		options = append(options, "VIRTUAL_TIMESTAMPS = TRUE")
	}
	if spec.ResolvedTimestamps != "" {
		options = append(options, "RESOLVED_TIMESTAMPS = "+intervalLiteral(spec.ResolvedTimestamps))
	}
	if spec.RetentionPeriod != "" {
		options = append(options, "RETENTION_PERIOD = "+intervalLiteral(spec.RetentionPeriod))
	}
	if spec.InitialScan {
		options = append(options, "INITIAL_SCAN = TRUE")
	}
	if spec.UserSIDs {
		options = append(options, "USER_SIDS = TRUE")
	}
	if spec.SchemaChanges {
		options = append(options, "SCHEMA_CHANGES = TRUE")
	}
	if spec.TopicAutoPartitioning {
		options = append(options, "TOPIC_AUTO_PARTITIONING = 'ENABLED'")
	}
	if spec.TopicMinActivePartitions != 0 {
		options = append(options, "TOPIC_MIN_ACTIVE_PARTITIONS = "+strconv.FormatUint(spec.TopicMinActivePartitions, 10))
	}
	statements := []string{fmt.Sprintf("ALTER TABLE %s ADD CHANGEFEED %s WITH (%s);",
		TablePath(table), quoteName(spec.Name), strings.Join(options, ", "))}
	topic := TopicPath(table, spec.Name)
	for _, consumer := range spec.Consumers {
		statements = append(statements, addConsumer(topic, consumer))
	}
	return statements
}

// DropStatement writes what drops changefeed name from table, with its topic
// and every consumer of it.
func DropStatement(table, name string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CHANGEFEED %s;", TablePath(table), quoteName(name))
}

// TopicStatements writes what moves the topic of a changefeed of table from
// what previous describes to what desired does, one `ALTER TOPIC` per change:
// the retention first, then the consumers previous alone names are dropped,
// those both name are changed, and those desired alone names are added.
//
// It also returns the consumers it drops and adds again. YDB keeps a
// consumer's codecs once it has any: `RESET (supported_codecs)` changes
// nothing on 26.2.1.14 and fails on 25.1.4.7, so a consumer that is to take
// any codec again is dropped and added, and starts reading from the beginning.
//
// A retention the declaration leaves out is set to YDB's 24 hours rather than
// reset, because `RESET (retention_period)` changes nothing on 26.2.1.14 and
// is refused on 25.1.4.7. A consumer change names the consumer's important
// and read_from settings whether or not they differ, so its outcome does not
// depend on what the consumer held.
func TopicStatements(table string, desired, previous ydbschema.ChangefeedSpec) (statements, restarted []string) {
	topic := TopicPath(table, desired.Name)
	if retentionSeconds(desired) != retentionSeconds(previous) {
		statements = append(statements, fmt.Sprintf("ALTER TOPIC %s SET (retention_period = %s);",
			topic, secondsLiteral(retentionSeconds(desired))))
	}
	for _, have := range previous.Consumers {
		if _, kept := consumerNamed(desired.Consumers, have.Name); !kept {
			statements = append(statements, dropConsumer(topic, have.Name))
		}
	}
	var added []ast.TopicConsumerSpec
	for _, want := range desired.Consumers {
		have, found := consumerNamed(previous.Consumers, want.Name)
		switch {
		case !found:
			added = append(added, want)
		case consumerEqual(want, have):
		case consumerRecreated(want, have):
			statements = append(statements, dropConsumer(topic, want.Name))
			added = append(added, want)
			restarted = append(restarted, want.Name)
		default:
			statements = append(statements, alterConsumer(topic, want, have))
		}
	}
	for _, want := range added {
		statements = append(statements, addConsumer(topic, want))
	}
	return statements, restarted
}

// addConsumer writes one ADD CONSUMER, with a WITH clause naming only what the
// consumer declares.
func addConsumer(topic string, consumer ast.TopicConsumerSpec) string {
	var settings []string
	if consumer.Important {
		settings = append(settings, "important = TRUE")
	}
	if consumer.ReadFrom != "" {
		settings = append(settings, "read_from = "+timestampLiteral(consumer.ReadFrom))
	}
	if len(consumer.SupportedCodecs) > 0 {
		settings = append(settings, "supported_codecs = "+codecsLiteral(consumer.SupportedCodecs))
	}
	if consumer.AvailabilityPeriod != "" {
		settings = append(settings, "availability_period = "+intervalLiteral(consumer.AvailabilityPeriod))
	}
	statement := fmt.Sprintf("ALTER TOPIC %s ADD CONSUMER %s", topic, quoteName(consumer.Name))
	if len(settings) > 0 {
		statement += " WITH (" + strings.Join(settings, ", ") + ")"
	}
	return statement + ";"
}

// alterConsumer writes one ALTER CONSUMER ... SET. It names important and
// read_from always, the codecs where the consumer is to have any, and the
// availability period where either side has one: zero removes it, which
// 26.2.1.14 takes, and a line without the setting has no consumer holding
// one.
func alterConsumer(topic string, desired, previous ast.TopicConsumerSpec) string {
	readFrom := desired.ReadFrom
	if readFrom == "" {
		readFrom = time0
	}
	settings := []string{
		"important = " + strings.ToUpper(strconv.FormatBool(desired.Important)),
		"read_from = " + timestampLiteral(readFrom),
	}
	if len(desired.SupportedCodecs) > 0 {
		settings = append(settings, "supported_codecs = "+codecsLiteral(desired.SupportedCodecs))
	}
	if desired.AvailabilityPeriod != "" || previous.AvailabilityPeriod != "" {
		settings = append(settings, "availability_period = "+secondsLiteral(intervalSeconds(desired.AvailabilityPeriod, 0)))
	}
	return fmt.Sprintf("ALTER TOPIC %s ALTER CONSUMER %s SET (%s);", topic, quoteName(desired.Name),
		strings.Join(settings, ", "))
}

// dropConsumer writes one DROP CONSUMER.
func dropConsumer(topic, name string) string {
	return fmt.Sprintf("ALTER TOPIC %s DROP CONSUMER %s;", topic, quoteName(name))
}

// time0 is the read_from YDB reports for a consumer that declared none.
const time0 = "1970-01-01T00:00:00Z"

// quoteName quotes an identifier for YQL.
func quoteName(name string) string {
	return sqlident.Quote(platform.YDB, name)
}

// quoteString writes a YQL string literal.
func quoteString(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
}

// intervalLiteral writes an interval as YDB's Interval literal, in the
// spelling [ydbttl.FormatInterval] gives the seconds it denotes.
func intervalLiteral(text string) string {
	return secondsLiteral(intervalSeconds(text, 0))
}

// secondsLiteral writes a number of seconds as YDB's Interval literal.
func secondsLiteral(seconds uint64) string {
	return "Interval(" + quoteString(ydbttl.FormatInterval(seconds)) + ")"
}

// timestampLiteral writes an RFC 3339 time as YDB's Timestamp literal.
func timestampLiteral(text string) string {
	return "Timestamp(" + quoteString(text) + ")"
}

// codecsLiteral writes a codec list as the string YDB takes.
func codecsLiteral(codecs []string) string {
	lower := make([]string, len(codecs))
	for i, codec := range codecs {
		lower[i] = strings.ToLower(codec)
	}
	return quoteString(strings.Join(lower, ","))
}
