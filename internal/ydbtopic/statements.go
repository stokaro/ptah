package ydbtopic

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// Path writes name, a topic's canonical reference, as one quoted YDB path:
// `<directory>/<topic>`, or the name alone at the database root.
func Path(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return sqlident.Quote(platform.YDB, name)
	}
	return sqlident.Qualified(platform.YDB, ref.Schema, ref.Name)
}

// CreateStatement writes what creates the topic name as spec declares it:
// one `CREATE TOPIC` naming each consumer and each setting spec declares. A
// setting spec leaves out is left to YDB, which gives a new topic the value
// [Resolve] reads the declaration with.
func CreateStatement(name string, spec ast.TopicSpec) string {
	var b strings.Builder
	b.WriteString("CREATE TOPIC " + Path(name))
	if len(spec.Consumers) > 0 {
		consumers := make([]string, len(spec.Consumers))
		for i, consumer := range spec.Consumers {
			consumers[i] = "CONSUMER " + quoteName(consumer.Name) + consumerWith(consumer)
		}
		b.WriteString(" (" + strings.Join(consumers, ", ") + ")")
	}
	if settings := declaredSettings(spec); len(settings) > 0 {
		b.WriteString(" WITH (" + strings.Join(settings, ", ") + ")")
	}
	return b.String() + ";"
}

// DropStatement writes what drops the topic name, with every message it holds
// and every consumer's position in it.
func DropStatement(name string) string {
	return "DROP TOPIC " + Path(name) + ";"
}

// AlterStatements writes what moves the topic name from previous to desired,
// and nothing when the two are the same topic.
//
// The first statement carries every change YDB makes in one ALTER TOPIC: the
// settings, the consumers dropped, the consumers changed in place and the
// consumers added. A consumer [Compare] reports as restarted is dropped there
// and added again by a second statement, because YDB refuses one ALTER TOPIC
// that names a consumer twice (`consumer 'b' referenced more than once`).
//
// The settings are written whole whenever any of them differs: every setting
// [Resolve] reads, as the value it resolves to, rather than only the ones that
// differ. A setting nobody declared keeps the value the topic was made with
// rather than following a change of another one -- the burst stays when the
// speed changes, and the thresholds stay at 80% and 20% when a strategy is
// given later -- so naming each is what makes one statement converge. YDB
// takes a setting named at the value it holds (measured on 25.1.4.7 and
// 26.2.1.14: `auto_partitioning_strategy = 'disabled'` on a topic with none,
// and `supported_codecs = ”` on a topic with no list).
func AlterStatements(name string, desired, previous ast.TopicSpec) []string {
	path := Path(name)
	changes := Compare(desired, previous)
	var actions []string
	if !SettingsEqual(desired, previous) {
		actions = append(actions, "SET ("+strings.Join(resolvedSettings(Resolve(desired)), ", ")+")")
	}
	for _, consumer := range changes.Removed {
		actions = append(actions, "DROP CONSUMER "+quoteName(consumer))
	}
	for _, consumer := range changes.Restarted {
		actions = append(actions, "DROP CONSUMER "+quoteName(consumer))
	}
	for _, consumer := range changes.Changed {
		want, _ := consumerNamed(desired.Consumers, consumer)
		have, _ := consumerNamed(previous.Consumers, consumer)
		actions = append(actions, "ALTER CONSUMER "+quoteName(consumer)+" SET ("+alteredConsumer(want, have)+")")
	}
	for _, consumer := range changes.Added {
		want, _ := consumerNamed(desired.Consumers, consumer)
		actions = append(actions, "ADD CONSUMER "+quoteName(consumer)+consumerWith(want))
	}
	var statements []string
	if len(actions) > 0 {
		statements = append(statements, "ALTER TOPIC "+path+" "+strings.Join(actions, ", ")+";")
	}
	if len(changes.Restarted) > 0 {
		readded := make([]string, len(changes.Restarted))
		for i, consumer := range changes.Restarted {
			want, _ := consumerNamed(desired.Consumers, consumer)
			readded[i] = "ADD CONSUMER " + quoteName(consumer) + consumerWith(want)
		}
		statements = append(statements, "ALTER TOPIC "+path+" "+strings.Join(readded, ", ")+";")
	}
	return statements
}

// declaredSettings writes the settings spec names, in the order YDB's
// documentation lists them.
func declaredSettings(spec ast.TopicSpec) []string {
	var settings []string
	add := func(declared bool, setting string) {
		if declared {
			settings = append(settings, setting)
		}
	}
	add(spec.MinActivePartitions != 0, AttributeMinActivePartitions+" = "+formatCount(spec.MinActivePartitions))
	add(spec.MaxActivePartitions != 0, AttributeMaxActivePartitions+" = "+formatCount(spec.MaxActivePartitions))
	add(spec.AutoPartitioningStrategy != "", AttributeStrategy+" = "+quoteString(spec.AutoPartitioningStrategy))
	add(spec.AutoPartitioningUpUtilizationPercent != 0,
		AttributeUpUtilizationPercent+" = "+formatCount(uint64(spec.AutoPartitioningUpUtilizationPercent)))
	add(spec.AutoPartitioningDownUtilizationPercent != 0,
		AttributeDownUtilizationPercent+" = "+formatCount(uint64(spec.AutoPartitioningDownUtilizationPercent)))
	add(spec.AutoPartitioningStabilizationWindow != "",
		AttributeStabilizationWindow+" = "+intervalLiteral(intervalSeconds(spec.AutoPartitioningStabilizationWindow, 0)))
	add(spec.RetentionPeriod != "", AttributeRetentionPeriod+" = "+intervalLiteral(intervalSeconds(spec.RetentionPeriod, 0)))
	add(spec.PartitionWriteSpeedBytesPerSecond != 0,
		AttributeWriteSpeed+" = "+formatCount(spec.PartitionWriteSpeedBytesPerSecond))
	add(spec.PartitionWriteBurstBytes != 0, AttributeWriteBurst+" = "+formatCount(spec.PartitionWriteBurstBytes))
	add(len(spec.SupportedCodecs) > 0, AttributeSupportedCodecs+" = "+codecsLiteral(spec.SupportedCodecs))
	return settings
}

// resolvedSettings writes every setting of settings, the thresholds and the
// maximum only where auto-partitioning is enabled, since YDB keeps no maximum
// otherwise.
func resolvedSettings(settings Settings) []string {
	written := []string{
		AttributeMinActivePartitions + " = " + formatCount(settings.MinActivePartitions),
		AttributeStrategy + " = " + quoteString(settings.Strategy),
	}
	if settings.AutoPartitioned() {
		written = append(written,
			AttributeMaxActivePartitions+" = "+formatCount(settings.MaxActivePartitions),
			AttributeUpUtilizationPercent+" = "+formatCount(uint64(settings.UpUtilizationPercent)),
			AttributeDownUtilizationPercent+" = "+formatCount(uint64(settings.DownUtilizationPercent)),
			AttributeStabilizationWindow+" = "+intervalLiteral(settings.StabilizationWindowSeconds),
		)
	}
	return append(written,
		AttributeRetentionPeriod+" = "+intervalLiteral(settings.RetentionSeconds),
		AttributeWriteSpeed+" = "+formatCount(settings.WriteSpeed),
		AttributeWriteBurst+" = "+formatCount(settings.WriteBurst),
		AttributeSupportedCodecs+" = "+codecsLiteral(settings.Codecs),
	)
}

// consumerWith writes the WITH clause of a new consumer, naming only what the
// consumer declares, or nothing for a consumer that declares nothing.
func consumerWith(consumer ast.TopicConsumerSpec) string {
	var settings []string
	if consumer.Important {
		settings = append(settings, AttributeImportant+" = TRUE")
	}
	if consumer.ReadFrom != "" {
		settings = append(settings, AttributeReadFrom+" = "+timestampLiteral(consumer.ReadFrom))
	}
	if len(consumer.SupportedCodecs) > 0 {
		settings = append(settings, AttributeSupportedCodecs+" = "+codecsLiteral(consumer.SupportedCodecs))
	}
	if consumer.AvailabilityPeriod != "" {
		settings = append(settings,
			AttributeAvailabilityPeriod+" = "+intervalLiteral(intervalSeconds(consumer.AvailabilityPeriod, 0)))
	}
	if len(settings) == 0 {
		return ""
	}
	return " WITH (" + strings.Join(settings, ", ") + ")"
}

// alteredConsumer writes the settings of one ALTER CONSUMER ... SET. It names
// important and read_from always, the codecs where the consumer is to have
// any, and the availability period where either side has one: zero removes
// it, which 26.2.1.14 takes, and a line without the setting has no consumer
// holding one.
func alteredConsumer(desired, previous ast.TopicConsumerSpec) string {
	readFrom := desired.ReadFrom
	if readFrom == "" {
		readFrom = epoch
	}
	settings := []string{
		AttributeImportant + " = " + strings.ToUpper(strconv.FormatBool(desired.Important)),
		AttributeReadFrom + " = " + timestampLiteral(readFrom),
	}
	if len(desired.SupportedCodecs) > 0 {
		settings = append(settings, AttributeSupportedCodecs+" = "+codecsLiteral(desired.SupportedCodecs))
	}
	if desired.AvailabilityPeriod != "" || previous.AvailabilityPeriod != "" {
		settings = append(settings,
			AttributeAvailabilityPeriod+" = "+intervalLiteral(intervalSeconds(desired.AvailabilityPeriod, 0)))
	}
	return strings.Join(settings, ", ")
}

// epoch is the read_from YDB reports for a consumer that declared none.
const epoch = "1970-01-01T00:00:00Z"

// quoteName quotes an identifier for YQL.
func quoteName(name string) string {
	return sqlident.Quote(platform.YDB, name)
}

// quoteString writes a YQL string literal.
func quoteString(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
}

// intervalLiteral writes a number of seconds as YDB's Interval literal.
func intervalLiteral(seconds uint64) string {
	return "Interval(" + quoteString(FormatSeconds(seconds)) + ")"
}

// timestampLiteral writes an RFC 3339 time as YDB's Timestamp literal.
func timestampLiteral(text string) string {
	return "Timestamp(" + quoteString(text) + ")"
}

// codecsLiteral writes a codec list as the string YDB takes, an empty list as
// the empty string, which takes any codec.
func codecsLiteral(codecs []string) string {
	lower := make([]string, len(codecs))
	for i, codec := range codecs {
		lower[i] = strings.ToLower(strings.TrimSpace(codec))
	}
	return quoteString(strings.Join(lower, ","))
}

// formatCount writes a count.
func formatCount(count uint64) string {
	return strconv.FormatUint(count, 10)
}

// consumerSubject names one consumer of the topic name, for a refusal.
func consumerSubject(name, consumer string) string {
	return fmt.Sprintf("consumer %q of topic %s", consumer, name)
}
