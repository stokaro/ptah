package ydbchangefeed

import (
	"slices"
	"strings"
	"time"

	"ptah.run/core/ast"
	"ptah.run/dialect/ydb/ydbschema"
)

// Equal reports whether two descriptions of a changefeed describe the one YDB
// keeps: the same options, the same retention and the same consumers.
//
// It compares as YDB keeps a changefeed rather than as it was written. A mode,
// a format and a codec compare in any case; an interval compares by the
// seconds it denotes, an empty retention as YDB's 24 hours and an empty
// interval otherwise as none; a read_from compares as an instant, an empty one
// as the start of the epoch YDB reports for it; a consumer's codecs compare as
// a set, because YDB keeps them in the order they were written; and a topic's
// starting partition count compares only where both sides name one, since a
// changefeed that declares none starts with as many partitions as the table
// has, which no declaration can know.
func Equal(desired, current ydbschema.ChangefeedSpec) bool {
	return !Recreated(desired, current) && !TopicChanged(desired, current)
}

// ListsEqual reports whether two lists hold the same changefeeds by name,
// each [Equal] to its namesake, in any order.
func ListsEqual(desired, current []ydbschema.ChangefeedSpec) bool {
	if len(desired) != len(current) {
		return false
	}
	for _, want := range desired {
		index := slices.IndexFunc(current, func(have ydbschema.ChangefeedSpec) bool { return have.Name == want.Name })
		if index < 0 || !Equal(want, current[index]) {
			return false
		}
	}
	return true
}

// Recreated reports whether moving a changefeed from current to desired
// drops it and adds it again: an option of `ADD CHANGEFEED` differs, which YDB
// changes in no other way, or current is disabled, which no statement
// reverses.
func Recreated(desired, current ydbschema.ChangefeedSpec) bool {
	switch {
	case !strings.EqualFold(desired.Mode, current.Mode),
		!strings.EqualFold(desired.Format, current.Format),
		desired.VirtualTimestamps != current.VirtualTimestamps,
		intervalSeconds(desired.ResolvedTimestamps, 0) != intervalSeconds(current.ResolvedTimestamps, 0),
		desired.InitialScan != current.InitialScan,
		desired.UserSIDs != current.UserSIDs,
		desired.SchemaChanges != current.SchemaChanges,
		desired.TopicAutoPartitioning != current.TopicAutoPartitioning,
		desired.Disabled != current.Disabled:
		return true
	}
	return desired.TopicMinActivePartitions != 0 && current.TopicMinActivePartitions != 0 &&
		desired.TopicMinActivePartitions != current.TopicMinActivePartitions
}

// TopicChanged reports whether the changefeed's topic holds another retention
// or other consumers than desired says, which `ALTER TOPIC` changes in place.
func TopicChanged(desired, current ydbschema.ChangefeedSpec) bool {
	if retentionSeconds(desired) != retentionSeconds(current) {
		return true
	}
	if len(desired.Consumers) != len(current.Consumers) {
		return true
	}
	for _, want := range desired.Consumers {
		have, found := consumerNamed(current.Consumers, want.Name)
		if !found || !consumerEqual(want, have) {
			return true
		}
	}
	return false
}

// retentionSeconds is a changefeed's retention in seconds, YDB's default where
// it names none.
func retentionSeconds(spec ydbschema.ChangefeedSpec) uint64 {
	return intervalSeconds(spec.RetentionPeriod, DefaultRetentionSeconds)
}

// intervalSeconds reads an interval, giving empty the value fallback. A value
// this package cannot read compares as its text would: it cannot equal a
// readable one, and a declaration holding one is refused before it is
// compared.
func intervalSeconds(text string, fallback uint64) uint64 {
	if strings.TrimSpace(text) == "" {
		return fallback
	}
	seconds, err := Seconds(text)
	if err != nil {
		return 0
	}
	return seconds
}

// consumerNamed finds a consumer by name.
func consumerNamed(consumers []ast.TopicConsumerSpec, name string) (ast.TopicConsumerSpec, bool) {
	index := slices.IndexFunc(consumers, func(consumer ast.TopicConsumerSpec) bool { return consumer.Name == name })
	if index < 0 {
		return ast.TopicConsumerSpec{}, false
	}
	return consumers[index], true
}

// consumerEqual compares two consumers of one name as YDB keeps them.
func consumerEqual(a, b ast.TopicConsumerSpec) bool {
	return a.Important == b.Important &&
		readFromInstant(a.ReadFrom).Equal(readFromInstant(b.ReadFrom)) &&
		codecsEqual(a.SupportedCodecs, b.SupportedCodecs) &&
		intervalSeconds(a.AvailabilityPeriod, 0) == intervalSeconds(b.AvailabilityPeriod, 0)
}

// readFromInstant reads a read_from, the start of the epoch where it is empty
// and the zero time where it does not parse.
func readFromInstant(text string) time.Time {
	if strings.TrimSpace(text) == "" {
		return time.Unix(0, 0)
	}
	instant, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(text))
	if err != nil {
		return time.Time{}
	}
	return instant
}

// codecsEqual compares two codec lists as sets, in any case.
func codecsEqual(a, b []string) bool {
	return slices.Equal(codecSet(a), codecSet(b))
}

// codecSet is a codec list folded to lower case, sorted and without repeats.
func codecSet(codecs []string) []string {
	set := make([]string, 0, len(codecs))
	for _, codec := range codecs {
		set = append(set, strings.ToLower(strings.TrimSpace(codec)))
	}
	slices.Sort(set)
	return slices.Compact(set)
}

// RetentionChanged reports an effective retention change, resolving omitted
// values through the same target default used by comparison and rendering.
func RetentionChanged(desired, current ydbschema.ChangefeedSpec) bool {
	return retentionSeconds(desired) != retentionSeconds(current)
}

// ConsumerPositionsLost names consumers whose positions a transition discards.
// A missing consumer is dropped; resetting a populated codec list also drops and
// recreates it. These are the same decisions the topic statement writer makes.
func ConsumerPositionsLost(desired, current ydbschema.ChangefeedSpec) []string {
	var names []string
	for _, have := range current.Consumers {
		want, found := consumerNamed(desired.Consumers, have.Name)
		if !found || consumerRecreated(want, have) {
			names = append(names, have.Name)
		}
	}
	slices.Sort(names)
	return names
}

// consumerRecreated is shared by statement generation and reversal assessment:
// neither may report preserved consumer state while the other emits a drop.
func consumerRecreated(desired, current ast.TopicConsumerSpec) bool {
	return len(desired.SupportedCodecs) == 0 && len(current.SupportedCodecs) > 0
}
