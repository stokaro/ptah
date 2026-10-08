// Package ydbschema owns typed YDB schema state and its local snapshot semantics.
package ydbschema

import "ptah.run/core/ast"

// ChangefeedSpec is a YDB changefeed: a stream of the changes made to one row
// table, which YDB writes to a topic of its own at the path
// `<table>/<changefeed>`.
//
// Most fields name an option of `ALTER TABLE ... ADD CHANGEFEED ... WITH
// (...)`, and YDB changes none of them in place: `ALTER CHANGEFEED ... SET`
// answers `MODE alter is not supported` for each. A change to one of them
// drops the changefeed and adds it again, which restarts the stream.
// RetentionPeriod and Consumers belong to the topic, and change in place
// through `ALTER TOPIC`.
//
// A field left at its zero value declares nothing. The two intervals are ISO
// 8601 durations that YDB keeps in whole seconds, and an empty one is YDB's
// default: no resolved timestamps, and records kept for 24 hours.
type ChangefeedSpec struct {
	// Name is the changefeed's name, unique among the table's changefeeds
	// and indexes, which share the table's path.
	Name string `json:"name"`
	// Mode is MODE, what a record carries: KEYS_ONLY, UPDATES, NEW_IMAGE,
	// OLD_IMAGE or NEW_AND_OLD_IMAGES.
	Mode string `json:"mode"`
	// Format is FORMAT, how a record is written: JSON or DEBEZIUM_JSON.
	Format string `json:"format"`
	// VirtualTimestamps is VIRTUAL_TIMESTAMPS: each record carries the
	// virtual timestamp of its change.
	VirtualTimestamps bool `json:"virtual_timestamps,omitempty"`
	// ResolvedTimestamps is RESOLVED_TIMESTAMPS, which YDB also spells
	// BARRIERS_INTERVAL: the interval at which YDB writes a barrier record
	// to every partition. Empty writes none.
	ResolvedTimestamps string `json:"resolved_timestamps,omitempty"`
	// InitialScan is INITIAL_SCAN: the stream opens with a record for every
	// row the table holds when the changefeed is added.
	InitialScan bool `json:"initial_scan,omitempty"`
	// UserSIDs is USER_SIDS: each record names the user whose change it is.
	UserSIDs bool `json:"user_sids,omitempty"`
	// SchemaChanges is SCHEMA_CHANGES: the stream carries a record for each
	// change of the table's schema.
	SchemaChanges bool `json:"schema_changes,omitempty"`
	// TopicMinActivePartitions is TOPIC_MIN_ACTIVE_PARTITIONS, the number of
	// partitions the topic starts with. Zero declares none, and YDB then
	// gives the topic one partition per partition of the table.
	TopicMinActivePartitions uint64 `json:"topic_min_active_partitions,omitempty"`
	// TopicAutoPartitioning is `TOPIC_AUTO_PARTITIONING = 'ENABLED'`: the
	// topic gains partitions as the table's write rate grows.
	TopicAutoPartitioning bool `json:"topic_auto_partitioning,omitempty"`
	// RetentionPeriod is RETENTION_PERIOD, how long the topic keeps a record
	// whether or not it was read. Empty keeps YDB's 24 hours.
	RetentionPeriod string `json:"retention_period,omitempty"`
	// Consumers are the consumers of the changefeed's topic. Each keeps its
	// position in the stream, which a stream that restarts loses.
	Consumers []ast.TopicConsumerSpec `json:"consumers,omitempty"`
	// Disabled reports a changefeed YDB no longer writes to. Only a reader
	// sets it: YDB has no statement that disables one (`ALTER CHANGEFEED ...
	// DISABLE` answers `Name not found: quote` on every measured line), so a
	// declaration cannot ask for it, and a disabled changefeed differs from
	// the declaration of the same one.
	Disabled bool `json:"disabled,omitempty"`
}

// Clone returns an independent copy, so a spec handed to a comparator or a
// planner cannot be changed through the consumer list it shares with the
// schema it came from.
func (s ChangefeedSpec) Clone() ChangefeedSpec {
	out := s
	if s.Consumers != nil {
		out.Consumers = make([]ast.TopicConsumerSpec, len(s.Consumers))
		for i, consumer := range s.Consumers {
			out.Consumers[i] = consumer.Clone()
		}
	}
	return out
}

// CloneChangefeeds copies a list of changefeeds with [ChangefeedSpec.Clone].
// Nil stays nil.
func CloneChangefeeds(changefeeds []ChangefeedSpec) []ChangefeedSpec {
	if changefeeds == nil {
		return nil
	}
	out := make([]ChangefeedSpec, len(changefeeds))
	for i, changefeed := range changefeeds {
		out[i] = changefeed.Clone()
	}
	return out
}
