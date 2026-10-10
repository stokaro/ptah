// Package ydbreplication owns the rules of a YDB async replication and of a
// transfer: what a declaration may say, what YDB keeps differently from how it
// was written, when two descriptions are the same, and the statements that
// create, change and drop one.
//
// An async replication copies tables of a source database into read-only
// replica tables YDB creates itself and keeps current; a transfer reads a
// topic's messages, turns each into rows through a YQL lambda and writes them
// into a table. Both reach another database through a connection string and
// a credential named by the secret that holds it. Several of YDB's answers
// shape the rules here, each measured on local-ydb 25.1.4.7 and 26.2.1.14
// unless a line is named:
//
//   - The items, the consistency level and the commit interval of a
//     replication never change in place (`CONSISTENCY_LEVEL is not supported
//     in ALTER`, and ALTER takes no FOR clause), and neither do a transfer's
//     source, target and consumer (`CONSUMER is not supported in ALTER`).
//   - The connection and the credentials change only while the replication
//     or transfer is paused (`Modifications are not allowed in StandBy
//     state`). A SET naming one of them keeps the others.
//   - A replication's connection string reads back rebuilt from its parts,
//     `grpc://host:port/?database=/path`, whatever spelling created it, and
//     a credential reads back as the secret it names: a secret path as its
//     absolute path, a secret name as written. A password or a token written
//     in clear is accepted and never read back, so a declaration never holds
//     one.
//   - A transfer's lambda reads back as written, wrapped in `$__ydb_transfer_
//     lambda = <lambda>;`, so it compares as text and nothing is normalized.
//   - A transfer's flush interval keeps whole seconds (`PT1.5S` reads back as
//     1s), and a replication's commit interval whole milliseconds.
//
// The annotation parser, the YAML reader, the renderer, the reader, the
// comparison and the planner each ask this package, so a declaration one of
// them accepts is one the others read the same way.
package ydbreplication

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ptah.run/internal/ydbtype"
)

// The attributes that declare a replication or a transfer and its
// connection, spelled as YDB spells the settings. The annotation parser, the
// YAML reader and the annotation registry read these names.
const (
	AttributeName               = "name"
	AttributeSchema             = "schema"
	AttributeConnectionString   = "connection_string"
	AttributeTokenSecretName    = "token_secret_name"
	AttributeTokenSecretPath    = "token_secret_path"
	AttributeUser               = "user"
	AttributePasswordSecretName = "password_secret_name"
	AttributePasswordSecretPath = "password_secret_path"
	AttributeConsistencyLevel   = "consistency_level"
	AttributeCommitInterval     = "commit_interval"
)

// The attributes of one replicated table. AttributeReplication names the
// replication the item belongs to, in the directory AttributeSchema names.
const (
	AttributeReplication = "replication"
	AttributeSource      = "source"
	AttributeTarget      = "target"
)

// The attributes of a transfer besides its name, its directory, its source,
// its target and its connection.
const (
	AttributeUsing          = "using"
	AttributeConsumer       = "consumer"
	AttributeBatchSizeBytes = "batch_size_bytes"
	AttributeFlushInterval  = "flush_interval"
)

// The consistency levels, as Ptah keeps them. YDB takes either in any case.
const (
	ConsistencyRow    = "row"
	ConsistencyGlobal = "global"
)

// The values YDB gives a setting the statement does not name, measured by
// describing a replication and a transfer created without it: a global
// replication commits every ten seconds, and a transfer writes batches of
// 8 MiB at least once a minute.
const (
	DefaultCommitIntervalMillis = 10_000
	DefaultBatchSizeBytes       = 8 << 20
	DefaultFlushIntervalSeconds = 60
)

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

// ParseReplication reads a replication's connection and consistency out of
// values, keyed by attribute name, and ignores every key it does not name:
// the name, the directory and the items are the caller's.
//
// The connection string is required, since a replication always reads
// another database. The commit interval needs the global consistency level:
// YDB refuses one with the row level (`Ambiguous consistency level`).
func ParseReplication(values map[string]string) (ReplicationSpec, error) {
	if _, ok := present(values, AttributeConnectionString); !ok {
		return ReplicationSpec{}, &DeclarationError{Attribute: AttributeConnectionString,
			Reason: "a replication reads another database, which YDB reaches through the connection string " +
				"(`Neither CONNECTION_STRING nor ENDPOINT/DATABASE are provided`)"}
	}
	connection, err := ParseConnection(values)
	if err != nil {
		return ReplicationSpec{}, err
	}
	spec := ReplicationSpec{Connection: connection}
	if raw, ok := present(values, AttributeConsistencyLevel); ok {
		level := strings.ToLower(raw)
		if level != ConsistencyRow && level != ConsistencyGlobal {
			return ReplicationSpec{}, &DeclarationError{Attribute: AttributeConsistencyLevel, Value: raw,
				Reason: "takes " + ConsistencyRow + " or " + ConsistencyGlobal}
		}
		spec.ConsistencyLevel = level
	}
	if raw, ok := present(values, AttributeCommitInterval); ok {
		if _, err := CommitIntervalMillis(raw); err != nil {
			return ReplicationSpec{}, &DeclarationError{Attribute: AttributeCommitInterval, Value: raw,
				Reason: err.Error()}
		}
		if spec.ConsistencyLevel != ConsistencyGlobal {
			return ReplicationSpec{}, &DeclarationError{Attribute: AttributeCommitInterval, Value: raw,
				Reason: "a commit interval belongs to the global consistency level; declare " +
					AttributeConsistencyLevel + " as " + ConsistencyGlobal + " (YDB answers `Ambiguous consistency " +
					"level` otherwise)"}
		}
		spec.CommitInterval = raw
	}
	return spec, nil
}

// ParseItem reads one replicated table out of values, keyed by attribute
// name, and ignores every key it does not name, the replication it belongs to
// included.
//
// The source is a path of the source database, relative to its root or
// absolute. The target is a path of this database relative to its root,
// because an absolute one would carry the database's own name into every
// database the declaration is applied to.
func ParseItem(values map[string]string) (Item, error) {
	source, ok := present(values, AttributeSource)
	if !ok || source == "" {
		return Item{}, &DeclarationError{Attribute: AttributeSource,
			Reason: "an item names the table or directory it replicates"}
	}
	if err := checkPath(AttributeSource, source, relativeOrAbsolute); err != nil {
		return Item{}, err
	}
	target, ok := present(values, AttributeTarget)
	if !ok || target == "" {
		return Item{}, &DeclarationError{Attribute: AttributeTarget,
			Reason: "an item names the path its replica is created at"}
	}
	if err := checkPath(AttributeTarget, target, relativeOnly); err != nil {
		return Item{}, err
	}
	return Item{Source: source, Target: target}, nil
}

// ParseTransfer reads a transfer out of values, keyed by attribute name, and
// ignores every key it does not name: the name and the directory are the
// caller's.
//
// The lambda is written inline, `($msg) -> { ... }`, and kept as written,
// because YDB keeps it as written. A lambda that ends the statement with a
// semicolon is refused, since the statement Ptah writes ends it. A
// connection string is optional: without one the topic is one of this
// database, and its path is relative to its root.
func ParseTransfer(values map[string]string) (TransferSpec, error) {
	connection, err := ParseConnection(values)
	if err != nil {
		return TransferSpec{}, err
	}
	spec := TransferSpec{Connection: connection}
	source, _ := present(values, AttributeSource)
	if source == "" {
		return TransferSpec{}, &DeclarationError{Attribute: AttributeSource,
			Reason: "a transfer names the topic it reads"}
	}
	sourceForm := relativeOnly
	if connection.ConnectionString != "" {
		sourceForm = relativeOrAbsolute
	}
	if err := checkPath(AttributeSource, source, sourceForm); err != nil {
		return TransferSpec{}, err
	}
	spec.Source = source
	target, _ := present(values, AttributeTarget)
	if target == "" {
		return TransferSpec{}, &DeclarationError{Attribute: AttributeTarget,
			Reason: "a transfer names the table it writes"}
	}
	if err := checkPath(AttributeTarget, target, relativeOnly); err != nil {
		return TransferSpec{}, err
	}
	spec.Target = target
	lambda, _ := present(values, AttributeUsing)
	if err := checkLambda(lambda); err != nil {
		return TransferSpec{}, err
	}
	spec.Lambda = lambda
	if consumer, ok := present(values, AttributeConsumer); ok {
		if consumer == "" || strings.Contains(consumer, "/") {
			return TransferSpec{}, &DeclarationError{Attribute: AttributeConsumer, Value: consumer,
				Reason: "takes the name of a consumer of the topic, which holds no slash"}
		}
		spec.Consumer = consumer
	}
	if raw, ok := present(values, AttributeBatchSizeBytes); ok {
		size, err := strconv.ParseUint(raw, 10, 63)
		if err != nil || size == 0 {
			return TransferSpec{}, &DeclarationError{Attribute: AttributeBatchSizeBytes, Value: raw,
				Reason: "takes a whole number of bytes above zero (`batch_size_bytes must be greater than 0`)"}
		}
		spec.BatchSizeBytes = size
	}
	if raw, ok := present(values, AttributeFlushInterval); ok {
		if _, err := FlushIntervalSeconds(raw); err != nil {
			return TransferSpec{}, &DeclarationError{Attribute: AttributeFlushInterval, Value: raw,
				Reason: err.Error()}
		}
		spec.FlushInterval = raw
	}
	return spec, nil
}

// checkLambda refuses a lambda Ptah cannot write as the statement's USING.
func checkLambda(lambda string) error {
	switch {
	case lambda == "":
		return &DeclarationError{Attribute: AttributeUsing,
			Reason: "a transfer turns each message into rows through a lambda, such as " +
				"($msg) -> { return [<| id: $msg._offset |>]; }"}
	case !strings.HasPrefix(lambda, "("):
		return &DeclarationError{Attribute: AttributeUsing, Value: lambda,
			Reason: "takes the lambda written inline, starting with its parameter list, such as ($msg) -> { ... }; " +
				"a named lambda would need a statement Ptah does not write before the CREATE TRANSFER"}
	case strings.HasSuffix(lambda, ";"):
		return &DeclarationError{Attribute: AttributeUsing, Value: lambda,
			Reason: "ends with a semicolon, which would end the statement before its WITH clause"}
	default:
		return nil
	}
}

// present returns the trimmed value of attribute and whether one was given.
func present(values map[string]string, attribute string) (string, bool) {
	raw, ok := values[attribute]
	if !ok {
		return "", false
	}
	return strings.TrimSpace(raw), true
}

// CommitIntervalMillis reads a commit interval and returns the milliseconds it
// denotes. YDB keeps whole milliseconds (measured: `PT0.0015S` reads back as
// 0.001s, `PT1.5S` as 1.500s) and refuses no time at all (`commit_interval
// must be positive`), so a finer fraction and a zero are refused.
func CommitIntervalMillis(text string) (uint64, error) {
	micros, err := positiveInterval(text)
	if err != nil {
		return 0, err
	}
	const microsPerMilli = uint64(time.Millisecond / time.Microsecond)
	if micros%microsPerMilli != 0 {
		return 0, errors.New("YDB keeps whole milliseconds, and drops the rest of this one")
	}
	return micros / microsPerMilli, nil
}

// FlushIntervalSeconds reads a flush interval and returns the seconds it
// denotes. YDB keeps whole seconds (measured: `PT1.5S` reads back as 1s) and
// refuses no time at all (`flush_interval must be positive`).
func FlushIntervalSeconds(text string) (uint64, error) {
	micros, err := positiveInterval(text)
	if err != nil {
		return 0, err
	}
	const microsPerSecond = uint64(time.Second / time.Microsecond)
	if micros%microsPerSecond != 0 {
		return 0, errors.New("YDB keeps whole seconds, and drops the fraction of this one")
	}
	return micros / microsPerSecond, nil
}

// positiveInterval reads an ISO 8601 duration as YDB's Interval takes one
// and returns its microseconds, refusing one of no length or a negative one.
func positiveInterval(text string) (uint64, error) {
	micros, ok := ydbtype.ParseInterval(text)
	switch {
	case !ok:
		return 0, errors.New("takes an ISO 8601 duration such as PT10S or PT1M")
	case micros <= 0:
		return 0, errors.New("is no positive time, and YDB takes only a positive interval")
	}
	return uint64(micros), nil // #nosec G115 -- the case above returned for every value below one
}

// FormatMillis writes a number of milliseconds as the ISO 8601 duration YDB's
// Interval reads: 60000 is `PT1M`, 1500 `PT1.5S`.
func FormatMillis(millis uint64) string {
	const microsPerMilli = uint64(time.Millisecond / time.Microsecond)
	return ydbtype.IntervalText(int64(min(millis, uint64(1)<<52) * microsPerMilli)) // #nosec G115 -- clamped far below what an Int64 of microseconds holds
}
