package ydbreplication

import (
	"slices"
	"strings"
)

// ReplicationChanges is how two descriptions of one replication differ.
type ReplicationChanges struct {
	// ConnectionString reports a connection string that differs as YDB
	// keeps it.
	ConnectionString bool
	// Credentials reports a credential that differs.
	Credentials bool
	// CreateOnly names the settings that differ and that YDB changes in no
	// replication in place, in a fixed order: items, consistency_level,
	// commit_interval.
	CreateOnly []string
}

// Any reports any difference.
func (c ReplicationChanges) Any() bool {
	return c.ConnectionString || c.Credentials || len(c.CreateOnly) > 0
}

// CompareReplication reads how desired differs from current, each resolved as
// YDB keeps it: the connection string in its canonical form, the consistency
// level row where none is named, the commit interval of a global replication
// ten seconds where none is named, and the items by what they replicate.
//
// A replication the database holds with no item -- one that failed before it
// resolved its tables, such as one whose secret YDB cannot read -- has its
// items left out of the comparison: the description says nothing about them,
// and reading the silence as no items would refuse every declaration of it.
func CompareReplication(desired, current ReplicationSpec) ReplicationChanges {
	var changes ReplicationChanges
	changes.ConnectionString = CanonicalConnectionString(desired.Connection.ConnectionString) !=
		CanonicalConnectionString(current.Connection.ConnectionString)
	changes.Credentials = !credentialsEqual(desired.Connection, current.Connection)
	if len(current.Items) > 0 && !ItemsEqual(desired, current) {
		changes.CreateOnly = append(changes.CreateOnly, "items")
	}
	if consistencyLevel(desired) != consistencyLevel(current) {
		changes.CreateOnly = append(changes.CreateOnly, AttributeConsistencyLevel)
	} else if commitIntervalMillis(desired) != commitIntervalMillis(current) {
		changes.CreateOnly = append(changes.CreateOnly, AttributeCommitInterval)
	}
	return changes
}

// ReplicationsEqual reports whether two descriptions of a replication
// describe the one YDB holds.
func ReplicationsEqual(desired, current ReplicationSpec) bool {
	return !CompareReplication(desired, current).Any()
}

// consistencyLevel is a replication's level, row where none is named.
func consistencyLevel(spec ReplicationSpec) string {
	if strings.EqualFold(spec.ConsistencyLevel, ConsistencyGlobal) {
		return ConsistencyGlobal
	}
	return ConsistencyRow
}

// commitIntervalMillis is a replication's commit interval in milliseconds:
// zero at the row level, and ten seconds at the global level where none is
// named.
func commitIntervalMillis(spec ReplicationSpec) uint64 {
	if consistencyLevel(spec) != ConsistencyGlobal {
		return 0
	}
	if strings.TrimSpace(spec.CommitInterval) == "" {
		return DefaultCommitIntervalMillis
	}
	millis, err := CommitIntervalMillis(spec.CommitInterval)
	if err != nil {
		return 0
	}
	return millis
}

// ItemsEqual reports whether two replications replicate the same tables to
// the same places.
//
// YDB reads a directory item back as one item per table it replicates:
// measured, `FOR srcdir AS dstdir` over srcdir/t1 and srcdir/sub/t2 reads
// back as `srcdir/t1 -> dstdir/t1` and `srcdir/sub/t2 -> dstdir/sub/t2`. So
// each item of current has to sit under exactly one item of desired, at the
// same place under its target, and each item of desired has to cover one at
// least. A source compares as the absolute path it resolves to in its own
// database, so `/local/t` and `t` of a replication reading /local are one
// source, and `t` of a replication reading /other is another: YDB stores each
// item's source as an absolute path when it creates the replication, and a
// later change of the connection string leaves those paths in the database
// they were resolved in. 26.2.1.14 reads the items back sorted by target and
// 25.1.4.7 in declaration order, so the order is not compared.
func ItemsEqual(desired, current ReplicationSpec) bool {
	desiredDatabase := ConnectionDatabase(desired.Connection.ConnectionString)
	currentDatabase := ConnectionDatabase(current.Connection.ConnectionString)
	covered := make([]int, len(desired.Items))
	for _, have := range current.Items {
		source := absoluteSource(have.Source, currentDatabase)
		target := cleanRelative(have.Target)
		index := slices.IndexFunc(desired.Items, func(want Item) bool {
			return covers(absoluteSource(want.Source, desiredDatabase), cleanRelative(want.Target), source, target)
		})
		if index < 0 {
			return false
		}
		covered[index]++
	}
	return !slices.Contains(covered, 0)
}

// covers reports whether the item from wantSource to wantTarget replicates
// the table at source to target: it names the table, or a directory holding
// it with target at the same place under its own target.
func covers(wantSource, wantTarget, source, target string) bool {
	if wantSource == source {
		return wantTarget == target
	}
	rest, under := strings.CutPrefix(source, wantSource+"/")
	return under && target == wantTarget+"/"+rest
}

// absoluteSource is the path source resolves to in database: itself where it
// is absolute or where database is not known, and under database otherwise.
func absoluteSource(source, database string) string {
	switch {
	case strings.HasPrefix(source, "/"):
		return "/" + cleanRelative(source)
	case database == "":
		return cleanRelative(source)
	}
	return "/" + cleanRelative(database) + "/" + cleanRelative(source)
}

// SourceKey is a source path as a comparison reads it: relative to database
// where it lies under it, and as written otherwise.
func SourceKey(source, database string) string {
	if strings.HasPrefix(source, "/") {
		if rest, under := strings.CutPrefix(source, strings.TrimRight(database, "/")+"/"); database != "" && under {
			return rest
		}
		return source
	}
	return cleanRelative(source)
}

// Targets lists the paths a replication's replica tables are created at, as
// declared: a table's path, or a directory that holds the replicas of a
// directory's tables.
func Targets(spec ReplicationSpec) []string {
	targets := make([]string, 0, len(spec.Items))
	for _, item := range spec.Items {
		targets = append(targets, cleanRelative(item.Target))
	}
	return targets
}

// UnderTarget reports whether the path of a table lies at one of targets or
// in a directory one of them names.
func UnderTarget(tablePath string, targets []string) bool {
	tablePath = cleanRelative(tablePath)
	for _, target := range targets {
		if tablePath == target || strings.HasPrefix(tablePath, target+"/") {
			return true
		}
	}
	return false
}

// TransferChanges is how two descriptions of one transfer differ.
type TransferChanges struct {
	// Lambda reports a lambda that differs as text.
	Lambda bool
	// Batch reports a batch size or a flush interval that differs as YDB
	// keeps it.
	Batch bool
	// ConnectionString reports a connection string that differs as YDB
	// keeps it.
	ConnectionString bool
	// Credentials reports a credential that differs.
	Credentials bool
	// CreateOnly names the settings that differ and that YDB changes in no
	// transfer in place, in a fixed order: source, target, consumer.
	CreateOnly []string
}

// Any reports any difference.
func (c TransferChanges) Any() bool {
	return c.Lambda || c.Batch || c.ConnectionString || c.Credentials || len(c.CreateOnly) > 0
}

// CompareTransfer reads how desired differs from current, each resolved as
// YDB keeps it: the lambda as written, the batch size and the flush interval
// at YDB's 8 MiB and minute where none is named, and the consumer as any
// where desired names none, since YDB then creates one of its own name.
func CompareTransfer(desired, current TransferSpec) TransferChanges {
	var changes TransferChanges
	changes.Lambda = strings.TrimSpace(desired.Lambda) != strings.TrimSpace(current.Lambda)
	changes.Batch = batchSize(desired) != batchSize(current) || flushSeconds(desired) != flushSeconds(current)
	changes.ConnectionString = CanonicalConnectionString(desired.Connection.ConnectionString) !=
		CanonicalConnectionString(current.Connection.ConnectionString)
	changes.Credentials = !credentialsEqual(desired.Connection, current.Connection)
	if absoluteSource(desired.Source, ConnectionDatabase(desired.Connection.ConnectionString)) !=
		absoluteSource(current.Source, ConnectionDatabase(current.Connection.ConnectionString)) {
		changes.CreateOnly = append(changes.CreateOnly, AttributeSource)
	}
	if cleanRelative(desired.Target) != cleanRelative(current.Target) {
		changes.CreateOnly = append(changes.CreateOnly, AttributeTarget)
	}
	if desired.Consumer != "" && desired.Consumer != current.Consumer {
		changes.CreateOnly = append(changes.CreateOnly, AttributeConsumer)
	}
	return changes
}

// TransfersEqual reports whether two descriptions of a transfer describe the
// one YDB holds.
func TransfersEqual(desired, current TransferSpec) bool {
	return !CompareTransfer(desired, current).Any()
}

// batchSize is a transfer's batch size, YDB's 8 MiB where none is named.
func batchSize(spec TransferSpec) uint64 {
	if spec.BatchSizeBytes == 0 {
		return DefaultBatchSizeBytes
	}
	return spec.BatchSizeBytes
}

// flushSeconds is a transfer's flush interval in seconds, YDB's minute where
// none is named.
func flushSeconds(spec TransferSpec) uint64 {
	if strings.TrimSpace(spec.FlushInterval) == "" {
		return DefaultFlushIntervalSeconds
	}
	seconds, err := FlushIntervalSeconds(spec.FlushInterval)
	if err != nil {
		return 0
	}
	return seconds
}

// LocalSource reports a transfer that reads a topic of its own database.
func LocalSource(spec TransferSpec) bool {
	return spec.Connection.ConnectionString == ""
}

// TablePath is the path of the YDB table name in the directory schema,
// relative to the database root: how a replica's target and a transfer's
// target name a table.
func TablePath(schema, name string) string {
	if schema = cleanRelative(schema); schema != "" {
		return schema + "/" + name
	}
	return name
}

// ReplicationRollbackTarget is the replication a rollback of a change from
// before to after reaches in place: before, except that a credential after
// holds and before does not stays, since YDB has no statement that takes a
// credential away.
func ReplicationRollbackTarget(before, after ReplicationSpec) ReplicationSpec {
	target := before.Clone()
	target.Connection = rollbackConnection(before.Connection, after.Connection)
	return target
}

// TransferRollbackTarget is the transfer a rollback of a change from before
// to after reaches in place, as [ReplicationRollbackTarget] reads a
// replication's.
func TransferRollbackTarget(before, after TransferSpec) TransferSpec {
	target := before
	target.Connection = rollbackConnection(before.Connection, after.Connection)
	return target
}

// rollbackConnection is before, with after's credential where before holds
// none and after holds one.
func rollbackConnection(before, after Connection) Connection {
	if HasCredentials(before) || !HasCredentials(after) {
		return before
	}
	kept := after
	kept.ConnectionString = before.ConnectionString
	return kept
}
