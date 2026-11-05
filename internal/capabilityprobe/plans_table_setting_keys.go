package capabilityprobe

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withTableSettingKeys adds the questions whether the line changes how a row
// table splits into partitions, its read replicas, and its key bloom filter,
// in YDB's spelling, `ALTER TABLE ... SET (...)`. The settings are YDB's own,
// which the YDB provider measures on its own lines, so every engine here is
// asked the statement and its refusal is the measurement, as with an index's
// partitioning.
func withTableSettingKeys(p plan, dialect string) plan {
	spelling, ok := typeKeySpellingFor(platform.NormalizeDialect(dialect))
	if !ok {
		return p
	}
	for _, setting := range tableSettingStatements() {
		p.experiments = append(p.experiments, proven(setting.key, schemaChange{
			setup:  []string{spelling.table(setting.table, "n "+spelling.integer)},
			change: []string{fmt.Sprintf("ALTER TABLE %s SET (%s)", setting.table, setting.clause)},
		}))
	}
	return p
}

// tableSettingStatement is one table setting the probe sets, on the table it
// creates for the purpose.
type tableSettingStatement struct {
	key    capability.Capability
	table  string
	clause string
}

// tableSettingStatements are the settings each key is asked with, the same on
// every engine.
func tableSettingStatements() []tableSettingStatement {
	return []tableSettingStatement{
		{key: capability.PartitioningOptions, table: "tsp",
			clause: "AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3"},
		{key: capability.ReadReplicas, table: "tsr", clause: `READ_REPLICAS_SETTINGS = "PER_AZ:1"`},
		{key: capability.KeyBloomFilter, table: "tsb", clause: "KEY_BLOOM_FILTER = ENABLED"},
	}
}
