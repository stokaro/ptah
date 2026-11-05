package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withColumnFamilyKeys adds the questions whether a table carries column
// families at all, and whether the line takes a family's cache mode, in YDB's
// spelling, `ALTER TABLE ... ADD FAMILY`. A pool kind differs from cluster to
// cluster, so no question names DATA. A column family in this sense is YDB's,
// which the YDB provider measures on its own lines, so every engine here is
// asked the statement and its refusal is the measurement, as with a
// changefeed; CockroachDB's FAMILY clause groups columns with no settings, and
// Ptah models none.
func withColumnFamilyKeys(p plan, dialect string) plan {
	spelling, ok := typeKeySpellingFor(platform.NormalizeDialect(dialect))
	if !ok {
		return p
	}
	for _, question := range columnFamilyQuestions() {
		p.experiments = append(p.experiments, proven(question.key, schemaChange{
			setup:  []string{spelling.table(question.table, "n "+spelling.integer)},
			change: []string{question.statement},
		}))
	}
	return p
}

// columnFamilyQuestion is one statement the probe sends about column
// families, on the table it creates for the purpose.
type columnFamilyQuestion struct {
	key       capability.Capability
	table     string
	statement string
}

// columnFamilyQuestions are the statements each key is asked with, the same on
// every engine.
func columnFamilyQuestions() []columnFamilyQuestion {
	return []columnFamilyQuestion{
		{key: capability.ColumnFamilies, table: "cff",
			statement: "ALTER TABLE cff ADD FAMILY cold (COMPRESSION = 'lz4'), ALTER COLUMN n SET FAMILY cold"},
		{key: capability.ColumnFamilyCacheMode, table: "cfm",
			statement: "ALTER TABLE cfm ADD FAMILY hot (CACHE_MODE = 'in_memory')"},
	}
}
