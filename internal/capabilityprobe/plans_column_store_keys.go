package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withColumnStoreKeys asks YDB's column-table clauses, each independently: a
// refusal of STORE=COLUMN alone says nothing about an index grammar or TTL
// tiers. Column tables are YDB's, which the YDB provider measures on its own
// lines, so every engine here is asked the statements and its refusal is the
// measurement.
func withColumnStoreKeys(p plan, dialect string) plan {
	const creation = "CREATE TABLE csk (id Uint64 NOT NULL, body Utf8, PRIMARY KEY(id)) PARTITION BY HASH(id) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=1)"
	spelling, ok := typeKeySpellingFor(dialect)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, proven(capability.ColumnStoreTables, schemaChange{change: []string{creation}}))
	for _, question := range []struct {
		key           capability.Capability
		table, clause string
	}{
		{capability.LocalBloomIndexes, "csb", "ADD INDEX local_ix LOCAL USING bloom_filter ON (n)"},
		{capability.LocalNgramIndexes, "csn", "ADD INDEX local_ix LOCAL USING bloom_ngram_filter ON (n)"},
		{capability.LocalMinMaxIndexes, "csm", "ADD INDEX local_ix LOCAL USING min_max ON (n)"},
		{capability.TieredTTL, "cst", "SET (TTL = Interval('P1D') TO EXTERNAL DATA SOURCE '/local/probe_source', Interval('P7D') DELETE ON n AS SECONDS)"},
	} {
		p.experiments = append(p.experiments, proven(question.key, schemaChange{
			setup:  []string{spelling.table(question.table, "n "+spelling.integer)},
			change: []string{"ALTER TABLE " + question.table + " " + question.clause},
		}))
	}
	return p
}
