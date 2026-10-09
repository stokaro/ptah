package capabilityprobe

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
)

func withColumnStoreKeys(p plan, dialect string) plan {
	const creation = "CREATE TABLE csk (id Uint64 NOT NULL, body Utf8, PRIMARY KEY(id)) PARTITION BY HASH(id) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=1)"
	if dialect != platform.YDB {
		return withForeignColumnStoreKeys(p, dialect, creation)
	}
	p.experiments = append(p.experiments, proven(capability.ColumnStoreTables, schemaChange{
		change: []string{creation}, after: []check{ydbColumnStoreReadback("csk")},
	}))
	for _, index := range []struct {
		key                  capability.Capability
		name, method, column string
	}{
		{capability.LocalBloomIndexes, "csb", "bloom_filter", "body"},
		{capability.LocalNgramIndexes, "csn", "bloom_ngram_filter", "body"},
		{capability.LocalMinMaxIndexes, "csm", "min_max", "id"},
	} {
		table := index.name
		method := index.method
		p.experiments = append(p.experiments, proven(index.key, schemaChange{
			setup:  []string{fmt.Sprintf("CREATE TABLE %s (id Uint64 NOT NULL, body Utf8, PRIMARY KEY(id)) PARTITION BY HASH(id) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=1)", table)},
			change: []string{fmt.Sprintf("ALTER TABLE %s ADD INDEX local_ix LOCAL USING %s ON (%s)", table, method, index.column)},
			after:  []check{ydbDescribedIndex(table, "local_ix", "the local index read through monitoring", func(index catalog.Index) bool { return index.Method == method })},
		}))
	}
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.TieredTTL] = "eviction tiers require an authenticated ObjectStorage external source and cluster tiering enabled; the default matrix has neither, so the live column-table integration contour measures this combination"
	return p
}

func ydbColumnStoreReadback(table string) check {
	return check{describes: "column storage with one shard and the declared hash key", inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
		attempt := Attempt{Statement: "read column table " + table + " through Ptah's YDB reader"}
		db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
		if err != nil {
			attempt.ServerErr = err.Error()
			return attempt, false, "was refused"
		}
		attempt.Accepted = true
		for _, found := range db.Tables {
			if found.Name == table {
				spec := found.YDBColumnTable
				return attempt, spec != nil && spec.Partitions == 1 && slices.Equal(spec.HashColumns, []string{"id"}), "read column storage and hash partitioning"
			}
		}
		return attempt, false, "found no such table"
	}}
}

// Other engines are asked each YDB clause independently. A rejection of
// STORE=COLUMN alone says nothing about an index grammar or TTL tiers.
func withForeignColumnStoreKeys(p plan, dialect, creation string) plan {
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
