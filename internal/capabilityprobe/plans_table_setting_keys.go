package capabilityprobe

import (
	"context"
	"fmt"
	"path"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// withTableSettingKeys adds the questions the YDB planner decides a row
// table's settings with: whether it changes how the table splits into
// partitions, its read replicas, and its key bloom filter.
//
// On YDB each is asked in YDB's spelling, `ALTER TABLE ... SET (...)`, and the
// setting is then read back through Ptah's reader, since no SQL describes a
// table's settings. The settings are YDB's own, so every other engine is asked
// the same statement and its refusal is the measurement, as with an index's
// partitioning.
func withTableSettingKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == platform.YDB {
		p.experiments = append(p.experiments, ydbTableSettingExperiments()...)
		return p
	}
	spelling, ok := typeKeySpellingFor(normalized)
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

// ydbTableSettingExperiments sets each setting on a table of its own and reads
// it back.
func ydbTableSettingExperiments() []experiment {
	t := ydbSpelling
	wants := map[capability.Capability]struct {
		expectation string
		want        func(catalog.Table) bool
	}{
		capability.PartitioningOptions: {
			expectation: "the table to read back as splitting by load with a minimum of 3 partitions",
			want: func(table catalog.Table) bool {
				read := readSettings(table)
				return read != nil && read.ByLoad != nil && *read.ByLoad && read.MinPartitions == 3
			},
		},
		capability.ReadReplicas: {
			expectation: "the table to read back with one read replica in every availability zone",
			want: func(table catalog.Table) bool {
				read := readSettings(table)
				return read != nil && read.ReadReplicas == "PER_AZ:1"
			},
		},
		capability.KeyBloomFilter: {
			expectation: "the table to read back with a key bloom filter",
			want: func(table catalog.Table) bool {
				read := readSettings(table)
				return read != nil && read.KeyBloomFilter != nil && *read.KeyBloomFilter
			},
		},
	}
	settings := tableSettingStatements()
	experiments := make([]experiment, 0, len(settings))
	for _, setting := range settings {
		read := wants[setting.key]
		experiments = append(experiments, proven(setting.key, schemaChange{
			setup:  []string{t.table(setting.table, "id Uint64 NOT NULL, n Int64", "id")},
			change: []string{fmt.Sprintf("ALTER TABLE %s SET (%s)", setting.table, setting.clause)},
			after:  []check{ydbDescribedTable(setting.table, read.expectation, read.want)},
		}))
	}
	return experiments
}

// ydbDescribedTable is a check that reads a table back through Ptah's own YDB
// reader, scoped to the probe's directory, and holds when want answers true
// for what it read.
func ydbDescribedTable(table, expectation string, want func(catalog.Table) bool) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read the settings of table %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, table))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Tables {
				if found.Name != table {
					continue
				}
				read := readSettings(found)
				if read == nil || read.IsZero() {
					return attempt, want(found), "read YDB's defaults"
				}
				return attempt, want(found), "read " + strings.Join(ydbpartition.CreateClause(read), ", ")
			}
			return attempt, false, "found no such table"
		},
	}
}

// readSettings is the settings the reader found a table holds, as the YDB
// owner's observed facet, or nil for a table holding YDB's defaults.
func readSettings(table catalog.Table) *ydbschema.TablePartitioning {
	read, found, err := schemaext.FacetAs[*ydbschema.ObservedTablePartitioning](table.Facets, ydbschema.TablePartitioningKind)
	if err != nil || !found {
		return nil
	}
	return &read.TablePartitioning
}
