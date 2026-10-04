package capabilityprobe

import (
	"context"
	"fmt"
	"path"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
)

// withIndexKeys adds the questions the YDB planner decides an index change in
// place with: whether it renames an index, and whether it changes an index's
// partitioning.
//
// On YDB each is asked in YDB's spelling and then used: the renamed index and
// the partitioning are read back through Ptah's reader, since no SQL describes
// an index. Index partitioning is YDB's own setting, so every other engine is
// asked the same statement and its refusal is the measurement, as with GLOBAL
// ASYNC. A rename is not: PostgreSQL, MySQL and others rename an index in
// place, and the key says whether Ptah's planner plans a renamed index as a
// rename, which only YDB's does, so those engines declare it.
func withIndexKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == platform.YDB {
		p.experiments = append(p.experiments, ydbIndexKeyExperiments()...)
		return p
	}
	spelling, ok := typeKeySpellingFor(normalized)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, proven(capability.IndexPartitioning, schemaChange{
		setup:  []string{spelling.table("ikp", "n "+spelling.integer)},
		change: []string{"ALTER TABLE ikp ALTER INDEX ikp_n SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3)"},
	}))
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.IndexRename] = "the key names whether Ptah's planner plans an index whose name alone " +
		"changed as a rename in place, which only the YDB planner does; this server renaming an index or not " +
		"says nothing about what this dialect's planner plans"
	return p
}

// ydbIndexKeyExperiments asks YDB to rename an asynchronous covering index and
// to change an index's minimum partition count, and reads each back.
func ydbIndexKeyExperiments() []experiment {
	t := ydbSpelling
	return []experiment{
		proven(capability.IndexRename, schemaChange{
			setup:  []string{t.table("ikr", "id Int64 NOT NULL, n Int64, m Int64, INDEX ikr_a GLOBAL ASYNC ON (n) COVER (m)", "id")},
			change: []string{"ALTER TABLE ikr RENAME INDEX ikr_a TO ikr_b"},
			after: []check{
				ydbDescribedIndex("ikr", "ikr_b", "the index under its new name, still GLOBAL ASYNC and covering m",
					func(index catalog.Index) bool {
						return index.Method == "GLOBAL ASYNC" && slices.Equal(index.IncludeColumns, []string{"m"})
					}),
				ydbIndexAbsent("ikr", "ikr_a"),
			},
		}),
		proven(capability.IndexPartitioning, schemaChange{
			setup:  []string{t.table("ikp", "id Int64 NOT NULL, n Int64, INDEX ikp_n GLOBAL ON (n)", "id")},
			change: []string{"ALTER TABLE ikp ALTER INDEX ikp_n SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3)"},
			after: []check{
				ydbDescribedIndex("ikp", "ikp_n", "the index's minimum partition count to read back as 3",
					func(index catalog.Index) bool {
						return index.Partitioning != nil && index.Partitioning.MinPartitions == 3
					}),
			},
		}),
	}
}

// ydbIndexAbsent reads table through Ptah's YDB reader and holds when it has no
// index named index: a rename that kept the old index beside a new one is not
// a rename.
func ydbIndexAbsent(table, index string) check {
	return check{
		describes: fmt.Sprintf("no index %s on table %s", index, table),
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read the indexes of table %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, table))}
			db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
			if err != nil {
				attempt.ServerErr = err.Error()
				return attempt, false, "was refused"
			}
			attempt.Accepted = true
			for _, found := range db.Indexes {
				if found.TableName == table && found.Name == index {
					return attempt, false, "still read " + found.Definition
				}
			}
			return attempt, true, "found no such index"
		},
	}
}
