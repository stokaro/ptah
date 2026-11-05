package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withIndexKeys adds the questions whether the planner renames an index in
// place and whether it changes an index's partitioning. Index partitioning is
// YDB's own setting, which the YDB provider measures on its own lines, so
// every engine here is asked the statement and its refusal is the
// measurement, as with GLOBAL ASYNC. A rename is not: PostgreSQL, MySQL and
// others rename an index in place, and the key says whether Ptah's planner
// plans a renamed index as a rename, which no built-in planner does, so those
// engines declare it.
func withIndexKeys(p plan, dialect string) plan {
	spelling, ok := typeKeySpellingFor(platform.NormalizeDialect(dialect))
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
		"changed as a rename in place, which no built-in planner does; this server renaming an index or not " +
		"says nothing about what this dialect's planner plans"
	return p
}
