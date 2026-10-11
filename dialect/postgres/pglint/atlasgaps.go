package pglint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/txrequire"
)

// The rules in this file close the PostgreSQL gaps a refresh of the Atlas
// analyzer comparison found (stokaro/ptah#2972). Each one reports a
// consequence no existing rule named; migration lint's comparison-gap rules
// record the method and the MySQL half.
//
// POSTGRESQL, MEASURED
//
// Every PostgreSQL claim below was measured on PostgreSQL 18.6 by running the
// statement inside a transaction and reading pg_locks for the relation while
// the transaction still held it. A committed statement has released its locks,
// so asking afterwards reports nothing and reads exactly like a lock-free
// operation.
//
//	CREATE INDEX on a partitioned parent   ShareLock on the parent AND on every
//	                                       partition                      PG108
//	ADD CONSTRAINT … EXCLUDE               AccessExclusiveLock            PG109
//	DROP CONSTRAINT pkey, ADD PRIMARY KEY  AccessExclusiveLock            PG312
//	REPLICA IDENTITY FULL                  AccessExclusiveLock            PG314
//	SET (autovacuum_enabled = false)       ShareUpdateExclusiveLock       PG320
//
// PG108 carries one more measurement, and it is the reason the rule exists
// beside PG101 rather than being folded into it: `CREATE INDEX CONCURRENTLY`
// on a partitioned table is refused outright -- `ERROR: cannot create index on
// partitioned table "p" concurrently`. PG101's remedy is to add CONCURRENTLY,
// which cannot be done here, so a reader who follows it writes a statement the
// server rejects.
//
// PG320 is the one rule here whose hazard is not a lock or a rewrite. The
// statement is cheap; what it costs is paid later, by the rows nothing
// reclaims.

// atlasGapExcludeConstraint reports an EXCLUDE constraint being added.
func atlasGapExcludeConstraint(w []string) bool {
	return lintkit.IsAlterTable(w) && lintkit.HasWordSeq(w, "EXCLUDE", "USING")
}

// atlasGapReplicaIdentity reports REPLICA IDENTITY being set to FULL or
// NOTHING.
//
// DEFAULT and USING INDEX are not reported: they keep a row identity logical
// replication can use, which is the property the hazard is about.
func atlasGapReplicaIdentity(w []string) bool {
	if !lintkit.IsAlterTable(w) || !lintkit.HasWordSeq(w, "REPLICA", "IDENTITY") {
		return false
	}
	return lintkit.HasWordSeq(w, "REPLICA", "IDENTITY", "FULL") ||
		lintkit.HasWordSeq(w, "REPLICA", "IDENTITY", "NOTHING")
}

// atlasGapDisablesAutovacuum reports autovacuum being turned off for a table.
//
// It reads the joined statement rather than the word list because
// `autovacuum_enabled=false`, `autovacuum_enabled = false` and
// `autovacuum_enabled =false` tokenize differently and mean the same thing.
func atlasGapDisablesAutovacuum(canonical string) bool {
	text := strings.ToUpper(canonical)
	if !strings.Contains(text, "AUTOVACUUM_ENABLED") {
		return false
	}
	compact := strings.NewReplacer(" ", "", "\t", "").Replace(text)
	return strings.Contains(compact, "AUTOVACUUM_ENABLED=FALSE") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED=OFF") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED='FALSE'") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED='OFF'") ||
		strings.Contains(compact, "AUTOVACUUM_ENABLED=0")
}

// atlasGapPostgresRules holds PG108, PG109, PG312, PG314 and PG320.
//
// PG312 is the Atlas identifier. Ptah's own PG312P, a different rule about a
// SECURITY DEFINER routine that does not pin search_path, keeps its code: the
// trailing P is what marks a rule of ours inside an Atlas family, and the two
// stay separately selectable, suppressible and reportable.
func atlasGapPostgresRules() []lintkit.Rule {
	return []lintkit.Rule{
		{
			Rule: lintext.Rule{
				Code:     "PG108",
				Title:    "index on a partitioned table locks every partition",
				Severity: lintext.Warning,
				Targets:  []string{"postgres"},
				Subsumes: []string{"PG101"},
				Input:    lintext.InputBaselineRefinement,
			},
			BaselineSubjects: partitionedIndexStateRequests,
			File: func(file *lintkit.File) []lintext.Violation {
				var findings []lintext.Violation
				for _, site := range partitionedIndexSites(file) {
					if !site.partitionedInFile && (!site.earlier || !file.Baseline.Partitioned(site.target.Normalized)) {
						continue
					}
					stmt := &file.Statements[site.index]
					findings = append(findings, lintext.Violation{Statement: site.index, Message: partitionedIndexMessage(stmt, site.target.Name)})
				}
				return findings
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "PG109",
				Title:    "exclusion constraint added under an exclusive lock",
				Severity: lintext.Warning,
				Targets:  []string{"postgres"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !atlasGapExcludeConstraint(stmt.Words) {
					return false, ""
				}
				return true, "adding an EXCLUDE constraint holds an ACCESS EXCLUSIVE lock while it builds the " +
					"backing index and checks every existing row against it, so reads and writes both stop for the " +
					"whole scan; there is no CONCURRENTLY form for it, so build the index separately with " +
					"CREATE INDEX CONCURRENTLY and add the constraint USING that index"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "PG312",
				Title:    "primary key redefined under an exclusive lock",
				Severity: lintext.Warning,
				Targets:  []string{"postgres"},
				Subsumes: []string{"PG104"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.RedefinesPrimaryKey(stmt.Words) {
					return false, ""
				}
				if lintkit.HasWordSeq(stmt.Words, "USING", "INDEX") {
					// The index already exists, so nothing is built under the
					// lock; the statement is a catalog edit and the expensive
					// half was the CREATE INDEX CONCURRENTLY before it.
					return false, ""
				}
				return true, "replacing the primary key builds the new unique index while holding an ACCESS " +
					"EXCLUSIVE lock, so reads and writes both stop for the whole build rather than for a catalog " +
					"edit; build the index first with CREATE INDEX CONCURRENTLY and add the constraint USING INDEX"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "PG314",
				Title:    "replica identity weakened for logical replication",
				Severity: lintext.Warning,
				Targets:  []string{"postgres"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !atlasGapReplicaIdentity(stmt.Words) {
					return false, ""
				}
				if lintkit.HasWordSeq(stmt.Words, "REPLICA", "IDENTITY", "NOTHING") {
					return true, "REPLICA IDENTITY NOTHING stops the table publishing any row identity, so a " +
						"logical replication subscriber cannot apply an UPDATE or DELETE from it and the change " +
						"is silently lost downstream; the statement itself takes an ACCESS EXCLUSIVE lock"
				}
				return true, "REPLICA IDENTITY FULL makes every UPDATE and DELETE write every column of the old " +
					"row into the WAL, so replication volume grows with the row width and a subscriber matches " +
					"rows by a full scan rather than by key; the statement itself takes an ACCESS EXCLUSIVE lock"
			},
		},
		{
			Rule: lintext.Rule{
				Code:     "PG320",
				Title:    "autovacuum disabled for a table",
				Severity: lintext.Warning,
				Targets:  []string{"postgres"},
			},
			Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !atlasGapDisablesAutovacuum(stmt.Canonical) {
					return false, ""
				}
				return true, "disabling autovacuum leaves dead rows for nothing to reclaim, so the table and its " +
					"indexes keep growing, planner statistics stop being refreshed, and the transaction id horizon " +
					"stops advancing for it; the statement is cheap -- it takes only a SHARE UPDATE EXCLUSIVE " +
					"lock -- and the cost is paid later, so pair it with the vacuum schedule that replaces it"
			},
		},
	}
}

// partitionedIndexSite is one CREATE INDEX that PG108 judges.
type partitionedIndexSite struct {
	index  int
	target lintkit.TableReference
	// partitionedInFile reports a target this file creates PARTITION BY.
	partitionedInFile bool
	// earlier reports a target this file does not create, so only the state
	// the version starts from can say whether it is partitioned.
	earlier bool
}

// partitionedIndexSites lists the index builds of an up file that PG108 can
// judge: every CREATE INDEX except the ON ONLY form, which builds the parent's
// index alone and is the first step of the remedy this rule names, and a
// TimescaleDB per-chunk build, which a partitioned table refuses outright
// (`unrecognized parameter namespace "timescaledb"`, measured on PostgreSQL
// 18.6 with TimescaleDB 2.30.1 installed). It is the
// one walk both halves of the rule read, the state request and the report, so
// the two cannot disagree about which statements are candidates.
func partitionedIndexSites(file *lintkit.File) []partitionedIndexSite {
	if !file.IsUp {
		return nil
	}
	created := make(map[string]bool)
	partitioned := make(map[string]bool)
	var sites []partitionedIndexSite
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if ref := lintkit.CreatedTableRef(stmt.Words); ref != "" {
			created[ref] = true
			if lintkit.HasWordSeq(stmt.Words, "PARTITION", "BY") {
				partitioned[ref] = true
			}
			continue
		}
		if !lintkit.IsCreateIndex(stmt.Words) {
			continue
		}
		target, only := createIndexTarget(stmt.Words, stmt.SourceWords)
		if only || target.Normalized == "" || txrequire.PerChunkIndexBuild(stmt.Words) {
			continue
		}
		sites = append(sites, partitionedIndexSite{
			index:             i,
			target:            target,
			partitionedInFile: lintkit.RefersToCreated(partitioned, target.Normalized),
			earlier:           !lintkit.RefersToCreated(created, target.Normalized),
		})
	}
	return sites
}

// partitionedIndexStateRequests is PG108's answer to which statements want the
// starting state: the index builds on a table an earlier migration created.
func partitionedIndexStateRequests(file *lintkit.File) []int {
	var indexes []int
	for _, site := range partitionedIndexSites(file) {
		if site.earlier {
			indexes = append(indexes, site.index)
		}
	}
	return indexes
}

// partitionedIndexMessage is PG108's message, spelling the table the way the
// migration does.
//
// Measured on PostgreSQL 18.6, on a parent with a partition that is itself
// partitioned: a plain build takes SHARE on the parent and on every partition
// at every level for the whole build, and an INSERT into any of them waits;
// CONCURRENTLY is refused on the parent, on the partitioned partition and with
// ON ONLY (`cannot create index on partitioned table ... concurrently`); the
// parent index created ON ONLY, each leaf built concurrently and attached,
// and the partitioned partition's own ON ONLY index attached in turn, leave
// every index valid.
func partitionedIndexMessage(stmt *lintext.Statement, table string) string {
	remedy := "build the index on each partition concurrently and attach it to a parent index created with ON ONLY"
	if slices.Contains(stmt.Words, "CONCURRENTLY") {
		return "PostgreSQL refuses CREATE INDEX CONCURRENTLY on the partitioned table " + table + "; " + remedy
	}
	return "building this index takes a SHARE lock on " + table + " and on every one of its partitions at once, " +
		"so writes stop across the whole set rather than one partition at a time; CONCURRENTLY is refused on a " +
		"partitioned table, so the way to keep writes going is to " + remedy
}
