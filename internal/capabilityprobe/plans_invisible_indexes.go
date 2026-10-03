package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withInvisibleIndexes adds the question whether an index can be hidden from
// the optimizer (stokaro/ptah#3853).
//
// The MySQL family and CockroachDB are asked in their own words, MySQL's
// INVISIBLE, MariaDB's IGNORED and CockroachDB's NOT VISIBLE, and the key is
// decided by what the statistics view records, not by acceptance alone.
// CockroachDB lists one statistics row per column of an index, the implicit
// key included, so its read-back counts index names. PostgreSQL, YugabyteDB,
// Spanner, SQLite, ClickHouse and SQL Server are asked MySQL's spelling, which
// each refuses, so a refusal is a measured answer; YDB is asked the word on
// its own ADD INDEX and refuses it the same way. Oracle declares the key
// instead: it takes `CREATE INDEX ... INVISIBLE` with a meaning of its own,
// which Ptah neither renders nor reads, so its acceptance would answer a
// different question.
func withInvisibleIndexes(p plan, dialect string) plan {
	const create = "CREATE INDEX ivx_k ON ivx (n) INVISIBLE"
	const statistics = "SELECT COUNT(*) FROM information_schema.STATISTICS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND INDEX_NAME = 'ivx_k' AND "
	setup := []string{"CREATE TABLE ivx (n int)"}
	switch dialect {
	case platform.MySQL:
		p.experiments = append(p.experiments,
			recordedExperiment(capability.InvisibleIndexes, setup, create, statistics+"IS_VISIBLE = 'NO'"))
	case platform.MariaDB:
		p.experiments = append(p.experiments, recordedExperiment(capability.InvisibleIndexes, setup,
			"CREATE INDEX ivx_k ON ivx (n) IGNORED", statistics+"IGNORED = 'YES'"))
	case platform.Oracle:
		if p.undecided == nil {
			p.undecided = make(map[capability.Capability]string)
		}
		p.undecided[capability.InvisibleIndexes] = "the key names the MySQL family's INVISIBLE and IGNORED " +
			"index; this server takes INVISIBLE with a meaning of its own, which Ptah neither renders nor " +
			"reads, so its acceptance would answer a different question"
	case platform.CockroachDB:
		table := postgresFamilySpelling(dialect).table("ivx", "n int", "n")
		p.experiments = append(p.experiments, recordedExperiment(capability.InvisibleIndexes, []string{table},
			"CREATE INDEX ivx_k ON ivx (n) NOT VISIBLE",
			"SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics "+
				"WHERE table_name = 'ivx' AND index_name = 'ivx_k' AND is_visible = 'NO'"))
	case platform.Postgres, platform.YugabyteDB, platform.Spanner:
		table := postgresFamilySpelling(dialect).table("ivx", "n int", "n")
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, []string{table}, create))
	case platform.ClickHouse:
		table := clickHouseSpelling.table("ivx", "n Int64", "n")
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, []string{table}, create))
	case platform.YDB:
		// YDB has no CREATE INDEX, so MySQL's word goes on YDB's own ADD
		// INDEX, where a refusal is the word's rather than the statement's.
		table := ydbSpelling.table("ivx", "id Int64 NOT NULL, n Int64", "id")
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, []string{table},
			"ALTER TABLE ivx ADD INDEX ivx_k GLOBAL ON (n) INVISIBLE"))
	default:
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, setup, create))
	}
	return p
}
