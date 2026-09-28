package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withInvisibleIndexes adds the question whether an index can be hidden from
// the optimizer (stokaro/ptah#3853).
//
// The MySQL family is asked in its own words, MySQL's INVISIBLE and MariaDB's
// IGNORED, and the key is decided by what STATISTICS records, not by
// acceptance alone. The PostgreSQL family, SQLite, ClickHouse and SQL Server
// are asked MySQL's spelling, which each refuses, so a refusal is a measured
// answer. Oracle declares the key instead: it takes `CREATE INDEX ... INVISIBLE`
// with a meaning of its own, which Ptah neither renders nor reads, so its
// acceptance would answer a different question.
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
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		table := postgresFamilySpelling(dialect).table("ivx", "n int", "n")
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, []string{table}, create))
	case platform.ClickHouse:
		table := clickHouseSpelling.table("ivx", "n Int64", "n")
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, []string{table}, create))
	default:
		p.experiments = append(p.experiments, acceptance(capability.InvisibleIndexes, setup, create))
	}
	return p
}
