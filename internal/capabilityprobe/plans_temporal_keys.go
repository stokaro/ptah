package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withTemporalKeys adds the question whether a primary key can be declared
// WITHOUT OVERLAPS (stokaro/ptah#6330).
//
// A primary key is the smallest statement that carries the clause, and the
// foreign key's PERIOD needs a temporal key to reference, so one question
// stands for both: a server that builds the key and records it is the server
// that takes PERIOD. Every dialect is asked, so a refusal is a measured answer.
//
// The key's columns are two ranges. PostgreSQL builds the key on a GiST index,
// and a scalar column in it needs the btree_gist extension, which a probe
// cannot assume; range columns take the index with no extension. A server that
// has no range type is asked with two integer columns, which it refuses at the
// clause whatever the types are. On PostgreSQL the answer is the catalog's, not
// the statement's: the key is decided true only where pg_constraint.conperiod
// records it.
func withTemporalKeys(p plan, dialect string) plan {
	const add = "ALTER TABLE tkt ADD CONSTRAINT tkt_pk PRIMARY KEY (a, b WITHOUT OVERLAPS)"
	switch dialect {
	case platform.Postgres, platform.YugabyteDB:
		p.experiments = append(p.experiments, recordedExperiment(capability.TemporalKeys,
			[]string{tableSpelling{}.table("tkt", "a int4range, b int4range", "a")}, add,
			"SELECT COUNT(*) FROM pg_constraint WHERE conname = 'tkt_pk' AND conperiod"))
		return p
	case platform.CockroachDB:
		p.experiments = append(p.experiments, acceptance(capability.TemporalKeys,
			[]string{tableSpelling{}.table("tkt", "a int, b int", "a")}, add))
		return p
	case platform.ClickHouse:
		p.experiments = append(p.experiments, acceptance(capability.TemporalKeys,
			[]string{clickHouseSpelling.table("tkt", "a Int64, b Int64", "a")}, add))
		return p
	case platform.Oracle:
		p.experiments = append(p.experiments, acceptance(capability.TemporalKeys,
			[]string{"CREATE TABLE tkt (a NUMBER(10), b NUMBER(10))"}, add))
		return p
	case platform.SQLite:
		// SQLite adds no constraint to a table that exists, so the key is
		// asked in the CREATE TABLE.
		p.experiments = append(p.experiments, acceptance(capability.TemporalKeys, nil,
			"CREATE TABLE tkt (a INTEGER, b INTEGER, CONSTRAINT tkt_pk PRIMARY KEY (a, b WITHOUT OVERLAPS))"))
		return p
	default:
		p.experiments = append(p.experiments, acceptance(capability.TemporalKeys,
			[]string{"CREATE TABLE tkt (a int NOT NULL, b int NOT NULL)"}, add))
		return p
	}
}
