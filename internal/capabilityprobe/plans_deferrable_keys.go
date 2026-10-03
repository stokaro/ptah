package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withDeferrableKeys adds the question whether a key can defer its check.
//
// It is a key of its own rather than part of deferrable_constraints because
// the engines that defer a foreign key do not all defer a key: YugabyteDB
// 2026.1.2 defers a foreign key and answers `DEFERRABLE unique constraints are
// not supported yet` to the statement here. A UNIQUE added to a table that
// exists is the smallest statement that carries the clause, and every dialect
// is asked it, so a refusal is a measured answer rather than an assumed one
// (stokaro/ptah#3824).
func withDeferrableKeys(p plan, dialect string) plan {
	const add = "ALTER TABLE dkt ADD CONSTRAINT dkt_uq UNIQUE (id) DEFERRABLE INITIALLY DEFERRED"
	var table string
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		table = postgresFamilySpelling(dialect).table("dkt", "n int, id int", "n")
	case platform.ClickHouse:
		table = clickHouseSpelling.table("dkt", "n Int64, id Int64", "n")
	case platform.Oracle:
		table = "CREATE TABLE dkt (n NUMBER(10), id NUMBER(10))"
	case platform.YDB:
		table = ydbSpelling.table("dkt", "n Int64 NOT NULL, id Int64", "n")
	case platform.SQLite:
		// SQLite adds no constraint to a table that exists, so the key is
		// asked in the CREATE TABLE.
		p.experiments = append(p.experiments, acceptance(capability.DeferrableKeys, nil,
			"CREATE TABLE dkt (id INTEGER, CONSTRAINT dkt_uq UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)"))
		return p
	default:
		table = "CREATE TABLE dkt (n int, id int)"
	}
	p.experiments = append(p.experiments, acceptance(capability.DeferrableKeys, []string{table}, add))
	return p
}
