package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withVirtualGeneratedColumns adds the question whether a column can be
// declared GENERATED ALWAYS AS (<expr>) VIRTUAL.
//
// It is a key of its own rather than part of generated_columns because the
// engines that build a STORED column do not all build a VIRTUAL one:
// PostgreSQL 17 and YugabyteDB 2026.1 answer `syntax error at or near
// "VIRTUAL"`, where PostgreSQL 18 and CockroachDB build it. Every dialect is
// asked the SQL-standard clause, so a refusal is a measured answer. ClickHouse
// computes a column on read as `ALIAS <expr>`, and SQL Server as `AS (<expr>)`
// without PERSISTED; the key names neither spelling.
func withVirtualGeneratedColumns(p plan, dialect string) plan {
	var create string
	switch dialect {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		create = tableSpelling{}.table("gvx", "n int, g int GENERATED ALWAYS AS (n + 1) VIRTUAL", "n")
	case platform.ClickHouse:
		create = clickHouseSpelling.table("gvx", "n Int64, g Int64 GENERATED ALWAYS AS (n + 1) VIRTUAL", "n")
	case platform.Oracle:
		create = "CREATE TABLE gvx (n NUMBER(10), g NUMBER(10) GENERATED ALWAYS AS (n + 1) VIRTUAL)"
	case platform.SQLite:
		create = "CREATE TABLE gvx (n INTEGER, g INTEGER GENERATED ALWAYS AS (n + 1) VIRTUAL)"
	default:
		create = "CREATE TABLE gvx (n int, g int GENERATED ALWAYS AS (n + 1) VIRTUAL)"
	}
	p.experiments = append(p.experiments, acceptance(capability.VirtualGeneratedColumns, nil, create))
	return p
}
