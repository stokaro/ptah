package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/txrequire"
)

// TestAnalyze_AStatementTheEngineRefusesInATransaction pins each statement the
// PostgreSQL descriptor states PostgreSQL refuses inside a transaction block,
// in the spellings a migration writes, as measured on PostgreSQL 18.6, and
// names it in the finding.
func TestAnalyze_AStatementTheEngineRefusesInATransaction(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{sql: "VACUUM base", want: "VACUUM is refused inside a transaction block"},
		{sql: "vacuum (analyze) base", want: "VACUUM is refused inside a transaction block"},
		{sql: "VACUUM", want: "VACUUM is refused inside a transaction block"},
		{sql: "REINDEX INDEX CONCURRENTLY base_i", want: "REINDEX ... CONCURRENTLY is refused inside a transaction block"},
		{sql: "REINDEX (VERBOSE) TABLE CONCURRENTLY base", want: "REINDEX ... CONCURRENTLY is refused inside a transaction block"},
		{sql: "REINDEX SCHEMA app", want: "REINDEX SCHEMA, DATABASE or SYSTEM is refused inside a transaction block"},
		{sql: "REINDEX DATABASE app", want: "REINDEX SCHEMA, DATABASE or SYSTEM is refused inside a transaction block"},
		{sql: "CREATE DATABASE reporting", want: "CREATE DATABASE is refused inside a transaction block"},
		{sql: "DROP DATABASE IF EXISTS reporting", want: "DROP DATABASE is refused inside a transaction block"},
		{sql: "ALTER DATABASE app SET TABLESPACE fast", want: "ALTER DATABASE ... SET TABLESPACE is refused inside a transaction block"},
		{sql: "ALTER SYSTEM SET work_mem = '4MB'", want: "ALTER SYSTEM is refused inside a transaction block"},
		{sql: "CREATE TABLESPACE fast LOCATION '/data'", want: "CREATE TABLESPACE is refused inside a transaction block"},
		{sql: "DROP TABLESPACE fast", want: "DROP TABLESPACE is refused inside a transaction block"},
		{sql: "DISCARD ALL", want: "DISCARD ALL is refused inside a transaction block"},
	}
	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)

			result := analyze("CREATE TABLE base (id int)", test.sql)

			c.Assert(result.RequiresAutocommit(), qt.IsTrue)
			c.Assert(result.Findings, qt.HasLen, 1)
			c.Assert(result.Findings[0].Reason, qt.Equals, txrequire.ReasonRefusedStatement)
			c.Assert(result.Findings[0].Statement.Index, qt.Equals, 1)
			c.Assert(result.Findings[0].Message, qt.Equals, test.want)
		})
	}
}

// TestAnalyze_AStatementTheEngineRunsInATransaction pins the controls PostgreSQL
// 18.6 accepts inside a transaction block beside the refused ones: an ANALYZE,
// a REINDEX of one index or table without CONCURRENTLY, an ALTER DATABASE that
// sets a parameter, a DISCARD of plans, and a word that only spells a refused
// one inside a literal or a name.
func TestAnalyze_AStatementTheEngineRunsInATransaction(t *testing.T) {
	for _, sql := range []string{
		"ANALYZE base",
		"REINDEX INDEX base_i",
		"REINDEX TABLE base",
		"ALTER DATABASE app SET default_tablespace = fast",
		"DISCARD PLANS",
		"INSERT INTO notes (body) VALUES ('VACUUM')",
		`SELECT 1 AS "vacuum"`,
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(analyze(sql).RequiresAutocommit(), qt.IsFalse)
		})
	}
}

// TestAnalyze_AnEngineThatStatesNoRefusalIsNotToldOfOne pins that the rule
// is the engine's: MySQL states no statement it refuses inside a transaction,
// and SQLite states VACUUM and not REINDEX, measured with sqlite3 3.51.
func TestAnalyze_AnEngineThatStatesNoRefusalIsNotToldOfOne(t *testing.T) {
	tests := []struct {
		dialect string
		sql     string
		want    bool
	}{
		{dialect: platform.MySQL, sql: "CREATE DATABASE reporting", want: false},
		{dialect: platform.SQLite, sql: "VACUUM", want: true},
		{dialect: platform.SQLite, sql: "VACUUM INTO 'backup.db'", want: true},
		{dialect: platform.SQLite, sql: "REINDEX", want: false},
	}
	for _, test := range tests {
		t.Run(test.dialect+" "+test.sql, func(t *testing.T) {
			c := qt.New(t)

			result := txrequire.Analyze(test.dialect, capability.Capabilities{}, []txrequire.Statement{{SQL: test.sql}})

			c.Assert(result.RequiresAutocommit(), qt.Equals, test.want)
		})
	}
}
