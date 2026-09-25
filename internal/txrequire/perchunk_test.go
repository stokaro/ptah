package txrequire_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/txrequire"
)

// timescaleCapabilities is what a connection to PostgreSQL with TimescaleDB
// installed carries.
var timescaleCapabilities = capability.Postgres16().With(capability.Hypertables, true)

func analyzeWith(caps capability.Capabilities, sql ...string) txrequire.Result {
	statements := make([]txrequire.Statement, 0, len(sql))
	for index, statement := range sql {
		statements = append(statements, txrequire.Statement{Index: index, Line: index + 1, SQL: statement})
	}
	return txrequire.Analyze(platform.Postgres, caps, statements)
}

// TestAnalyze_PerChunkIndexNeedsAutocommit pins the per-chunk build as a
// statement no transaction can hold. Measured on TimescaleDB 2.30.1 over
// PostgreSQL 18.6: inside BEGIN ... COMMIT it is refused with `cannot run
// inside a transaction block` (25001), for each spelling below that builds per
// chunk.
func TestAnalyze_PerChunkIndexNeedsAutocommit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "the parameter without a value", sql: "CREATE INDEX i ON h (c) WITH (timescaledb.transaction_per_chunk)"},
		{name: "unique, set to true", sql: "CREATE UNIQUE INDEX i ON h (c, t) WITH (timescaledb.transaction_per_chunk = true)"},
		{name: "a prefix of true", sql: "CREATE INDEX i ON h (c) WITH (timescaledb.transaction_per_chunk = t)"},
		{name: "a quoted on after another parameter", sql: `CREATE INDEX i ON h (c) WITH (fillfactor = 70, "timescaledb".transaction_per_chunk = 'on')`},
		{name: "IF NOT EXISTS", sql: "CREATE INDEX IF NOT EXISTS i ON h (c) WITH (timescaledb.transaction_per_chunk = yes)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := analyzeWith(timescaleCapabilities, "CREATE TABLE u (c int)", test.sql)

			c.Assert(result.Findings, qt.HasLen, 1)
			c.Assert(result.Findings[0].Reason, qt.Equals, txrequire.ReasonPerChunkIndex)
			c.Assert(result.Findings[0].Statement.Line, qt.Equals, 2)
			c.Assert(result.Findings[0].Remedy, qt.Contains, "no_transaction")
		})
	}
}

// TestAnalyze_PerChunkIndexControls are the statements that must stay
// transactional: the spellings that build like a plain index, the parameter
// outside WITH, and every per-chunk build on a server without TimescaleDB,
// which refuses the parameter itself (`unrecognized parameter namespace`) in
// a transaction or out of one.
func TestAnalyze_PerChunkIndexControls(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		sql  string
	}{
		{name: "set to false", caps: timescaleCapabilities, sql: "CREATE INDEX i ON h (c) WITH (timescaledb.transaction_per_chunk = false)"},
		{name: "set to off", caps: timescaleCapabilities, sql: "CREATE INDEX i ON h (c) WITH (timescaledb.transaction_per_chunk = off)"},
		{name: "set to 0", caps: timescaleCapabilities, sql: "CREATE INDEX i ON h (c) WITH (timescaledb.transaction_per_chunk = 0)"},
		{name: "another parameter alone", caps: timescaleCapabilities, sql: "CREATE INDEX i ON h (c) WITH (fillfactor = 70)"},
		{name: "the name in a predicate", caps: timescaleCapabilities, sql: "CREATE INDEX i ON h (c) WHERE c <> 'timescaledb.transaction_per_chunk'"},
		{name: "not an index", caps: timescaleCapabilities, sql: "CREATE TABLE v (c int) WITH (timescaledb.transaction_per_chunk)"},
		{name: "a server without TimescaleDB", caps: capability.Postgres16(), sql: "CREATE INDEX i ON h (c) WITH (timescaledb.transaction_per_chunk)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := analyzeWith(test.caps, "CREATE TABLE u (c int)", test.sql)

			c.Assert(result.RequiresAutocommit(), qt.IsFalse)
		})
	}
}
