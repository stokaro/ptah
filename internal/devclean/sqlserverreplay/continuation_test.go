package sqlserverreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// TestGuard_JudgesTheStatementAfterAnAsteriskOrABareVariable holds a refused
// statement right after a part that ends a statement though it reads as one
// that goes on: the asterisk of OUTPUT deleted.* or inserted.*, a bare @,
// which is a variable, and a view that CREATE SCHEMA creates. SQL Server 2025
// ran the statement after each one. SHUTDOWN is pinned here and never run
// live.
func TestGuard_JudgesTheStatementAfterAnAsteriskOrABareVariable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{name: "DROP DATABASE after OUTPUT deleted.*", statement: "DELETE FROM dbo.items OUTPUT deleted.* DROP DATABASE [x];", operation: "DROP DATABASE"},
		{
			name:      "an INSERT into another database after OUTPUT deleted.*",
			statement: "DELETE FROM dbo.items OUTPUT deleted.* INSERT INTO other.dbo.kept VALUES (1)",
			operation: `cross-database reference "other.dbo.kept"`,
		},
		{name: "SHUTDOWN after OUTPUT deleted.*", statement: "DELETE FROM dbo.items OUTPUT deleted.* SHUTDOWN WITH NOWAIT", operation: "SHUTDOWN server operation"},
		{
			name:      "DROP DATABASE after OUTPUT inserted.*",
			statement: "UPDATE dbo.items SET id = 2 OUTPUT inserted.* DROP DATABASE [other]",
			operation: "DROP DATABASE",
		},
		{name: "USE after the OUTPUT of an UPDATE", statement: "UPDATE dbo.items SET id = 2 OUTPUT inserted.* USE master", operation: "USE"},
		{name: "DROP DATABASE after a bare @", statement: "DECLARE @ int = 1 SELECT @ DROP DATABASE [other]", operation: "DROP DATABASE"},
		{
			name:      "sp_executesql after a bare @",
			statement: "DECLARE @ int = 1 SELECT @ EXEC sp_executesql N'INSERT other.dbo.kept VALUES (1)'",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{name: "USE after a bare @ assigned to itself", statement: "DECLARE @ int = 1 SET @ = @ USE master", operation: "USE"},
		{name: "SHUTDOWN after a bare @", statement: "SELECT @ SHUTDOWN WITH NOWAIT", operation: "SHUTDOWN server operation"},
		{name: "RECONFIGURE after a bare @", statement: "SELECT @ RECONFIGURE", operation: "RECONFIGURE server operation"},
		{name: "sp_configure after a bare @", statement: "SELECT @ EXEC sp_configure 'show advanced options', 1", operation: "EXEC/EXECUTE sublanguage"},
		{name: "KILL after a bare @", statement: "SELECT @ KILL 52", operation: "KILL server operation"},
		{name: "GRANT CONTROL SERVER after a bare @", statement: "SELECT @ GRANT CONTROL SERVER TO public", operation: "permission mutation"},
		{name: "CREATE LOGIN after a bare @", statement: "SELECT @ CREATE LOGIN l6688 WITH PASSWORD = 'x'", operation: "CREATE LOGIN"},
		{
			name:      "GRANT after a view CREATE SCHEMA creates",
			statement: "CREATE SCHEMA s9 CREATE VIEW v9 AS SELECT 1 AS a GRANT SELECT ON v9 TO public",
			operation: "permission mutation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_TrustedSQLJudgesTheStatementAfterAnAsteriskOrABareVariable keeps,
// under trust, the refusals above that protect the run. Trust lifts DROP
// DATABASE, CREATE LOGIN, an EXEC and a write into another database, by
// design, so those rows are not repeated here.
func TestGuard_TrustedSQLJudgesTheStatementAfterAnAsteriskOrABareVariable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{name: "SHUTDOWN after OUTPUT deleted.*", statement: "DELETE FROM dbo.items OUTPUT deleted.* SHUTDOWN WITH NOWAIT", operation: "SHUTDOWN server operation"},
		{name: "USE after the OUTPUT of an UPDATE", statement: "UPDATE dbo.items SET id = 2 OUTPUT inserted.* USE master", operation: "USE"},
		{name: "USE after a bare @ assigned to itself", statement: "DECLARE @ int = 1 SET @ = @ USE master", operation: "USE"},
		{name: "SHUTDOWN after a bare @", statement: "SELECT @ SHUTDOWN WITH NOWAIT", operation: "SHUTDOWN server operation"},
		{name: "RECONFIGURE after a bare @", statement: "SELECT @ RECONFIGURE", operation: "RECONFIGURE server operation"},
		{name: "KILL after a bare @", statement: "SELECT @ KILL 52", operation: "KILL server operation"},
		{name: "GRANT CONTROL SERVER after a bare @", statement: "SELECT @ GRANT CONTROL SERVER TO public", operation: "permission mutation"},
		{
			name:      "GRANT after a view CREATE SCHEMA creates",
			statement: "CREATE SCHEMA s9 CREATE VIEW v9 AS SELECT 1 AS a GRANT SELECT ON v9 TO public",
			operation: "permission mutation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{TrustedSQL: true}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// temporalCreate and temporalAlter are the temporal table forms SQL Server
// 2025 accepted. The END of ROW END closes no block.
const (
	temporalCreate = "CREATE TABLE dbo.tt (id int PRIMARY KEY, " +
		"vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, " +
		"vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL, PERIOD FOR SYSTEM_TIME (vf, vt)) " +
		"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tt_history))"
	temporalAlter = "ALTER TABLE dbo.ta ADD " +
		"vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL CONSTRAINT df_ta_vf DEFAULT SYSUTCDATETIME(), " +
		"vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL " +
		"CONSTRAINT df_ta_vt DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'), PERIOD FOR SYSTEM_TIME (vf, vt)"
)

// TestGuard_AcceptsATemporalTable replays the temporal forms with and
// without trust, alone and with a statement after them.
func TestGuard_AcceptsATemporalTable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "CREATE TABLE", statement: temporalCreate},
		{name: "ALTER TABLE ADD", statement: temporalAlter},
		{name: "CREATE TABLE and a statement after it", statement: temporalCreate + " CREATE INDEX ix ON dbo.tt (id)"},
		{name: "ALTER TABLE ADD and a statement after it", statement: temporalAlter + " ALTER TABLE dbo.ta SET (SYSTEM_VERSIONING = ON)"},
	}
	for _, test := range tests {
		for _, scope := range []replay.Scope{{}, {TrustedSQL: true}} {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)

				c.Assert(validate(test.statement, scope), qt.IsNil)
			})
		}
	}
}

// TestGuard_RefusesALedgerTable covers the ledger forms, whose TRANSACTION_ID
// END and SEQUENCE_NUMBER END close no block. They are read, and refused as a
// ledger table, which the realm cleanup cannot drop, trusted or not.
func TestGuard_RefusesALedgerTable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{
			name: "an updatable ledger table",
			statement: "CREATE TABLE dbo.tl (id int, " +
				"ts bigint GENERATED ALWAYS AS TRANSACTION_ID START HIDDEN NOT NULL, " +
				"te bigint GENERATED ALWAYS AS TRANSACTION_ID END HIDDEN NULL, " +
				"ss bigint GENERATED ALWAYS AS SEQUENCE_NUMBER START HIDDEN NOT NULL, " +
				"se bigint GENERATED ALWAYS AS SEQUENCE_NUMBER END HIDDEN NULL) " +
				"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tl_history), LEDGER = ON)",
		},
		{
			name: "an append-only ledger table",
			statement: "CREATE TABLE dbo.tla (id int, " +
				"ts bigint GENERATED ALWAYS AS TRANSACTION_ID START HIDDEN NOT NULL, " +
				"ss bigint GENERATED ALWAYS AS SEQUENCE_NUMBER START HIDDEN NOT NULL) " +
				"WITH (LEDGER = ON (APPEND_ONLY = ON))",
		},
	}
	for _, test := range tests {
		for _, scope := range []replay.Scope{{}, {TrustedSQL: true}} {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)

				c.Assert(validate(test.statement, scope), qt.ErrorMatches, refusal("LEDGER table"))
			})
		}
	}
}
