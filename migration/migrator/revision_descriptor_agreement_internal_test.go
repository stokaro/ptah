package migrator

// White-box testing required: these are the unexported builders that turn a
// dialect's descriptor into revision-table SQL, and the values a migration
// sends are not observable through the public API without a live server of
// each engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRevisionTableDescriptors_AgreeWithTheSwitches pins what each built-in
// descriptor's revision table makes the builders write, as the per-dialect
// switches the descriptors replaced wrote it. The expected values were
// captured from those switches, so a descriptor that drifts from them fails
// here rather than on a server.
func TestRevisionTableDescriptors_AgreeWithTheSwitches(t *testing.T) {
	tests := []struct {
		dialect        string
		firstRow       string
		emptyIsNull    bool
		versionNumber  string
		engineClause   string
		noEngineClause bool
		update         string
		literal        string
		rolledBack     bool
	}{
		{
			dialect:        "postgres",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   "",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "$ptah$a\\b$ptah$",
			rolledBack:     true,
		},
		{
			dialect:        "mysql",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS SIGNED)",
			engineClause:   " ENGINE=InnoDB",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "X'615c62'",
			rolledBack:     true,
		},
		{
			dialect:        "mariadb",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS SIGNED)",
			engineClause:   " ENGINE=InnoDB",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "X'615c62'",
			rolledBack:     true,
		},
		{
			dialect:        "clickhouse",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   " ENGINE = MergeTree",
			noEngineClause: false,
			update:         "ALTER TABLE T\nUPDATE a = ?\nWHERE version = ?\nSETTINGS mutations_sync = 1",
			literal:        "'a\\\\b'",
			rolledBack:     false,
		},
		{
			dialect:        "sqlite",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   "",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "'a\\b'",
			rolledBack:     true,
		},
		{
			dialect:        "sqlserver",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   "",
			noEngineClause: true,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "N'a\\b'",
			rolledBack:     true,
		},
		{
			dialect:        "cockroachdb",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   "",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "$ptah$a\\b$ptah$",
			rolledBack:     true,
		},
		{
			dialect:        "yugabytedb",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   "",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "$ptah$a\\b$ptah$",
			rolledBack:     true,
		},
		{
			dialect:        "spanner",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS BIGINT)",
			engineClause:   "",
			noEngineClause: false,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "$ptah$a\\b$ptah$",
			rolledBack:     false,
		},
		{
			dialect:        "oracle",
			firstRow:       "FETCH FIRST 1 ROWS ONLY",
			emptyIsNull:    true,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS NUMBER(19))",
			engineClause:   "",
			noEngineClause: true,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "'a\\b'",
			rolledBack:     false,
		},
		{
			dialect:        "ydb",
			firstRow:       "LIMIT 1",
			emptyIsNull:    false,
			versionNumber:  "CAST(CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END AS Int64)",
			engineClause:   "",
			noEngineClause: true,
			update:         "UPDATE T\nSET a = ?\nWHERE version = ?",
			literal:        "'a\\\\b'u",
			rolledBack:     false,
		}}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisionFirstRowClause(test.dialect), qt.Equals, test.firstRow)
			c.Assert(emptyStringIsNull(test.dialect), qt.Equals, test.emptyIsNull)
			c.Assert(atlasVersionNumberExpressionFor(test.dialect), qt.Equals, test.versionNumber)
			c.Assert(revisionEngineClauseFor(test.dialect, ""), qt.Equals, test.engineClause)
			c.Assert(revisionTableHasNoEngineClause(test.dialect), qt.Equals, test.noEngineClause)
			c.Assert(revisionUpdateSQL(test.dialect, "T", "a = ?"), qt.Equals, test.update)
			c.Assert(atlasRevisionStringLiteral(test.dialect, `a\b`), qt.Equals, test.literal)
			c.Assert(migrationProgressRolledBack(test.dialect, MigrationTxModeFile), qt.Equals, test.rolledBack)
		})
	}
}
