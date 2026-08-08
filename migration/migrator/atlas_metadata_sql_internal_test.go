package migrator

// White-box testing required: these guards assert the generated SQL text of
// unexported builders. The behavior they protect cannot be observed through the
// public API on SQLite — its CAST is lenient, so a dot-prefixed metadata row
// casts to 0 instead of failing — while PostgreSQL and MySQL reject the same
// cast outright. Every exported-API test therefore passes with or without the
// guard; only the SQL text distinguishes them.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

// atlasMetadataNullGuard restates the expected CASE arm as a literal rather
// than referencing the production constant, so rewriting that constant is
// itself a test failure instead of a silently-agreeing tautology.
const atlasMetadataNullGuard = `CASE WHEN version LIKE '.%' THEN NULL ELSE version END`

func TestAtlasVersionNumberExpression_GuardsEveryDialectBranch(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		wantCast string
	}{
		{name: "mysql", dialect: "mysql", wantCast: "SIGNED"},
		{name: "mariadb", dialect: "mariadb", wantCast: "SIGNED"},
		{name: "postgres", dialect: "postgres", wantCast: "BIGINT"},
		{name: "sqlite", dialect: "sqlite", wantCast: "BIGINT"},
		{name: "sqlserver", dialect: "sqlserver", wantCast: "BIGINT"},
		{name: "unset dialect falls back to the default branch", dialect: "", wantCast: "BIGINT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			expression := atlasVersionNumberExpressionFor(tt.dialect)

			c.Assert(expression, qt.Contains, atlasMetadataNullGuard,
				qt.Commentf("dialect %q must NULL-guard the version cast; a bare CAST fails on strict dialects", tt.dialect))
			c.Assert(expression, qt.Contains, "CAST("+atlasMetadataNullGuard+" AS "+tt.wantCast+")")
		})
	}
}

// TestAtlasUnfilteredRevisionSQL_CarriesNullGuard pins the three Atlas-format
// statements that select over every revision row. They have no
// atlasMetadataRowPredicate of their own, so the cast guard is their only
// protection against a dot-prefixed metadata row.
func TestAtlasUnfilteredRevisionSQL_CarriesNullGuard(t *testing.T) {
	m := (&Migrator{}).WithRevisionTableFormat(RevisionTableFormatAtlas)

	tests := []struct {
		name string
		sql  string
	}{
		{name: "get version", sql: m.getVersionSQL()},
		{name: "count revisions above", sql: m.countRevisionsAboveSQL()},
		{name: "delete revisions above", sql: m.deleteRevisionsAboveSQL()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(tt.sql, qt.Contains, atlasMetadataNullGuard,
				qt.Commentf("%s selects over every revision row and must NULL-guard the cast", tt.name))
			c.Assert(tt.sql, qt.Not(qt.Contains), atlasMetadataRowPredicate,
				qt.Commentf("%s has no WHERE filter, which is why the cast guard is load-bearing", tt.name))
		})
	}
}

// TestAtlasFilteredRevisionSQL_ExcludesMetadataRows pins the complementary half:
// every Atlas-format statement that returns revision rows filters dot-prefixed
// versions out, so metadata rows never reach version math, status, or pending
// calculations.
func TestAtlasFilteredRevisionSQL_ExcludesMetadataRows(t *testing.T) {
	m := (&Migrator{}).WithRevisionTableFormat(RevisionTableFormatAtlas)

	tests := []struct {
		name string
		sql  string
	}{
		{name: "applied migrations", sql: m.getAppliedMigrationsSQL()},
		{name: "applied revisions", sql: m.getAppliedRevisionsSQL()},
		{name: "all revisions", sql: m.getRevisionsSQL()},
		{name: "dirty revision", sql: m.getDirtyRevisionSQL()},
		{name: "count revisions", sql: m.countRevisionsSQL()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(tt.sql, qt.Contains, atlasMetadataRowPredicate,
				qt.Commentf("%s must exclude dot-prefixed Atlas metadata rows", tt.name))
		})
	}
}

func TestAtlasRevisionSQL_TreatsCompletedRollbackAsDirty(t *testing.T) {
	c := qt.New(t)
	m := (&Migrator{}).WithRevisionTableFormat(RevisionTableFormatAtlas)

	c.Assert(m.getDirtyRevisionSQL(), qt.Contains,
		"COALESCE(operator_version, '') = 'Ptah/down'")
	c.Assert(m.getAppliedMigrationsSQL(), qt.Contains,
		"NOT (COALESCE(operator_version, '') = 'Ptah/down')")
	c.Assert(m.getAppliedRevisionsSQL(), qt.Contains,
		"NOT (COALESCE(operator_version, '') = 'Ptah/down')")
}

// TestPtahRevisionSQL_HasNoMetadataFilter pins that the guard is Atlas-format
// only: the native layout stores numeric versions in a BIGINT column, where a
// dot-prefixed version cannot exist, so it carries neither the predicate nor
// the cast guard.
func TestPtahRevisionSQL_HasNoMetadataFilter(t *testing.T) {
	c := qt.New(t)
	m := &Migrator{}

	for name, sql := range map[string]string{
		"get version":        m.getVersionSQL(),
		"applied migrations": m.getAppliedMigrationsSQL(),
		"all revisions":      m.getRevisionsSQL(),
		"count revisions":    m.countRevisionsSQL(),
		"applied revisions":  m.getAppliedRevisionsSQL(),
	} {
		c.Assert(sql, qt.Not(qt.Contains), atlasMetadataRowPredicate,
			qt.Commentf("%s (ptah layout) must not carry the Atlas metadata filter", name))
		c.Assert(sql, qt.Not(qt.Contains), atlasMetadataNullGuard,
			qt.Commentf("%s (ptah layout) must not carry the Atlas cast guard", name))
	}
}

// atlasRevisionsGuardTable and atlasRevisionsGuardSQLServerObject are the two
// caller-supplied strings atlasRevisionsTableDDL interpolates. Passing them
// explicitly keeps every row driven by the dialect alone.
const (
	atlasRevisionsGuardTable           = "`schema_migrations`"
	atlasRevisionsGuardSQLServerObject = "N'[dbo].[schema_migrations]'"
)

// TestAtlasRevisionsTableDDL_GuardsEveryDialectBranch pins the partial_hashes
// column for every dialect Ptah connects to, not just the one #950 reported.
// The bug was that ClickHouse had no branch and silently inherited a
// MySQL-shaped default declaring the column JSON; the same silence is waiting
// for the next dialect added without one, so the guard enumerates all of them.
//
// This runs without Docker, which matters because docker-compose.yaml pins
// ClickHouse 26, where the invalid DDL is accepted and only the round-trip half
// of the defect reproduces. The live fixture cannot see the DDL half on that
// image; this test can, on every image.
func TestAtlasRevisionsTableDDL_GuardsEveryDialectBranch(t *testing.T) {
	tests := []struct {
		name              string
		dialect           string
		wantPartialHashes string
		assert            func(c *qt.C, ddl string)
	}{
		{
			name:              "postgres gets a native JSONB column",
			dialect:           platform.Postgres,
			wantPartialHashes: "partial_hashes JSONB NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "executed_at TIMESTAMPTZ NOT NULL")
			},
		},
		{
			name:              "cockroachdb follows the postgres family",
			dialect:           platform.CockroachDB,
			wantPartialHashes: "partial_hashes JSONB NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "executed_at TIMESTAMPTZ NOT NULL")
			},
		},
		{
			name:              "yugabytedb follows the postgres family",
			dialect:           platform.YugabyteDB,
			wantPartialHashes: "partial_hashes JSONB NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "executed_at TIMESTAMPTZ NOT NULL")
			},
		},
		{
			name:              "mysql keeps the default JSON column",
			dialect:           platform.MySQL,
			wantPartialHashes: "partial_hashes JSON NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "CREATE TABLE IF NOT EXISTS "+atlasRevisionsGuardTable)
			},
		},
		{
			name:              "mariadb keeps the default JSON column",
			dialect:           platform.MariaDB,
			wantPartialHashes: "partial_hashes JSON NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "CREATE TABLE IF NOT EXISTS "+atlasRevisionsGuardTable)
			},
		},
		{
			name:              "sqlite keeps the default JSON column",
			dialect:           platform.SQLite,
			wantPartialHashes: "partial_hashes JSON NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "CREATE TABLE IF NOT EXISTS "+atlasRevisionsGuardTable)
			},
		},
		{
			name:              "spanner keeps the default JSON column",
			dialect:           platform.Spanner,
			wantPartialHashes: "partial_hashes JSON NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "CREATE TABLE IF NOT EXISTS "+atlasRevisionsGuardTable)
			},
		},
		{
			name:              "sqlserver stores the JSON document as text",
			dialect:           platform.SQLServer,
			wantPartialHashes: "partial_hashes NVARCHAR(MAX) NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "IF OBJECT_ID("+atlasRevisionsGuardSQLServerObject+", N'U') IS NULL")
			},
		},
		{
			// ClickHouse reads a trailing NULL as Nullable(T). A JSON column here
			// is asked for as Nullable(JSON): rejected outright by ClickHouse 24.x
			// (code 43) and, on later servers, accepted but silently storing `{}`
			// in place of the JSON null Ptah wrote. Pin the whole statement rather
			// than one column, so widening any other column has to be deliberate.
			name:              "clickhouse stores the JSON document as text",
			dialect:           platform.ClickHouse,
			wantPartialHashes: "partial_hashes TEXT NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Not(qt.Contains), "JSON",
					qt.Commentf("any JSON token here becomes Nullable(JSON) on ClickHouse"))
				c.Assert(ddl, qt.Equals, "CREATE TABLE IF NOT EXISTS "+atlasRevisionsGuardTable+` (
    version VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    type BIGINT NOT NULL DEFAULT 2,
    applied BIGINT NOT NULL DEFAULT 0,
    total BIGINT NOT NULL DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,
    execution_time BIGINT NOT NULL,
    error TEXT NULL,
    error_stmt TEXT NULL,
    hash VARCHAR(255) NOT NULL,
    partial_hashes TEXT NULL,
    operator_version VARCHAR(255) NOT NULL
)`)
			},
		},
		{
			name:              "unset dialect falls back to the default branch",
			dialect:           "",
			wantPartialHashes: "partial_hashes JSON NULL",
			assert: func(c *qt.C, ddl string) {
				c.Assert(ddl, qt.Contains, "CREATE TABLE IF NOT EXISTS "+atlasRevisionsGuardTable)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			ddl := atlasRevisionsTableDDL(tt.dialect, atlasRevisionsGuardTable, atlasRevisionsGuardSQLServerObject)

			c.Assert(ddl, qt.Contains, tt.wantPartialHashes,
				qt.Commentf("dialect %q must declare partial_hashes as %q", tt.dialect, tt.wantPartialHashes))
			c.Assert(ddl, qt.Contains, "operator_version",
				qt.Commentf("dialect %q must still carry the full Atlas revision layout", tt.dialect))
			tt.assert(c, ddl)
		})
	}
}

// TestCreateAtlasRevisionsTableSQL_ZeroValueMigratorDoesNotPanic pins the
// reason atlasRevisionsTableDDL takes a dialect instead of reading m.conn:
// the method dereferenced m.conn.Info() directly, so no assertion about its
// output could run without a live database.
func TestCreateAtlasRevisionsTableSQL_ZeroValueMigratorDoesNotPanic(t *testing.T) {
	c := qt.New(t)

	got := (&Migrator{}).createAtlasRevisionsTableSQL()

	c.Assert(got, qt.Contains, "CREATE TABLE IF NOT EXISTS")
	c.Assert(got, qt.Contains, "partial_hashes JSON NULL")
}

func TestRevisionUpdateSQL_ClickHouseUsesSynchronousMutation(t *testing.T) {
	c := qt.New(t)

	got := revisionUpdateSQL(platform.ClickHouse, "`revisions`", "applied = ?, total = ?")

	c.Assert(got, qt.Equals, `ALTER TABLE `+"`revisions`"+`
UPDATE applied = ?, total = ?
WHERE version = ?
SETTINGS mutations_sync = 1`)
}

func TestRevisionUpdateSQL_TransactionalDialectUsesUpdate(t *testing.T) {
	c := qt.New(t)

	got := revisionUpdateSQL(platform.Postgres, `"revisions"`, "applied = ?, total = ?")

	c.Assert(got, qt.Equals, `UPDATE "revisions"
SET applied = ?, total = ?
WHERE version = ?`)
}
