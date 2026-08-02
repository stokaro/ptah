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

	"github.com/stokaro/ptah/core/platform"
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
