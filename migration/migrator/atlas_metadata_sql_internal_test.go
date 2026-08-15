package migrator

// White-box testing required: these guards assert the generated SQL text of
// unexported builders. The behavior they protect cannot be observed through the
// public API on SQLite — its CAST is lenient, so a dot-prefixed metadata row
// casts to 0 instead of failing — while PostgreSQL and MySQL reject the same
// cast outright. Every exported-API test therefore passes with or without the
// guard; only the SQL text distinguishes them.

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/revisiontable"
)

func TestWithAtlasRepeatableVersionsIsExplicitAndCloned(t *testing.T) {
	c := qt.New(t)
	versions := []int64{1, 1}
	repeatable, err := NewFSMigrationProvider(
		fstest.MapFS{
			"1_converted.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		WithMigrationDirFormat(MigrationDirFormatAtlas),
		WithAtlasRevisionVersions(map[int64]string{1: ""}),
		WithAtlasRepeatableVersions(versions),
	)
	c.Assert(err, qt.IsNil)
	versions[0] = 2
	c.Assert(repeatable.Migrations(), qt.HasLen, 1)
	c.Assert(repeatable.Migrations()[0].isAtlasRepeatable(), qt.IsTrue)

	ordinary, err := NewFSMigrationProvider(
		fstest.MapFS{
			"1_converted.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
		},
		WithMigrationDirFormat(MigrationDirFormatAtlas),
		WithAtlasRevisionVersions(map[int64]string{1: ""}),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(ordinary.Migrations(), qt.HasLen, 1)
	c.Assert(ordinary.Migrations()[0].isAtlasRepeatable(), qt.IsFalse)
}

func TestUnownedExactAtlasRevisionsAboveKeepsLoadedUnmappedMigration(t *testing.T) {
	c := qt.New(t)
	provider, err := NewFSMigrationProvider(
		fstest.MapFS{
			"2_loaded.sql":  {Data: []byte("SELECT 2;\n")},
			"10_target.sql": {Data: []byte("SELECT 10;\n")},
		},
		WithMigrationDirFormat(MigrationDirFormatAtlas),
		WithAtlasRevisionVersions(map[int64]string{10: "1"}),
	)
	c.Assert(err, qt.IsNil)
	m := &Migrator{
		migrationProvider:   provider,
		revisionTableFormat: RevisionTableFormatAtlas,
	}

	removed, err := m.unownedExactAtlasRevisionsAbove(
		[]MigrationRevision{{Version: 2, AtlasVersion: "2"}},
		m.migrationByVersion(10),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(removed, qt.HasLen, 0)
}

func TestUnownedExactAtlasRevisionsAboveRefusesWithoutSourceComparator(t *testing.T) {
	c := qt.New(t)
	provider, err := NewFSMigrationProvider(
		fstest.MapFS{
			"10_target.sql": {Data: []byte("SELECT 10;\n")},
		},
		WithMigrationDirFormat(MigrationDirFormatAtlas),
		WithAtlasRevisionVersions(map[int64]string{10: "1"}),
	)
	c.Assert(err, qt.IsNil)
	m := &Migrator{
		migrationProvider:   provider,
		revisionTableFormat: RevisionTableFormatAtlas,
	}

	removed, err := m.unownedExactAtlasRevisionsAbove(
		[]MigrationRevision{{Version: 0, AtlasVersion: "2", hasAtlasVersion: true}},
		m.migrationByVersion(10),
	)

	c.Assert(removed, qt.IsNil)
	c.Assert(err, qt.ErrorMatches,
		`cannot set Atlas revision: source order between retired exact identity "2" and target "1" is ambiguous`)
}

func TestUnownedExactAtlasRevisionsAbovePassesPersistedRoleFactsToComparator(t *testing.T) {
	c := qt.New(t)
	provider, err := NewFSMigrationProvider(
		fstest.MapFS{
			"10_target.sql": {Data: []byte("SELECT 10;\n")},
		},
		WithMigrationDirFormat(MigrationDirFormatAtlas),
		WithAtlasRevisionVersions(map[int64]string{10: "10"}),
	)
	c.Assert(err, qt.IsNil)
	var gotLeft, gotRight AtlasRevisionOrderIdentity
	m := &Migrator{
		migrationProvider:   provider,
		revisionTableFormat: RevisionTableFormatAtlas,
		atlasRevisionCompare: func(left, right AtlasRevisionOrderIdentity) (int, bool) {
			gotLeft = left
			gotRight = right
			return -1, true
		},
	}

	removed, err := m.unownedExactAtlasRevisionsAbove(
		[]MigrationRevision{{
			Version:         0,
			AtlasVersion:    "20",
			hasAtlasVersion: true,
			AtlasType:       AtlasRevisionTypeBaseline | AtlasRevisionTypeApplied,
			OperatorVersion: revisiontable.SourceIdentityOperatorVersion,
		}},
		m.migrationByVersion(10),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(removed, qt.HasLen, 0)
	c.Assert(gotLeft, qt.DeepEquals, AtlasRevisionOrderIdentity{
		RevisionVersion: "20",
		AtlasType:       AtlasRevisionTypeBaseline | AtlasRevisionTypeApplied,
		OperatorVersion: revisiontable.SourceIdentityOperatorVersion,
	})
	c.Assert(gotRight, qt.DeepEquals, AtlasRevisionOrderIdentity{
		RevisionVersion: "10",
		AtlasType:       AtlasRevisionTypeApplied,
		OperatorVersion: revisiontable.SourceIdentityOperatorVersion,
	})
}

// atlasMetadataNullGuard restates the expected CASE arm as a literal rather
// than referencing the production constant, so rewriting that constant is
// itself a test failure instead of a silently-agreeing tautology. It excludes
// metadata rows and Atlas repeatable version tokens from numeric casts.
const atlasMetadataNullGuard = `CASE WHEN version LIKE '.%' OR version = 'R' OR version LIKE '%R' THEN NULL ELSE version END`

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

func TestAtlasVersionNumberExpression_MapsOpaqueRevisionIdentitiesToRuntimeOrder(t *testing.T) {
	c := qt.New(t)
	m, err := NewFSMigrator(nil, fstest.MapFS{
		"0000000000000000010_plain.sql":  {Data: []byte("SELECT 1;")},
		"0000000000000000020_dotted.sql": {Data: []byte("SELECT 2;")},
		"0000000000000000030_named.sql":  {Data: []byte("SELECT 3;")},
		"0000000000000000040_repeat.sql": {Data: []byte("SELECT 4;")},
	}, WithMigrationDirFormat(MigrationDirFormatAtlas), WithAtlasRevisionVersions(map[int64]string{
		10: "01",
		20: "1.5",
		30: "x'y",
		40: "",
	}))
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(RevisionTableFormatAtlas)

	wantMapping := "CASE version WHEN '01' THEN 10 WHEN '1.5' THEN 20 WHEN 'x''y' THEN 30 WHEN '' THEN 40 " +
		"ELSE CAST(0 AS BIGINT) END"
	c.Assert(m.atlasVersionNumberExpression(), qt.Equals, wantMapping)
	c.Assert(m.getVersionSQL(), qt.Contains, wantMapping)
	c.Assert(m.countRevisionsAboveSQL(), qt.Contains, wantMapping)
	c.Assert(m.deleteRevisionsAboveSQL(), qt.Contains, wantMapping)
}

func TestAtlasVersionNumberExpression_UsesTypedZeroForRetiredExactHistory(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "mysql", dialect: platform.MySQL, want: "CAST(0 AS SIGNED)"},
		{name: "mariadb", dialect: platform.MariaDB, want: "CAST(0 AS SIGNED)"},
		{name: "default", dialect: "", want: "CAST(0 AS BIGINT)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasRetiredVersionNumberExpressionFor(tt.dialect), qt.Equals, tt.want)
		})
	}
}

func TestAtlasRuntimeVersionKeepsRetiredExactHistorySeparateFromNativeParsing(t *testing.T) {
	c := qt.New(t)
	exact, err := NewFSMigrator(
		nil,
		fstest.MapFS{"2_plain.sql": {Data: []byte("SELECT 2;")}},
		WithMigrationDirFormat(MigrationDirFormatAtlas),
		WithAtlasRevisionVersions(map[int64]string{}),
	)
	c.Assert(err, qt.IsNil)
	exact = exact.WithRevisionTableFormat(RevisionTableFormatAtlas)

	version, err := exact.atlasRuntimeVersion("foo")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))
	c.Assert(exact.atlasVersionNumberExpression(), qt.Equals,
		"CASE version WHEN '2' THEN 2 ELSE CAST(0 AS BIGINT) END")
	version, err = exact.atlasRuntimeVersion("2")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))
	version, err = exact.atlasRuntimeVersion("3")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))

	native := (&Migrator{}).WithRevisionTableFormat(RevisionTableFormatAtlas)
	version, err = native.atlasRuntimeVersion("2")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))
	_, err = native.atlasRuntimeVersion("foo")
	c.Assert(err, qt.ErrorMatches,
		`Atlas revision version "foo" is not a numeric or repeatable Ptah migration version: .*`)
}

func TestAtlasRevisionStringLiteralPreservesQuotesAndBackslashesByDialect(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		value   string
		want    string
	}{
		{name: "postgres apostrophe", dialect: platform.Postgres, value: `x'y`, want: `'x''y'`},
		{name: "postgres backslash", dialect: platform.Postgres, value: `x'\y`, want: `$ptah$x'\y$ptah$`},
		{name: "postgres empty", dialect: platform.Postgres, value: "", want: `''`},
		{
			name:    "postgres delimiter collision",
			dialect: platform.Postgres,
			value:   `x\$ptah$y\$ptah1$z`,
			want:    `$ptah2$x\$ptah$y\$ptah1$z$ptah2$`,
		},
		{name: "cockroachdb backslash", dialect: platform.CockroachDB, value: `x\y`, want: `$ptah$x\y$ptah$`},
		{name: "yugabytedb backslash", dialect: platform.YugabyteDB, value: `x\y`, want: `$ptah$x\y$ptah$`},
		{name: "spanner backslash", dialect: platform.Spanner, value: `x\y`, want: `$ptah$x\y$ptah$`},
		{name: "mysql quote and backslash", dialect: platform.MySQL, value: `x'\y`, want: `X'78275c79'`},
		{name: "mysql empty", dialect: platform.MySQL, value: "", want: `X''`},
		{name: "mariadb quote and backslash", dialect: platform.MariaDB, value: `x'\y`, want: `X'78275c79'`},
		{name: "mariadb empty", dialect: platform.MariaDB, value: "", want: `X''`},
		{name: "clickhouse", dialect: platform.ClickHouse, value: `x'\y`, want: `'x''\\y'`},
		{name: "sqlite", dialect: platform.SQLite, value: `x'\y`, want: `'x''\y'`},
		{name: "sqlserver unicode", dialect: platform.SQLServer, value: `x'猫`, want: `N'x''猫'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(atlasRevisionStringLiteral(tt.dialect, tt.value), qt.Equals, tt.want)
		})
	}
}

func TestAtlasExactIdentityPredicateUsesSafePostgresMetadataLiteral(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "cockroachdb", dialect: platform.CockroachDB},
		{name: "yugabytedb", dialect: platform.YugabyteDB},
		{name: "spanner", dialect: platform.Spanner},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			predicate := atlasExactIdentityRowPredicateFor(tt.dialect)

			c.Assert(predicate, qt.Equals,
				`version <> '.atlas_cloud_identifier'`)
		})
	}
}

func TestAtlasExactIdentityPredicateUsesSafeMySQLMetadataLiteral(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			predicate := atlasExactIdentityRowPredicateFor(tt.dialect)

			c.Assert(predicate, qt.Equals,
				`version <> X'2e61746c61735f636c6f75645f6964656e746966696572'`)
		})
	}
}

func TestAtlasVersionNumberExpression_UsesHighestRuntimeForRepeatedHistoricalIdentity(t *testing.T) {
	c := qt.New(t)
	m, err := NewFSMigrator(nil, fstest.MapFS{
		"0000000000000000010_baseline.sql": {Data: []byte("SELECT 1;")},
	}, WithMigrationDirFormat(MigrationDirFormatAtlas), WithAtlasRevisionVersions(map[int64]string{
		10: "2",
		20: "2",
	}))
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(RevisionTableFormatAtlas)

	expression := m.atlasVersionNumberExpression()

	c.Assert(expression, qt.Contains, "WHEN '2' THEN 20")
	c.Assert(expression, qt.Not(qt.Contains), "WHEN '2' THEN 10")
	c.Assert(expression, qt.Matches, `^CASE version WHEN '2' THEN 20 ELSE .+ END$`)
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

func TestAtlasFilteredRevisionSQL_IncludesRetiredDotIdentityAndExcludesKnownMetadata(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sqlserver", dialect: platform.SQLServer},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			m, err := NewFSMigrator(nil, fstest.MapFS{
				"10_dot.sql": {Data: []byte("SELECT 1;")},
			}, WithMigrationDirFormat(MigrationDirFormatAtlas), WithAtlasRevisionVersions(map[int64]string{
				10: ".foo",
			}))
			c.Assert(err, qt.IsNil)
			m = m.WithRevisionTableFormat(RevisionTableFormatAtlas)
			wantPredicate := atlasExactIdentityRowPredicateFor(tt.dialect)
			c.Assert(wantPredicate, qt.Equals,
				"version <> "+atlasRevisionStringLiteral(tt.dialect, atlasCloudIdentifierVersion))
			// This Migrator has no live connection, so its generated statements
			// take the default dialect. The helper assertion above is the
			// dialect-specific control; these assertions pin that every filtered
			// statement consumes the same predicate builder.
			generatedPredicate := atlasExactIdentityRowPredicateFor("")
			for _, sql := range []string{
				m.getAppliedMigrationsSQL(),
				m.getAppliedRevisionsSQL(),
				m.getRevisionsSQL(),
				m.getDirtyRevisionSQL(),
				m.countRevisionsSQL(),
			} {
				c.Assert(sql, qt.Contains, generatedPredicate)
			}
			c.Assert(m.atlasVersionNumberExpression(), qt.Contains, "WHEN '.foo' THEN 10")
		})
	}
}

func TestAtlasFilteredRevisionSQL_IncludesSquashedHistoricalDotIdentity(t *testing.T) {
	c := qt.New(t)
	m, err := NewFSMigrator(nil, fstest.MapFS{
		"20_baseline.sql": {Data: []byte("SELECT 2;")},
	}, WithMigrationDirFormat(MigrationDirFormatAtlas), WithAtlasRevisionVersions(map[int64]string{
		10: ".foo",
		20: "2",
	}))
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(RevisionTableFormatAtlas)

	c.Assert(m.migrationProvider.Migrations(), qt.HasLen, 1)
	c.Assert(m.migrationProvider.Migrations()[0].RevisionVersion(), qt.Equals, "2")
	c.Assert(m.atlasRevisionRowPredicate(), qt.Equals,
		"version <> '.atlas_cloud_identifier'")
	c.Assert(m.atlasVersionNumberExpression(), qt.Contains, "WHEN '.foo' THEN 10")
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
		// wantContains is what else the branch has to spell, and wantAbsent is
		// what it must never spell. Both are lists rather than one string
		// because a branch can owe several tokens, and a row that owes none
		// simply names none.
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:              "postgres gets a native JSONB column",
			dialect:           platform.Postgres,
			wantPartialHashes: "partial_hashes JSONB NULL",
			wantContains:      []string{"executed_at TIMESTAMPTZ NOT NULL"},
		},
		{
			name:              "cockroachdb follows the postgres family",
			dialect:           platform.CockroachDB,
			wantPartialHashes: "partial_hashes JSONB NULL",
			wantContains:      []string{"executed_at TIMESTAMPTZ NOT NULL"},
		},
		{
			name:              "yugabytedb follows the postgres family",
			dialect:           platform.YugabyteDB,
			wantPartialHashes: "partial_hashes JSONB NULL",
			wantContains:      []string{"executed_at TIMESTAMPTZ NOT NULL"},
		},
		{
			name:              "mysql keeps the default JSON column and binary revision identity",
			dialect:           platform.MySQL,
			wantPartialHashes: "partial_hashes JSON NULL",
			wantContains: []string{
				"CREATE TABLE IF NOT EXISTS " + atlasRevisionsGuardTable,
				"version VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY",
			},
		},
		{
			name:              "mariadb keeps the default JSON column and binary revision identity",
			dialect:           platform.MariaDB,
			wantPartialHashes: "partial_hashes JSON NULL",
			wantContains: []string{
				"CREATE TABLE IF NOT EXISTS " + atlasRevisionsGuardTable,
				"version VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY",
			},
		},
		{
			name:              "sqlite keeps the default JSON column",
			dialect:           platform.SQLite,
			wantPartialHashes: "partial_hashes JSON NULL",
			wantContains:      []string{"CREATE TABLE IF NOT EXISTS " + atlasRevisionsGuardTable},
		},
		{
			name:              "spanner keeps the default JSON column",
			dialect:           platform.Spanner,
			wantPartialHashes: "partial_hashes JSON NULL",
			wantContains:      []string{"CREATE TABLE IF NOT EXISTS " + atlasRevisionsGuardTable},
		},
		{
			name:              "sqlserver stores the JSON document as text",
			dialect:           platform.SQLServer,
			wantPartialHashes: "partial_hashes NVARCHAR(MAX) NULL",
			wantContains: []string{
				"IF OBJECT_ID(" + atlasRevisionsGuardSQLServerObject + ", N'U') IS NULL",
				"version NVARCHAR(255) COLLATE Latin1_General_100_BIN2 PRIMARY KEY",
			},
		},
		{
			// ClickHouse reads a trailing NULL as Nullable(T), so any JSON token
			// in this statement is asked for as Nullable(JSON): rejected outright
			// by ClickHouse 24.x (code 43) and, on later servers, accepted but
			// silently storing `{}` in place of the JSON null Ptah wrote.
			// TestAtlasRevisionsTableDDL_ClickHousePinsTheWholeStatement pins the
			// rest of the statement for the same reason.
			name:              "clickhouse stores the JSON document as text",
			dialect:           platform.ClickHouse,
			wantPartialHashes: "partial_hashes TEXT NULL",
			wantAbsent:        []string{"JSON"},
		},
		{
			name:              "unset dialect falls back to the default branch",
			dialect:           "",
			wantPartialHashes: "partial_hashes JSON NULL",
			wantContains:      []string{"CREATE TABLE IF NOT EXISTS " + atlasRevisionsGuardTable},
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
			for _, want := range tt.wantContains {
				c.Assert(ddl, qt.Contains, want,
					qt.Commentf("dialect %q must spell %q", tt.dialect, want))
			}
			for _, unwanted := range tt.wantAbsent {
				c.Assert(ddl, qt.Not(qt.Contains), unwanted,
					qt.Commentf("dialect %q must never spell %q", tt.dialect, unwanted))
			}
		})
	}
}

// TestAtlasRevisionsTableDDL_ClickHousePinsTheWholeStatement is the one branch
// where a column at a time is not enough. Every trailing NULL on ClickHouse is
// a Nullable(T) declaration, so widening any column of this statement can turn
// it into a type the server refuses or silently substitutes; pinning the whole
// text makes such a change deliberate rather than incidental.
func TestAtlasRevisionsTableDDL_ClickHousePinsTheWholeStatement(t *testing.T) {
	c := qt.New(t)

	ddl := atlasRevisionsTableDDL(
		platform.ClickHouse,
		atlasRevisionsGuardTable,
		atlasRevisionsGuardSQLServerObject,
	)

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
