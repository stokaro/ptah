package atlas

// White-box testing required: legacyFlywayRefusal renders the recovery SQL for
// an operator using --revisions-schema, and that branch is unreachable through
// the command on sqlite, the only driver the unit suite has - the apply fails
// opening the revision table before the detector runs. No exported API exposes
// the rendered message.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/revisiontable"
)

// TestLegacyFlywayRefusalRendersRecoverySQL is white-box because the branch it
// covers cannot be reached through the command: --revisions-schema names a
// database schema, and on sqlite — the only driver the unit suite has — the
// apply fails while opening the revision table, long before the detector runs.
// The end-to-end path is covered by the tests in the _test package; this pins
// the one thing they cannot see, which is that an operator using
// --revisions-schema is pointed at the table that actually holds their rows
// rather than at the default one.
func TestLegacyFlywayRefusalRendersRecoverySQL(t *testing.T) {
	stale := []legacyFlywayRevision{
		{source: "V1__init.sql", recorded: "10000", target: "1"},
		{source: "V2__seed.sql", recorded: "20000", target: "2"},
	}

	t.Run("default schema", func(t *testing.T) {
		c := qt.New(t)
		got := legacyFlywayRefusal(stale, "", "sqlite")

		c.Assert(got, qt.Contains, "2 obsolete revision row(s)")
		c.Assert(got, qt.Contains, "V1__init.sql")
		c.Assert(got, qt.Contains,
			"UPDATE atlas_schema_revisions SET version = '1' WHERE version = '10000';")
		c.Assert(got, qt.Contains,
			"UPDATE atlas_schema_revisions SET version = '2' WHERE version = '20000';")
	})

	t.Run("explicit revisions schema", func(t *testing.T) {
		c := qt.New(t)
		got := legacyFlywayRefusal(stale, `"reporting"."atlas_schema_revisions"`, "postgres")

		c.Assert(got, qt.Contains,
			`UPDATE "reporting"."atlas_schema_revisions" SET version = '1' WHERE version = '10000';`)
		c.Assert(got, qt.Not(qt.Contains), "UPDATE atlas_schema_revisions")
	})

	t.Run("dialect-quoted table identifiers are used verbatim", func(t *testing.T) {
		c := qt.New(t)
		for _, test := range []struct {
			identifier string
			dialect    string
		}{
			{identifier: `"reporting"."atlas_schema_revisions"`, dialect: "postgres"},
			{identifier: "`reporting`.`atlas_schema_revisions`", dialect: "mysql"},
			{identifier: "[reporting].[atlas_schema_revisions]", dialect: "sqlserver"},
		} {
			got := legacyFlywayRefusal(stale, test.identifier, test.dialect)

			c.Assert(got, qt.Contains, "UPDATE "+test.identifier+" SET version =")
		}
	})

	t.Run("exact tokens are escaped and duplicate obsolete rows are deleted", func(t *testing.T) {
		c := qt.New(t)
		got := legacyFlywayRefusal([]legacyFlywayRevision{
			{source: "Vx__quote.sql", recorded: "461", target: "x'y"},
			{source: "R__repeat.sql", recorded: "922", target: "", delete: true},
		}, "", "sqlite")

		c.Assert(got, qt.Contains,
			"UPDATE atlas_schema_revisions SET version = 'x''y' WHERE version = '461';")
		c.Assert(got, qt.Contains,
			"DELETE FROM atlas_schema_revisions WHERE version = '922';")
	})

	t.Run("mysql family uses mode-independent hexadecimal literals", func(t *testing.T) {
		c := qt.New(t)
		for _, dialect := range []string{"mysql", "mariadb"} {
			got := legacyFlywayRefusal([]legacyFlywayRevision{
				{source: "Vx__escaped.sql", recorded: `old\key`, target: `x\'y`},
			}, "", dialect)

			c.Assert(got, qt.Contains,
				`UPDATE atlas_schema_revisions SET version = X'785c2779' WHERE version = X'6f6c645c6b6579';`)
		}
	})

	t.Run("postgres family uses mode-independent dollar quotes", func(t *testing.T) {
		c := qt.New(t)
		for _, dialect := range []string{"postgres", "cockroachdb", "yugabytedb", "spanner"} {
			got := legacyFlywayRefusal([]legacyFlywayRevision{
				{source: "Vx__escaped.sql", recorded: `old\key`, target: `x\'y`},
			}, "", dialect)

			c.Assert(got, qt.Contains,
				`UPDATE atlas_schema_revisions SET version = $ptah$x\'y$ptah$ WHERE version = $ptah$old\key$ptah$;`)
		}
	})

	t.Run("clickhouse escapes backslashes", func(t *testing.T) {
		c := qt.New(t)
		got := legacyFlywayRefusal([]legacyFlywayRevision{
			{source: "Vx__escaped.sql", recorded: `old\key`, target: `x\'y`},
		}, "", "clickhouse")

		c.Assert(got, qt.Contains,
			`UPDATE atlas_schema_revisions SET version = 'x\\''y' WHERE version = 'old\\key';`)
	})

	t.Run("mysql empty identity stays explicitly present", func(t *testing.T) {
		c := qt.New(t)
		got := legacyFlywayRefusal([]legacyFlywayRevision{
			{source: "V.sql", recorded: "10000", target: ""},
		}, "", "mysql")

		c.Assert(got, qt.Contains,
			`UPDATE atlas_schema_revisions SET version = X'' WHERE version = X'3130303030';`)
	})

	t.Run("sqlserver preserves unicode identity", func(t *testing.T) {
		c := qt.New(t)
		got := legacyFlywayRefusal([]legacyFlywayRevision{
			{source: "V猫__unicode.sql", recorded: "10000", target: "猫"},
		}, "", "sqlserver")

		c.Assert(got, qt.Contains,
			`UPDATE atlas_schema_revisions SET version = N'猫' WHERE version = N'10000';`)
	})
}

func TestAnalyzeLegacyFlywayRevisionsRefusesAnotherMigrationExactToken(t *testing.T) {
	c := qt.New(t)
	covered := []atlasmigrateimport.FlywayCoveredSourceVersion{
		{Source: "V1__first.sql", Token: "1", Version: 10000},
		{Source: "V10000__collision.sql", Token: "10000", Version: 20000},
	}
	legacy := []atlasmigrateimport.LegacyFlywayVersion{
		{Source: "V1__first.sql", Legacy: 10000, Current: 10000},
	}

	got := analyzeLegacyFlywayRevisions(covered, legacy, []recordedFlywayRevision{{identity: "10000", operatorVersion: "Ptah"}})

	c.Assert(got.stale, qt.HasLen, 0)
	c.Assert(got.ambiguous, qt.HasLen, 1)
	c.Assert(got.ambiguous[0].source, qt.Equals, "V1__first.sql")
	c.Assert(got.ambiguous[0].target, qt.Equals, "1")
	c.Assert(got.ambiguous[0].recorded, qt.Equals, "10000")
	c.Assert(got.ambiguous[0].collisionSource, qt.Equals, "V10000__collision.sql")
	c.Assert(got.ambiguous[0].collisionTarget, qt.Equals, "10000")
	c.Assert(got.ambiguous[0].collisionIsExactToken, qt.IsTrue)
	c.Assert(ambiguousLegacyFlywayRefusal(got.ambiguous), qt.Contains,
		"automatic recovery cannot determine which migration owns the recorded row")
}

func TestAnalyzeLegacyFlywayRevisionsKeepsAnotherExactTokenWhenTargetExists(t *testing.T) {
	c := qt.New(t)
	covered := []atlasmigrateimport.FlywayCoveredSourceVersion{
		{Source: "V1__first.sql", Token: "1", Version: 10000},
		{Source: "V10000__collision.sql", Token: "10000", Version: 20000},
	}
	legacy := []atlasmigrateimport.LegacyFlywayVersion{
		{Source: "V1__first.sql", Legacy: 10000, Current: 10000},
	}

	got := analyzeLegacyFlywayRevisions(covered, legacy, []recordedFlywayRevision{
		{identity: "1", operatorVersion: "Ptah"},
		{identity: "10000", operatorVersion: "Ptah"},
	})

	c.Assert(got.stale, qt.HasLen, 0)
	c.Assert(got.ambiguous, qt.HasLen, 0)
}

func TestAnalyzeLegacyFlywayRevisionsRefusesCollidingRetiredKeys(t *testing.T) {
	c := qt.New(t)
	covered := []atlasmigrateimport.FlywayCoveredSourceVersion{
		{Source: "B2499__base.sql", Token: "2499", Version: 102010000},
		{Source: "5dir/V10201__old.sql", Token: "10201", Version: 200000000},
	}
	legacy := []atlasmigrateimport.LegacyFlywayVersion{
		{Source: "5dir/V10201__old.sql", Legacy: 102010000, Current: 200000000},
	}

	got := analyzeLegacyFlywayRevisions(covered, legacy, []recordedFlywayRevision{{identity: "102010000", operatorVersion: "Ptah"}})

	c.Assert(got.stale, qt.HasLen, 0)
	c.Assert(got.ambiguous, qt.HasLen, 1)
	c.Assert(got.ambiguous[0].recorded, qt.Equals, "102010000")
	c.Assert(got.ambiguous[0].collisionTarget, qt.Equals, "10201")
	c.Assert(got.ambiguous[0].collisionIsExactToken, qt.IsFalse)
	c.Assert(ambiguousLegacyFlywayRefusal(got.ambiguous), qt.Contains,
		"older Ptah ordering-key candidate for both")
	c.Assert(ambiguousLegacyFlywayRefusal(got.ambiguous), qt.Contains,
		"automatic recovery cannot determine which migration owns the recorded row")
}

func TestAnalyzeLegacyFlywayRevisionsRefusesRetiredExactNumericIdentity(t *testing.T) {
	c := qt.New(t)
	covered := []atlasmigrateimport.FlywayCoveredSourceVersion{
		{Source: "V1__first.sql", Token: "1", Version: 10000},
	}
	legacy := []atlasmigrateimport.LegacyFlywayVersion{
		{Source: "V1__first.sql", Legacy: 10000, Current: 10000},
	}

	got := analyzeLegacyFlywayRevisions(covered, legacy, []recordedFlywayRevision{{
		identity:        "10000",
		operatorVersion: revisiontable.SourceIdentityOperatorVersion,
	}})

	c.Assert(got.stale, qt.HasLen, 0)
	c.Assert(got.ambiguous, qt.HasLen, 1)
	c.Assert(got.ambiguous[0].recorded, qt.Equals, "10000")
	c.Assert(got.ambiguous[0].recordedHasNonLegacyProvenance, qt.IsTrue)
	c.Assert(ambiguousLegacyFlywayRefusal(got.ambiguous), qt.Contains,
		"is an exact source identity or belongs to another writer")
}
