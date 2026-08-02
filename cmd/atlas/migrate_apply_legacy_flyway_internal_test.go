package atlas

// White-box testing required: legacyFlywayRefusal renders the recovery SQL for
// an operator using --revisions-schema, and that branch is unreachable through
// the command on sqlite, the only driver the unit suite has - the apply fails
// opening the revision table before the detector runs. No exported API exposes
// the rendered message.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasmigrateimport"
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
	c := qt.New(t)
	stale := []atlasmigrateimport.LegacyFlywayVersion{
		{Source: "V1__init.sql", Legacy: 10000, Current: 4611686018427469511},
		{Source: "V2__seed.sql", Legacy: 20000, Current: 4611686018427510315},
	}

	c.Run("default schema", func(c *qt.C) {
		got := legacyFlywayRefusal(stale, "")

		c.Assert(got, qt.Contains, "stokaro/ptah#982")
		c.Assert(got, qt.Contains, "2 already-applied migration(s)")
		c.Assert(got, qt.Contains, "V1__init.sql")
		c.Assert(got, qt.Contains,
			"UPDATE atlas_schema_revisions SET version = '4611686018427469511' WHERE version = '10000';")
		c.Assert(got, qt.Contains,
			"UPDATE atlas_schema_revisions SET version = '4611686018427510315' WHERE version = '20000';")
	})

	c.Run("explicit revisions schema", func(c *qt.C) {
		got := legacyFlywayRefusal(stale, "reporting")

		c.Assert(got, qt.Contains,
			"UPDATE reporting.atlas_schema_revisions SET version = '4611686018427469511' WHERE version = '10000';")
		c.Assert(got, qt.Not(qt.Contains), "UPDATE atlas_schema_revisions")
	})

	c.Run("a blank revisions schema is not a schema", func(c *qt.C) {
		got := legacyFlywayRefusal(stale, "   ")

		c.Assert(got, qt.Contains, "UPDATE atlas_schema_revisions SET version =")
	})
}
