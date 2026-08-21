package atlasmigrateimport

// White-box testing required: the reserved slot is an unexported constant, and
// the point of this test is that the migrator's restatement of it cannot drift
// from the value the importer actually assigns.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

// The migrator recognizes a converted repeatable by its version, so it can say
// what happened to the file instead of reporting it as tampering. It restates
// the slot rather than importing it, because the importer sits above that
// package (stokaro/ptah#1702).
func TestConvertedFlywayRepeatableVersionMatchesTheImporter(t *testing.T) {
	c := qt.New(t)

	c.Assert(int64(flywayRepeatableVersion), qt.Equals, migrator.ConvertedFlywayRepeatableVersion)
}

// TestFlywayRepeatableVersionSitsAboveEveryVersionedBand is why that slot was
// chosen, held here so a change to the banding cannot quietly move it into the
// range a versioned migration can reach.
func TestFlywayRepeatableVersionSitsAboveEveryVersionedBand(t *testing.T) {
	c := qt.New(t)

	c.Assert(flywayRepeatableVersion > flywayVersionedBand, qt.IsTrue)
	c.Assert(flywayRepeatableVersion > flywayBandSize, qt.IsTrue)
}
