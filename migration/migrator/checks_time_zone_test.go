package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

func TestParseChecks_TimeZoneLossCannotRunAfterConversion(t *testing.T) {
	c := qt.New(t)
	checks, err := migrator.ParseChecks(`-- +ptah check kind=time-zone-loss phase=after name="events.at" assert="SELECT 1"`, "postgres")
	c.Assert(err, qt.ErrorMatches, `\+ptah check kind=time-zone-loss reads the rows before the body and takes no phase=after`)
	c.Assert(checks, qt.HasLen, 0)
}
