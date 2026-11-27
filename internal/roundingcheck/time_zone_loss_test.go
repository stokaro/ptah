package roundingcheck_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/roundingcheck"
	"ptah.run/internal/roundingscan"
	"ptah.run/migration/migrator"
)

func TestChecked_TimeZoneLossKeepsItsKindAndWarning(t *testing.T) {
	c := qt.New(t)
	change := roundingscan.Change{Table: "events", Column: "at", HeldType: "timestamptz", DeclaredType: "timestamp", Temporal: true}
	block, err := roundingcheck.Checked("postgres", []roundingscan.Change{change})
	c.Assert(err, qt.IsNil)
	c.Assert(block, qt.Contains, "which removes a time zone or offset; the check refuses the migration while the column holds a non-NULL value")
	checks, err := migrator.ParseChecks(block, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.HasLen, 1)
	c.Assert(checks[0].Kind, qt.Equals, migrator.CheckKindTimeZoneLoss)
	c.Assert(checks[0].Assert, qt.Equals, `SELECT CASE WHEN EXISTS (SELECT 1 FROM "events" WHERE "at" IS NOT NULL) THEN 0 ELSE 1 END`)
	c.Assert(migrator.ValueRounding{}.Waives(checks[0]), qt.IsFalse)
	c.Assert(migrator.ValueRounding{Allowed: true}.Waives(checks[0]), qt.IsTrue)
	c.Assert(roundingcheck.Allowed([]roundingscan.Change{change}), qt.Contains, "the information loss was allowed when this file was generated, so no check reads the rows")
}
