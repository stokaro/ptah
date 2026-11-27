package roundingscan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/roundingscan"
)

// No cast can prove that the source zone survives in a zone-free target.
// Reading non-NULL values also covers whole seconds and UTC offsets.
func TestAssertion_TimeZoneLossReadsEveryNonNullValue(t *testing.T) {
	tests := []struct {
		dialect, held, declared, want string
	}{
		{"postgres", "timetz(6)", "time(6)", `SELECT CASE WHEN EXISTS (SELECT 1 FROM "events" WHERE "at" IS NOT NULL) THEN 0 ELSE 1 END`},
		{"postgres", "timestamptz", "timestamp", `SELECT CASE WHEN EXISTS (SELECT 1 FROM "events" WHERE "at" IS NOT NULL) THEN 0 ELSE 1 END`},
		{"cockroachdb", "TIMETZ", "TIME", `SELECT CASE WHEN EXISTS (SELECT 1 FROM "events" WHERE "at" IS NOT NULL) THEN 0 ELSE 1 END`},
		{"yugabytedb", "timestamptz", "timestamp", `SELECT CASE WHEN EXISTS (SELECT 1 FROM "events" WHERE "at" IS NOT NULL) THEN 0 ELSE 1 END`},
		{"sqlserver", "DATETIMEOFFSET(7)", "DATETIME2(7)", `SELECT CASE WHEN EXISTS (SELECT 1 FROM [events] WHERE [at] IS NOT NULL) THEN 0 ELSE 1 END`},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.held, func(t *testing.T) {
			c := qt.New(t)
			change := roundingscan.Change{Table: "events", Column: "at", HeldType: test.held, DeclaredType: test.declared, Temporal: true}
			assertion, err := roundingscan.Assertion(test.dialect, change)
			c.Assert(err, qt.IsNil)
			c.Assert(assertion, qt.Equals, test.want)
		})
	}
}
