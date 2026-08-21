package matviewrefresh_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/matviewrefresh"
)

// TestRefuse_CarriesTheSentinelAndTheReason pins the two things every caller
// depends on: a sentinel it can branch on, and one wording.
//
// The sentinel is deliberately not ErrUnknownAttribute. A caller branching on
// that treats the attribute as a typo, and a retired attribute is the opposite:
// it was spelled correctly and meant something once (stokaro/ptah#1625).
func TestRefuse_CarriesTheSentinelAndTheReason(t *testing.T) {
	c := qt.New(t)

	err := matviewrefresh.Refuse("analytics.user_counts")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrRetiredAttribute)
	c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrUnknownAttribute)
	c.Assert(err.Error(), qt.Contains, `materialized view "analytics.user_counts"`)
	c.Assert(err.Error(), qt.Contains, matviewrefresh.Attribute)
	c.Assert(err.Error(), qt.Contains, matviewrefresh.Reason)
}

// TestReason_NamesNoDialect is the property that separates this refusal from
// the capability error it replaces.
//
// The old message read `postgres cannot represent materialized view "x"
// refresh strategy "concurrently"`, which said the target was the limitation.
// It was not: no target refreshes as part of reconciliation, because a
// materialized view is populated on CREATE and goes stale from source data a
// schema comparison cannot see. A dialect name in this message would send an
// operator looking for a target that behaves differently.
func TestReason_NamesNoDialect(t *testing.T) {
	dialects := []string{
		"postgres", "cockroachdb", "yugabytedb", "clickhouse",
		"mysql", "mariadb", "sqlite", "sqlserver", "spanner",
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(matviewrefresh.Reason, qt.Not(qt.Contains), dialect)
			c.Assert(matviewrefresh.Refuse("x").Error(), qt.Not(qt.Contains), dialect)
		})
	}
}
