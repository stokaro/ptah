package matviewrefresh_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/matviewrefresh"
)

func TestValidateAcceptsTheManualStrategy(t *testing.T) {
	for _, strategy := range []string{"", "manual", "  MANUAL  "} {
		t.Run(strategy, func(t *testing.T) {
			c := qt.New(t)

			err := matviewrefresh.Validate("postgresql", "user_counts", strategy)

			c.Assert(err, qt.IsNil)
			c.Assert(matviewrefresh.Canonical(strategy), qt.Equals, "manual")
		})
	}
}

func TestValidateNamesAnUnsupportedStrategy(t *testing.T) {
	c := qt.New(t)

	err := matviewrefresh.Validate("postgresql", "analytics.user_counts", "  EVERY 5 MINUTES  ")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `postgres cannot represent materialized view "analytics.user_counts" refresh strategy "  EVERY 5 MINUTES  "; "manual" is the only strategy, and a schedule is not something Ptah runs: .*`)
	var capabilityErr *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &capabilityErr)
	c.Assert(capabilityErr.Dialect, qt.Equals, "postgres")
	c.Assert(capabilityErr.Feature, qt.Equals, "materialized view refresh strategy")
}

func TestValidateDeclaredRefusesTheFirstUnsupportedView(t *testing.T) {
	c := qt.New(t)

	err := matviewrefresh.ValidateDeclared("clickhouse", []goschema.MaterializedView{
		{Name: "manual_view", RefreshStrategy: "manual"},
		{Name: "scheduled_view", RefreshStrategy: "every 5 minutes"},
		{Name: "concurrent_view", RefreshStrategy: "concurrently"},
	})

	c.Assert(err, qt.ErrorMatches, `clickhouse cannot represent materialized view "scheduled_view" refresh strategy "every 5 minutes"; "manual" is the only strategy, and a schedule is not something Ptah runs: .*`)
}

// TestValidateGivesEachRefusedStrategyItsOwnReason is why the sentence changed.
// "Only manual is currently supported" reads as a feature nobody has written
// yet; each of these was decided on a measurement, and a reader who is told
// which one can stop looking for a flag that turns it on (stokaro/ptah#1625).
func TestValidateGivesEachRefusedStrategyItsOwnReason(t *testing.T) {
	c := qt.New(t)

	concurrently := matviewrefresh.Validate("postgres", "mv", "concurrently")
	scheduled := matviewrefresh.Validate("postgres", "mv", "every 5 minutes")
	nonsense := matviewrefresh.Validate("postgres", "mv", "sometimes")

	c.Assert(concurrently, qt.ErrorMatches, `.*data operation with no point in a schema apply to attach to.*`)
	c.Assert(scheduled, qt.ErrorMatches, `.*a schedule is not something Ptah runs.*`)
	c.Assert(nonsense, qt.ErrorMatches, `.*no other value names anything the supported engines do`)
	c.Assert(concurrently.Error(), qt.Not(qt.Equals), scheduled.Error())
	c.Assert(scheduled.Error(), qt.Not(qt.Equals), nonsense.Error())
}

// TestValidateStillAcceptsOnlyManual is the control: giving the refusals better
// words must not turn any of them into an acceptance.
func TestValidateStillAcceptsOnlyManual(t *testing.T) {
	c := qt.New(t)

	for _, strategy := range []string{"concurrently", "every 5 minutes", "sometimes"} {
		c.Assert(matviewrefresh.Validate("postgres", "mv", strategy), qt.IsNotNil,
			qt.Commentf("strategy %q", strategy))
	}
	c.Assert(matviewrefresh.Validate("postgres", "mv", "manual"), qt.IsNil)
}
