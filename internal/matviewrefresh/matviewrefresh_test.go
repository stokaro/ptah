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
	c.Assert(err, qt.ErrorMatches, `postgres cannot represent materialized view "analytics.user_counts" refresh strategy "  EVERY 5 MINUTES  "; only "manual" is currently supported`)
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

	c.Assert(err, qt.ErrorMatches, `clickhouse cannot represent materialized view "scheduled_view" refresh strategy "every 5 minutes"; only "manual" is currently supported`)
}
