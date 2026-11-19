package mysqlpartition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/mysqlpartition"
)

// The refusal names the partitioning, says Ptah does not model it, and wraps
// the unsupported-feature sentinel.
func TestRefusal(t *testing.T) {
	c := qt.New(t)

	err := mysqlpartition.Refusal("PARTITION BY on table sales")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: PARTITION BY on table sales: Ptah does not model MySQL or MariaDB partitioning `+
		`\(stokaro/ptah#6357\); leave it out of the desired schema and manage the partitions outside Ptah`)
}
