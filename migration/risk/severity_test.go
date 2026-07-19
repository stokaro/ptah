package risk_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/risk"
)

func TestSeverityRankAndMachineLevels(t *testing.T) {
	c := qt.New(t)

	c.Assert(risk.Rank(risk.Safe) < risk.Rank(risk.Warning), qt.IsTrue)
	c.Assert(risk.Rank(risk.Warning) < risk.Rank(risk.Error), qt.IsTrue)
	c.Assert(risk.Rank(risk.Error), qt.Equals, risk.Rank(risk.Destructive))

	c.Assert(risk.IsBlocking(risk.Warning), qt.IsFalse)
	c.Assert(risk.IsBlocking(risk.Error), qt.IsTrue)
	c.Assert(risk.IsBlocking(risk.Destructive), qt.IsTrue)
	c.Assert(risk.SARIFLevel(risk.Warning), qt.Equals, "warning")
	c.Assert(risk.SARIFLevel(risk.Destructive), qt.Equals, "error")
}
