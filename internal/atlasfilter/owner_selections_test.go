package atlasfilter_test

import (
	"ptah.run/core/selectext"
	"ptah.run/internal/builtintest"
)

// ownerSelections are the scope selections of the bundled owners, which a
// command passes from its runtime.
func ownerSelections() selectext.Set {
	return builtintest.Runtime().Selections()
}
