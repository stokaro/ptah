// Package rollbacklimit turns "roll back the last N applied migrations" into
// the version a rollback stops at. Native `ptah migrations down --limit` and
// the amount `ptah-compat migrate down` takes as its argument both resolve
// through [Target], so the two surfaces stop at the same version for the same
// history.
package rollbacklimit

import "slices"

// Target returns the version a rollback of the last limit applied migrations
// stops at: the highest applied version below the limit highest ones, or 0
// when the history holds limit versions or fewer, so a limit larger than the
// history rolls back what is applied and nothing more. applied may be in any
// order and is not modified. A limit of 0 selects no migration and returns the
// highest applied version, so the rollback reverts nothing; callers that read 0
// as "no limit" do not call Target for it. Target of an empty history is 0.
func Target(applied []int64, limit uint64) int64 {
	sorted := slices.Sorted(slices.Values(applied))
	kept := uint64(len(sorted)) - min(limit, uint64(len(sorted)))
	if kept == 0 {
		return 0
	}
	return sorted[kept-1]
}
