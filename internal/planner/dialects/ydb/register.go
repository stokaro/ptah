package ydb

import (
	"sync"

	"ptah.run/core/platform"
	"ptah.run/migration/planner"
)

// registration registers the planner once per process; see [RegisterPlanner].
var registration = sync.OnceValue(func() error {
	return planner.Register(platform.YDB, func(opts planner.Options) planner.Planner {
		return NewWithCapabilities(opts.CapabilitiesFor(platform.YDB)).
			WithTableRebuild(opts.AllowTableRebuild).
			WithTableRebuildRequest(opts.TableRebuildRequest)
	})
})

// RegisterPlanner registers the YDB planner with migration/planner's
// process-global registry, once per process; every later call returns the
// first call's result. The built-in runtime calls it when it assembles the YDB
// owner, so migration/planner, which links no owner, plans ydb only once a
// built-in runtime exists in the process.
//
// The order dependence is a known cost of the registry seam. A planner that
// is not Go code in this process cannot register into a global registry, so
// target planners are to come from the runtime instead (stokaro/ptah#4204).
func RegisterPlanner() error {
	return registration()
}
