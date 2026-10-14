package builtin

import (
	"fmt"
	"sync"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/internal/planner/dialects/mssql"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planner/dialects/oracle"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/internal/planner/dialects/sqlite"
	ydbplanner "ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/planner"
)

// registerPlanners registers the migration planner of every bundled dialect
// in migration/planner's process-global registry, once per process; every
// later call returns the first call's result. migration/planner links no
// dialect's planner, so it plans a bundled dialect only once a built-in
// runtime exists in the process.
var registerPlanners = sync.OnceValue(func() error {
	for _, dialect := range builtindialects.Descriptors() {
		if dialect.Name == platform.YDB {
			continue
		}
		factory, found := plannerFactory(dialect)
		if !found {
			return fmt.Errorf("no migration planner for the bundled dialect %q", dialect.Name)
		}
		if err := planner.Register(dialect.Name, factory); err != nil {
			return err
		}
	}
	// The YDB owner registers its own planner, once per process.
	return ydbplanner.RegisterPlanner()
})

// plannerFactory is the planner of a bundled dialect. A dialect of the
// PostgreSQL or the MySQL family is planned by its family's planner, built
// for the dialect so its capability preset decides what it emits.
func plannerFactory(dialect descriptor.Dialect) (planner.Factory, bool) {
	name := dialect.Name
	switch {
	case dialect.Family == platform.Postgres:
		return func(opts planner.Options) planner.Planner {
			plan := postgres.NewForDialect(name, opts.CapabilitiesFor(name)).
				WithConcurrentIndexRefs(opts.ConcurrentIndexRefs...).
				WithConcurrentIndexDropRefs(opts.ConcurrentIndexDropRefs...).
				WithSkipChangeKinds(opts.SkipChangeKinds...)
			if opts.ConcurrentIndexes {
				plan = plan.WithConcurrentIndexes()
			}
			if opts.ConcurrentIndexDrops {
				plan = plan.WithConcurrentIndexDrops()
			}
			return plan
		}, true
	case dialect.Family == platform.MySQL:
		return func(opts planner.Options) planner.Planner {
			return mysql.NewForDialect(name, opts.CapabilitiesFor(name))
		}, true
	case name == platform.SQLServer:
		return func(opts planner.Options) planner.Planner {
			return mssql.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	case name == platform.Oracle:
		return func(opts planner.Options) planner.Planner {
			return oracle.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	case name == platform.ClickHouse:
		return func(opts planner.Options) planner.Planner {
			return clickhouse.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	case name == platform.SQLite:
		return func(opts planner.Options) planner.Planner {
			return sqlite.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	default:
		return nil, false
	}
}
