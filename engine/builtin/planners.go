package builtin

import (
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/internal/planner/dialects/mssql"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planner/dialects/oracle"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/internal/planner/dialects/sqlite"
	ydbplanner "ptah.run/internal/planner/dialects/ydb"
)

// plannerFactory is the planner of a bundled dialect. A dialect of the
// PostgreSQL or the MySQL family is planned by its family's planner, built
// for the dialect so its capability preset decides what it emits.
func plannerFactory(dialect descriptor.Dialect) (planext.Factory, bool) {
	name := dialect.Name
	switch {
	case name == platform.YDB:
		return func(opts planext.Options) planext.Planner {
			return ydbplanner.NewWithCapabilities(opts.CapabilitiesFor(name)).WithTableRebuild(opts.AllowTableRebuild).WithTableRebuildRequest(opts.TableRebuildRequest)
		}, true
	case dialect.Family == platform.Postgres:
		return func(opts planext.Options) planext.Planner {
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
		return func(opts planext.Options) planext.Planner {
			return mysql.NewForDialect(name, opts.CapabilitiesFor(name))
		}, true
	case name == platform.SQLServer:
		return func(opts planext.Options) planext.Planner {
			return mssql.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	case name == platform.Oracle:
		return func(opts planext.Options) planext.Planner {
			return oracle.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	case name == platform.ClickHouse:
		return func(opts planext.Options) planext.Planner {
			return clickhouse.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	case name == platform.SQLite:
		return func(opts planext.Options) planext.Planner {
			return sqlite.NewWithCapabilities(opts.CapabilitiesFor(name))
		}, true
	default:
		return nil, false
	}
}
