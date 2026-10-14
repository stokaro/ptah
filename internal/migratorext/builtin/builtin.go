// Package builtin lists the migrator services of the engines Ptah ships and
// looks one up by dialect. It names no engine itself: each service states
// the dialects it serves.
package builtin

import (
	"ptah.run/core/platform"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/clickhouse"
	"ptah.run/internal/migratorext/mysql"
	"ptah.run/internal/migratorext/oracle"
	"ptah.run/internal/migratorext/postgres"
	"ptah.run/internal/migratorext/spanner"
	"ptah.run/internal/migratorext/sqlite"
	"ptah.run/internal/migratorext/sqlserver"
	"ptah.run/internal/migratorext/ydb"
)

// services maps each dialect a built-in service serves to it. It is built
// once from the list below and never written again.
var services = index(
	postgres.Services(),
	mysql.MySQL(),
	mysql.MariaDB(),
	clickhouse.Services(),
	sqlite.Services(),
	sqlserver.Services(),
	spanner.Services(),
	oracle.Services(),
	ydb.Services(),
)

func index(all ...migratorext.Services) map[string]migratorext.Services {
	byDialect := make(map[string]migratorext.Services)
	for _, served := range all {
		for _, dialect := range served.Dialects {
			if _, taken := byDialect[dialect]; taken {
				panic("migratorext/builtin: two services serve dialect " + dialect)
			}
			byDialect[dialect] = served
		}
	}
	return byDialect
}

// Lookup returns the services of the dialect a spelling selects, folded as
// platform.NormalizeDialect folds it, and the zero Services, which takes
// every generic behavior, for a dialect no built-in service serves.
func Lookup(dialect string) migratorext.Services {
	return services[platform.NormalizeDialect(dialect)]
}

// Dialects returns every dialect a built-in service serves.
func Dialects() []string {
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	return names
}
