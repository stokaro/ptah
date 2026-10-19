// Package builtin lists the database URL services of the engines Ptah ships
// and looks one up by dialect. It names no engine itself: each service states
// the dialects it serves.
package builtin

import (
	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/clickhouse"
	"ptah.run/internal/dburlext/postgres"
	"ptah.run/internal/dburlext/sqlite"
	"ptah.run/internal/dburlext/sqlserver"
	"ptah.run/internal/dburlext/ydb"
)

// services maps each dialect a built-in service serves to it. It is built
// once from the list below and never written again.
var services = index(
	postgres.Services(),
	sqlserver.Services(),
	clickhouse.Services(),
	sqlite.Services(),
	ydb.Services(),
)

func index(all ...dburlext.Services) map[string]dburlext.Services {
	byDialect := make(map[string]dburlext.Services)
	for _, served := range all {
		for _, dialect := range served.Dialects {
			if _, taken := byDialect[dialect]; taken {
				panic("dburlext/builtin: two services serve dialect " + dialect)
			}
			byDialect[dialect] = served
		}
	}
	return byDialect
}

// Lookup returns the services of the dialect a spelling selects, folded as
// platform.NormalizeDialect folds it, and the zero Services, which takes
// every generic behavior, for a dialect no built-in service serves.
func Lookup(dialect string) dburlext.Services {
	return services[platform.NormalizeDialect(dialect)]
}

// Dialects returns every dialect a built-in service serves, in no order.
func Dialects() []string {
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	return names
}
