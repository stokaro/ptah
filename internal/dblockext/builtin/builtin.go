// Package builtin lists the lock services of the engines Ptah ships and looks
// one up by dialect. It names no engine itself: each service states the
// dialects it serves.
package builtin

import (
	"ptah.run/core/platform"
	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/mysql"
	"ptah.run/internal/dblockext/postgres"
	"ptah.run/internal/dblockext/sqlserver"
	"ptah.run/internal/dblockext/ydb"
)

// services maps each dialect a built-in service serves to it. It is built
// once from the list below and never written again.
var services = index(
	postgres.Services(),
	mysql.MySQL(),
	mysql.MariaDB(),
	sqlserver.Services(),
	ydb.Services(),
)

func index(all ...dblockext.Services) map[string]dblockext.Services {
	byDialect := make(map[string]dblockext.Services)
	for _, served := range all {
		for _, dialect := range served.Dialects {
			if _, taken := byDialect[dialect]; taken {
				panic("dblockext/builtin: two services serve dialect " + dialect)
			}
			byDialect[dialect] = served
		}
	}
	return byDialect
}

// Lookup returns the services of the dialect a spelling selects, folded as
// platform.NormalizeDialect folds it, and the zero Services, which take no
// lock, for a dialect no built-in service serves.
func Lookup(dialect string) dblockext.Services {
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
