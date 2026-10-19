// Package builtin composes the routine services of the engines Ptah ships.
// Each owner states its grammar names; consumers look them up without naming
// an engine or interpreting its routine options.
package builtin

import (
	"ptah.run/internal/routineext"
	"ptah.run/internal/routineext/mysql"
	"ptah.run/internal/routineext/postgres"
	"ptah.run/internal/routineext/sqlserver"
)

var services = index(mysql.Services(), postgres.Services(), sqlserver.Services())

func index(all ...routineext.Services) map[string]routineext.Services {
	result := make(map[string]routineext.Services)
	for _, service := range all {
		for _, grammar := range service.Grammars {
			if _, found := result[grammar]; found {
				panic("routineext/builtin: two services serve grammar " + grammar)
			}
			result[grammar] = service
		}
	}
	return result
}

// Lookup returns the services of a named descriptor.RoutineGrammar.Body,
// or zero services when no owner serves that grammar.
func Lookup(grammar string) routineext.Services { return services[grammar] }
