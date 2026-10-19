// Package builtin composes the routine services of the engines Ptah ships.
// Each owner states its grammar names; consumers look them up without naming
// an engine or interpreting its routine options.
package builtin

import (
	"ptah.run/core/ast"
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

// Raw reads the permissive routine forms used when no owner grammar is
// selected. This product default deliberately combines the shipped grammars.
func Raw(cursor routineext.Cursor, start int, target string) (ast.Node, error) {
	var sql string
	var err error
	if target == "PROC" {
		sql, err = sqlserver.Raw(cursor, start)
	} else {
		sql, err = mysql.Collect(cursor, start)
	}
	if err != nil {
		return nil, err
	}
	return ast.NewRawSQL(sql), nil
}

// EndsSetting is the quoted-header SET boundary the permissive function
// reader shares with the quoted routine owner.
func EndsSetting(value string) bool { return postgres.EndsSetting(value) }
