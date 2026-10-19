// Package builtin lists the reach services of the lexers Ptah ships and
// looks one up by the lexer a descriptor names. It names no lexer itself: each
// service states the lexers it serves.
package builtin

import (
	"ptah.run/internal/reachext"
	"ptah.run/internal/reachext/yql"
)

// services maps each lexer a built-in service serves to it. It is built once
// from the list below and never written again.
var services = index(
	yql.Services(),
)

func index(all ...reachext.Services) map[string]reachext.Services {
	byLexer := make(map[string]reachext.Services)
	for _, served := range all {
		for _, lexer := range served.Lexers {
			if _, taken := byLexer[lexer]; taken {
				panic("reachext/builtin: two services serve lexer " + lexer)
			}
			byLexer[lexer] = served
		}
	}
	return byLexer
}

// Lookup returns the services of a lexer, named as descriptor.Lexical.Lexer
// names it, and the zero Services for the shared lexer, which a descriptor
// names with the empty string, and for a lexer no built-in service serves.
func Lookup(lexer string) reachext.Services {
	return services[lexer]
}
