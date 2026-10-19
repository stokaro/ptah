// Package builtin lists the DO-block readers of the procedural languages Ptah
// ships and looks one up by the language a descriptor names. It names no
// language itself: each service states the languages it serves.
package builtin

import (
	"ptah.run/internal/doblockext"
	"ptah.run/internal/doblockext/postgres"
)

// services maps each language a built-in service serves to it. It is built
// once from the list below and never written again.
var services = index(
	postgres.Services(),
)

func index(all ...doblockext.Services) map[string]doblockext.Services {
	byLanguage := make(map[string]doblockext.Services)
	for _, served := range all {
		for _, language := range served.Languages {
			if _, taken := byLanguage[language]; taken {
				panic("doblockext/builtin: two services serve language " + language)
			}
			byLanguage[language] = served
		}
	}
	return byLanguage
}

// Lookup returns the services of a language, named as
// descriptor.Statements.DoBlocks names it, and the zero Services for the
// empty name, which a descriptor of an engine without DO blocks states, and
// for a language no built-in service serves.
func Lookup(language string) doblockext.Services {
	return services[language]
}
