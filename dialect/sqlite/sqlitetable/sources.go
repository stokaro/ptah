package sqlitetable

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
)

// Annotations is the owner's contribution to the Go annotation frontend. A Go
// annotation states a table's options as the platform properties
// `platform.sqlite.strict` and `platform.sqlite.without_rowid`, which every
// table directive can carry, so a table that states neither has neither. The
// claim is [OptionsCoverage].
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner:    Owner,
		Kinds:    []schemaext.Kind{TableKind},
		Coverage: annotation.Unlimited(OptionsCoverage),
	}
}

// YAML is the owner's contribution to the YAML schema frontend. A YAML table
// states its options in its sqlite platform group, so the document could have
// stated them on any table, and the claim is [OptionsCoverage].
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: Owner, Kinds: []schemaext.Kind{TableKind}, Coverage: OptionsCoverage}
}

// OptionsCoverage is the claim of a source that can state the options of
// every table it declares: a table it declares without them has neither.
func OptionsCoverage() (schemaext.Coverage, error) {
	return TableCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}
