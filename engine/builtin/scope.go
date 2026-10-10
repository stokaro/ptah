package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy/policyrender"
)

// prepareScopedDatabase resolves scope before validating target declarations.
// Direct built-in entry points use the registration naming declaration without
// constructing a provider registry for every schema.
func prepareScopedDatabase(database *schemamodel.Database, dialect string, caps capability.Capabilities) (*schemamodel.Database, error) {
	selected, err := resolveTargetSelection(dialect)
	if err != nil {
		return nil, err
	}
	scoped, err := schemamodel.ScopeToTarget(database, selected)
	if err != nil {
		return nil, err
	}
	if err := validateDatabaseDeclarations(dialect, caps, scoped); err != nil {
		return nil, err
	}
	if scoped == nil {
		return nil, nil
	}
	// A render answers the row-security owner's default requests itself, on
	// every target: it writes no switch statement the schema does not declare.
	coverage, err := policyrender.LowerCoverage(scoped.FeatureCoverage)
	if err != nil {
		return nil, err
	}
	lowered := *scoped
	lowered.FeatureCoverage = coverage
	return &lowered, nil
}

// Both whole-schema and direct AST entry points resolve source bindings with
// the same names as the built-in registration. An excluded value cannot be
// reused on a target that its captured source binding includes.
func resolveTargetSelection(dialect string) (schemaext.TargetSelection, error) {
	spelling, err := schemaext.NormalizeTargetSpelling(dialect)
	if err != nil {
		return schemaext.TargetSelection{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, dialect)
	}
	name := platform.NormalizeDialect(spelling)
	if name == "" {
		return schemaext.TargetSelection{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, dialect)
	}
	var aliases []string
	for _, candidate := range platform.DialectSpellings() {
		if candidate != name && platform.NormalizeDialect(candidate) == name {
			aliases = append(aliases, candidate)
		}
	}
	return schemaext.NewTargetSelection(name, aliases...)
}
