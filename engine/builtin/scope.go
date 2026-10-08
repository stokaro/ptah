package builtin

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// prepareScopedDatabase resolves scope before validating target declarations.
// Direct built-in entry points use the registration naming declaration without
// constructing a provider registry for every schema.
func prepareScopedDatabase(database *schemamodel.Database, dialect string, caps capability.Capabilities) (*schemamodel.Database, error) {
	spelling, err := schemaext.NormalizeTargetSpelling(dialect)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, dialect)
	}
	name := platform.NormalizeDialect(spelling)
	if name == "" {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, dialect)
	}
	var aliases []string
	for _, candidate := range platform.DialectSpellings() {
		if candidate != name && platform.NormalizeDialect(candidate) == name {
			aliases = append(aliases, candidate)
		}
	}
	selected, err := schemaext.NewTargetSelection(name, aliases...)
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
	return scoped, nil
}
