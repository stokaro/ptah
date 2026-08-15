package mysqllike

import (
	"cmp"
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
)

// ValidateDeclaredRoles refuses a MySQL-family schema whose declared roles
// Ptah cannot read or compare. Complete-schema validation and rendering share
// this check, while the visitors use the same error for direct AST rendering.
//
// When a schema declares several roles, the sentence names the lexicographically
// first of them rather than the first one parsed. Two gates answer the same
// schema -- this one, before a caller asks for SQL, and the MySQL planner, which
// sorts diff.RolesAdded and so refuses on the alphabetically first CREATE ROLE
// it renders. Naming the parse-order role here made the two disagree about the
// same schema, and moved the name whenever a declaration was reordered.
func ValidateDeclaredRoles(dialect string, roles []goschema.Role) error {
	normalized := platform.NormalizeDialect(dialect)
	if (normalized != platform.MySQL && normalized != platform.MariaDB) || len(roles) == 0 {
		return nil
	}
	first := slices.MinFunc(roles, func(a, b goschema.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return unsupportedRoleError(normalized, "CREATE ROLE", first.Name)
}

func unsupportedRoleError(dialect, operation, name string) error {
	return fmt.Errorf(
		"%w: %s: %s %s: Ptah does not read or compare MySQL-family role state; manage roles outside Ptah for this target",
		ptaherr.ErrUnsupportedFeature,
		dialect,
		operation,
		name,
	)
}
