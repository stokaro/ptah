package mysqllike

import (
	"fmt"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
)

// ValidateDeclaredRoles refuses a MySQL-family schema whose declared roles
// Ptah cannot read or compare. Complete-schema validation and rendering share
// this check, while the visitors use the same error for direct AST rendering.
func ValidateDeclaredRoles(dialect string, roles []goschema.Role) error {
	normalized := platform.NormalizeDialect(dialect)
	if (normalized != platform.MySQL && normalized != platform.MariaDB) || len(roles) == 0 {
		return nil
	}
	return unsupportedRoleError(normalized, "CREATE ROLE", roles[0].Name)
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
