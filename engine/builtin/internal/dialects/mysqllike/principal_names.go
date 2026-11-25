package mysqllike

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// ValidateDeclaredPrincipalNames refuses unsupported MySQL-family principal
// spellings before comparison, including a revoke whose spelling would otherwise
// miss the catalog name. MySQL normalizes surrounding white space in role references.
// MariaDB preserves leading white space and trailing tabs, but CREATE ROLE
// removes trailing ASCII spaces. Requiring a stored spelling keeps the shared
// model's exact names from identifying another principal.
func ValidateDeclaredPrincipalNames(dialect string, database *schemamodel.Database) error {
	normalized := platform.NormalizeDialect(dialect)
	if (normalized != platform.MySQL && normalized != platform.MariaDB) || database == nil {
		return nil
	}
	var names []string
	for _, role := range database.Roles {
		names = append(names, role.Name)
		names = append(names, role.MemberOf...)
	}
	for _, grants := range [][]schemamodel.Grant{database.Grants, database.RevokedGrants, database.DeniedGrants} {
		for _, grant := range grants {
			names = append(names, grant.Role)
		}
	}
	slices.Sort(names)
	return validatePrincipalNames(dialect, names...)
}

func validatePrincipalNames(dialect string, names ...string) error {
	normalized := platform.NormalizeDialect(dialect)
	for _, name := range names {
		if normalized == platform.MySQL && strings.TrimSpace(name) != name {
			return fmt.Errorf("%w: mysql: principal %q has surrounding white space, which Ptah does not support for MySQL; use its stored name without padding",
				ptaherr.ErrUnsupportedFeature, name)
		}
		if normalized == platform.MariaDB && strings.HasSuffix(name, " ") {
			return fmt.Errorf("%w: mariadb: principal %q has trailing ASCII spaces, which CREATE ROLE removes; use its stored name without trailing spaces",
				ptaherr.ErrUnsupportedFeature, name)
		}
	}
	return nil
}
