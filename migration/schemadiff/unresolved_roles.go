package schemadiff

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
)

// validateResolvedRoles refuses a declaration that names a role the read
// could not resolve: a [coverage.Role] record with the [coverage.Unresolved]
// reason. A MySQL-family read makes one where two accounts read as one
// principal, as the role 'nm@localhost'@'%' and the login 'nm'@'localhost'
// do, and leaves that name's privileges out of the read so the rest stays
// readable. A plan for the name would change one account while comparing
// against neither: a grant would be planned again, and a held grant the
// declaration leaves out would never be revoked.
func validateResolvedRoles(desired *schemamodel.Database, database *catalog.Database) error {
	if desired == nil || database == nil {
		return nil
	}
	var problems []error
	refused := make(map[string]bool)
	refuse := func(name string) {
		name = strings.TrimSpace(name)
		limit, limited := database.NotDescribed.Limit(coverage.Role, name)
		if name == "" || !limited || limit.WholeKind() || limit.Reason != coverage.Unresolved || refused[limit.Name] {
			return
		}
		refused[limit.Name] = true
		problems = append(problems, fmt.Errorf(
			"role %q names a principal the read could not resolve, such as two accounts that read as one name, "+
				"so a plan cannot say which one it changes: rename one of them", name,
		))
	}
	for _, role := range desired.Roles {
		refuse(role.Name)
		for _, group := range role.MemberOf {
			refuse(group)
		}
	}
	for _, grant := range desired.Grants {
		refuse(grant.Role)
	}
	for _, revoke := range desired.RevokedGrants {
		refuse(revoke.Role)
	}
	return errors.Join(problems...)
}
