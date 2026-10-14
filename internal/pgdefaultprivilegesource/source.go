// Package pgdefaultprivilegesource owns default-privilege schema declarations.
// Source frontends pass data to the owner; identity, privilege sets, target
// scopes, and declaration conflicts have one interpretation across formats.
package pgdefaultprivilegesource

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/dialectscope"
)

// Directive is the Go annotation for privileges on a role's future objects.
const Directive = "ptah:schema:defaultprivilege"

// Key is the top-level YAML section for default-privilege declarations.
const Key = "default_privileges"

// Declare captures a source declaration. A grantable name must occur in grants;
// an empty grant/revoke pair is refused. Empty schema means the global default.
// Target names are normalized separately from the PostgreSQL object identity.
func Declare(identity pgdefaultprivilege.Identity, grants, grantable, revoked []string, comment string, targets []string) (schemaext.Object, error) {
	ref, err := pgdefaultprivilege.Ref(identity, identifier.ForDialect("postgres"))
	if err != nil {
		return schemaext.Object{}, err
	}
	identity, err = pgdefaultprivilege.IdentityOf(ref)
	if err != nil {
		return schemaext.Object{}, err
	}
	privileges, options, revokes := names(grants), names(grantable), names(revoked)
	if len(privileges)+len(revokes) == 0 {
		return schemaext.Object{}, fmt.Errorf("%w: a default privilege requires privileges or revoked", schemaext.ErrInvalidValue)
	}
	for _, name := range options {
		if !slices.Contains(privileges, name) {
			return schemaext.Object{}, fmt.Errorf("%w: grantable privilege %q is not in privileges", schemaext.ErrInvalidValue, name)
		}
	}
	desired := pgdefaultprivilege.Desired{Revoked: revokes, Comment: comment}
	for _, name := range privileges {
		desired.Privileges = append(desired.Privileges, pgdefaultprivilege.Grant{Privilege: name, WithOption: slices.Contains(options, name)})
	}
	for _, group := range [][]string{privileges, revokes} {
		if len(group) == 0 {
			continue
		}
		if err := (&pgdefaultprivilege.Operation{Identity: identity, Action: pgdefaultprivilege.GrantRights, Privileges: group}).Validate(); err != nil {
			return schemaext.Object{}, err
		}
	}
	object, err := pgdefaultprivilege.DesiredObject(ref, desired)
	if err != nil {
		return schemaext.Object{}, err
	}
	if len(targets) > 0 {
		object.Targets, err = dialectscope.Parse(strings.Join(targets, ","))
		if err != nil {
			return schemaext.Object{}, err
		}
	}
	return object, nil
}

func names(values []string) []string {
	var result []string
	for _, value := range values {
		value = strings.ToUpper(strings.TrimSpace(value))
		if value != "" {
			result = append(result, value)
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}
