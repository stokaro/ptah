// Package privilegeselect describes the schema and roles default privileges
// reference. The host applies selection policy through core/selectext.
package privilegeselect

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/feature/pgdefaultprivilege"
)

// Service describes captured default privileges without catalog I/O. Its zero
// value is ready for concurrent use.
type Service struct{}

// Extension registers default privileges as schema-wide state without a named
// object selector. A global ACL remains global when a connection names a schema.
func Extension() selectext.Extension {
	return selectext.Extension{Owner: pgdefaultprivilege.Owner, Scopes: Service{}, Baseline: Service{}, Additive: Service{}, ChangeKinds: []schemaext.Kind{pgdefaultprivilege.ChangeKind}, Selections: []selectext.Selection{
		{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Naming: selectext.SchemaWide},
	}}
}

// DescribeScopes describes both roles of each ACL and the named grantor of an
// unknown catalog class. FOR ALL ROLES has no named grantor. Coverage-only
// subjects report scope without inventing a value or a declared role.
func (Service) DescribeScopes(ctx context.Context, request selectext.ScopeRequest) (selectext.ScopeResult, error) {
	if ctx == nil {
		return selectext.ScopeResult{}, fmt.Errorf("%w: scope description requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return selectext.ScopeResult{}, err
	}
	if err := validate(request); err != nil {
		return selectext.ScopeResult{}, err
	}
	result := selectext.ScopeResult{Complete: true}
	for _, subject := range request.Subjects() {
		if err := ctx.Err(); err != nil {
			return selectext.ScopeResult{}, err
		}
		scope, err := describe(subject)
		if err != nil {
			return selectext.ScopeResult{}, err
		}
		result.Subjects = append(result.Subjects, scope)
	}
	if err := ctx.Err(); err != nil {
		return selectext.ScopeResult{}, err
	}
	return result, nil
}

func validate(request selectext.ScopeRequest) error {
	objects, err := request.Objects.All()
	if err != nil {
		return err
	}
	for _, object := range objects {
		switch value := object.Value.(type) {
		case *pgdefaultprivilege.Desired:
			if err := pgdefaultprivilege.ValidateDesired(value); err != nil {
				return err
			}
		case *pgdefaultprivilege.Observed:
			if err := pgdefaultprivilege.ValidateObserved(value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unexpected default privilege value %T", schemaext.ErrInvalidValue, value)
		}
	}
	for _, subject := range request.Coverage.SubjectRecords() {
		if err := pgdefaultprivilege.ValidateCoverageSubject(subject); err != nil {
			return err
		}
	}
	return nil
}

func describe(subject selectext.ScopeSubject) (selectext.SubjectScope, error) {
	if subject.Kind != pgdefaultprivilege.DefaultPrivilegeKind {
		return selectext.SubjectScope{}, fmt.Errorf("%w: unrelated default privilege scope", schemaext.ErrInvalidValue)
	}
	scope := selectext.SubjectScope{ScopeSubject: subject}
	if subject.Subject.Kind == pgdefaultprivilege.ClassSubjectKind {
		class, err := pgdefaultprivilege.ClassOf(subject.Subject)
		if err != nil {
			return selectext.SubjectScope{}, err
		}
		scope.Schema, scope.Roles = class.Schema, namedRoles(class.Grantor)
		return scope, nil
	}
	identity, err := pgdefaultprivilege.IdentityOf(subject.Subject)
	if err != nil {
		return selectext.SubjectScope{}, err
	}
	scope.Schema, scope.Roles = identity.Schema, namedRoles(identity.Grantor, identity.Grantee)
	return scope, nil
}

func namedRoles(names ...string) []string {
	var roles []string
	for _, name := range names {
		switch strings.ToUpper(name) {
		case "", "PUBLIC", "CURRENT_ROLE", "CURRENT_USER", "SESSION_USER":
			continue
		}
		roles = append(roles, name)
	}
	slices.Sort(roles)
	return slices.Compact(roles)
}
