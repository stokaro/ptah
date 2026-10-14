package privilegecompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// Service compares captured default-privilege objects. Its zero value is ready
// for concurrent use. Declared grantors and explicitly declared identities grant
// removal authority; grantees do not. Global deltas retain built-in privileges.
type Service struct{}

type pair struct {
	ref      objectidentity.ID
	identity pgdefaultprivilege.Identity
	desired  *pgdefaultprivilege.Desired
	observed *pgdefaultprivilege.Observed
}

// CompareObjects returns ordered changes without mutating either snapshot.
// Unknown catalog knowledge withholds changes instead of treating silence as
// absence. Errors and cancellation return no partial result.
func (Service) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: default privilege comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: default privilege comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{pgdefaultprivilege.DefaultPrivilegeKind}) || len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported default privilege comparison request", schemaext.ErrInvalidValue)
	}
	pairs, err := capturePairs(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	roles := make([]string, 0, len(request.DeclaredRoles))
	for _, role := range request.DeclaredRoles {
		roles = append(roles, role.Name.Source)
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		comparePair(pair, roles, request, &result)
	}
	return result, ctx.Err()
}

func comparePair(p pair, roles []string, request schemaext.ObjectComparisonRequest, result *schemaext.ObjectComparisonResult) {
	desiredKnowledge := pairKnowledge(request.Desired.Coverage, p, request)
	if unknown(desiredKnowledge) {
		if p.desired != nil {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: p.ref,
				Reason: "desired default privileges are not established: " + desiredKnowledge.Reason})
		}
		return
	}
	currentKnowledge := pairKnowledge(request.Current.Coverage, p, request)
	if unknown(currentKnowledge) {
		if change, ok := convergeOwner(p, roles, request); ok {
			result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: p.ref, Value: change})
			return
		}
		if p.desired != nil {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: p.ref,
				Reason: "default privileges are not established: " + currentKnowledge.Reason})
		}
		return
	}
	var desired []declaration
	if p.desired != nil {
		desired = append(desired, declaration{Identity: p.identity, Desired: p.desired})
	}
	var observed []observedRow
	if p.observed != nil {
		for _, granted := range p.observed.Privileges {
			observed = append(observed, observedRow{entry: privilegeEntry(p.identity, granted)})
		}
		for _, revoked := range p.observed.Revoked {
			observed = append(observed, observedRow{entry: privilegeEntry(p.identity, pgdefaultprivilege.Grant{Privilege: revoked}), Revoked: true})
		}
	}
	difference := calculate(desired, observed, roles, request.Identifiers)
	before := p.observed.Copy()
	if before == nil {
		before = &pgdefaultprivilege.Observed{}
	}
	change := &pgdefaultprivilege.Change{Before: before, Granted: grantsOf(difference.grants), Revoked: grantsOf(difference.revokes),
		GrantOptionsAdded: namesOf(difference.optionsAdded), GrantOptionsRevoked: namesOf(difference.optionsRevoked)}
	if len(change.Granted)+len(change.Revoked)+len(change.GrantOptionsAdded)+len(change.GrantOptionsRevoked) != 0 {
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: p.ref, Value: change})
	}
}

func privilegeEntry(identity pgdefaultprivilege.Identity, grant pgdefaultprivilege.Grant) entry {
	return entry{Grantor: identity.Grantor, Schema: identity.Schema, ObjectType: identity.ObjectType,
		Grantee: identity.Grantee, Privilege: grant.Privilege, WithOption: grant.WithOption}
}

func grantsOf(entries []entry) []pgdefaultprivilege.Grant {
	var result []pgdefaultprivilege.Grant
	for _, entry := range entries {
		result = append(result, pgdefaultprivilege.Grant{Privilege: entry.Privilege, WithOption: entry.WithOption})
	}
	return result
}

func namesOf(entries []entry) []string {
	var result []string
	for _, entry := range entries {
		result = append(result, entry.Privilege)
	}
	return result
}

func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

// A named class limit applies to every grantee in that ACL. FOR ALL ROLES is a
// separate catalog scope and does not hide a named role's inspected defaults.
func pairKnowledge(known schemaext.Coverage, p pair, request schemaext.ObjectComparisonRequest) schemaext.Knowledge {
	for _, claim := range known.SubjectRecords() {
		if claim.Subject.Kind != pgdefaultprivilege.ClassSubjectKind {
			continue
		}
		class, err := pgdefaultprivilege.ClassOf(claim.Subject)
		if err != nil {
			continue
		} // capturePairs validates every subject before comparison.
		if class.Grantor != "" && class.ObjectType == p.identity.ObjectType &&
			request.Identifiers.TableIdentityKey(class.Grantor) == request.Identifiers.TableIdentityKey(p.identity.Grantor) &&
			request.Identifiers.TableIdentityKey(class.Schema) == request.Identifiers.TableIdentityKey(p.identity.Schema) {
			return claim.Knowledge
		}
	}
	return known.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, p.ref)
}
