package privilegecompare

import (
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgprivilege"
)

// convergeOwner handles a CockroachDB owner ACL whose exact global identity the
// reader marks unrepresentable. A whole unread catalog is not this observation.
// Only ALL operations converge from every possible initial subset. The change
// records that reversal cannot reconstruct that subset.
func convergeOwner(p pair, roles []string, request schemaext.ObjectComparisonRequest) (*pgdefaultprivilege.Change, bool) {
	if platform.NormalizeDialect(request.Target) != platform.CockroachDB || p.identity.Schema != "" ||
		request.Identifiers.TableIdentityKey(p.identity.Grantor) != request.Identifiers.TableIdentityKey(p.identity.Grantee) {
		return nil, false
	}
	knowledge, explicit := request.Current.Coverage.SubjectKnowledge(pgdefaultprivilege.DefaultPrivilegeKind, p.ref)
	if !explicit || knowledge.State != schemaext.Unrepresentable {
		return nil, false
	}
	var desired pgdefaultprivilege.Desired
	if p.desired != nil {
		desired = *p.desired
	}
	revoked := desired.Revoked
	if pgprivilege.NamesAll(p.identity.ObjectType, revoked) {
		revoked = []string{allPrivilege}
	}
	change := &pgdefaultprivilege.Change{UnknownBefore: true}
	switch {
	case len(desired.Privileges) == 0 && slices.Equal(revoked, []string{allPrivilege}):
		change.Revoked = []pgdefaultprivilege.Grant{{Privilege: allPrivilege}}
	case len(desired.Privileges) == 0 && len(revoked) == 0:
		managed := slices.ContainsFunc(roles, func(role string) bool {
			return request.Identifiers.TableIdentityKey(role) == request.Identifiers.TableIdentityKey(p.identity.Grantor)
		})
		if !managed && p.desired == nil {
			return nil, false
		}
		change.Granted = []pgdefaultprivilege.Grant{{Privilege: allPrivilege}}
	case len(revoked) == 0 && len(desired.Privileges) == 1 && desired.Privileges[0].Privilege == allPrivilege:
		change.Granted = slices.Clone(desired.Privileges)
	default:
		return nil, false
	}
	return change, true
}
