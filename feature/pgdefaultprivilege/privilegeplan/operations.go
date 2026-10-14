package privilegeplan

import (
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgdefaultacl"
)

// operations preserves the order a restoration requires: clear the ACL, restore
// its built-in base, apply built-in revocations, then restore additional rights.
// ALL remains symbolic so an older server never receives a newer privilege name.
func operations(identity pgdefaultprivilege.Identity, change *pgdefaultprivilege.Change) []*pgdefaultprivilege.Operation {
	var result []*pgdefaultprivilege.Operation
	add := func(action pgdefaultprivilege.Action, names []string, withOption bool) {
		if len(names) != 0 {
			result = append(result, &pgdefaultprivilege.Operation{Identity: identity, Action: action, Privileges: names, WithOption: withOption})
		}
	}
	grants := func(values []pgdefaultprivilege.Grant) {
		var plain, grantable []string
		for _, value := range values {
			if value.WithOption {
				grantable = append(grantable, value.Privilege)
			} else {
				plain = append(plain, value.Privilege)
			}
		}
		add(pgdefaultprivilege.GrantRights, plain, false)
		add(pgdefaultprivilege.GrantRights, grantable, true)
	}
	if change.Restore != nil {
		add(pgdefaultprivilege.RevokeRights, []string{"ALL"}, false)
		if identity.Schema == "" {
			switch {
			case pgdefaultacl.Builtin(identity.ObjectType, identity.Grantor, identity.Grantee, "ALL"):
				add(pgdefaultprivilege.GrantRights, []string{"ALL"}, false)
			case pgdefaultacl.Builtin(identity.ObjectType, identity.Grantor, identity.Grantee, "EXECUTE"):
				add(pgdefaultprivilege.GrantRights, []string{"EXECUTE"}, false)
			case pgdefaultacl.Builtin(identity.ObjectType, identity.Grantor, identity.Grantee, "USAGE"):
				add(pgdefaultprivilege.GrantRights, []string{"USAGE"}, false)
			}
		}
		add(pgdefaultprivilege.RevokeRights, change.Restore.Revoked, false)
		grants(change.Restore.Privileges)
		return result
	}
	add(pgdefaultprivilege.RevokeGrantOptions, change.GrantOptionsRevoked, false)
	var revoked []string
	for _, grant := range change.Revoked {
		revoked = append(revoked, grant.Privilege)
	}
	add(pgdefaultprivilege.RevokeRights, revoked, false)
	grants(change.Granted)
	add(pgdefaultprivilege.GrantRights, change.GrantOptionsAdded, true)
	return result
}
