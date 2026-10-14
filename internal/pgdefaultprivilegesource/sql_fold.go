package pgdefaultprivilegesource

import (
	"fmt"
	"slices"

	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/pgprivilege"
)

// applySQLOperation folds ordered statements, unlike the unordered declarations
// other sources combine. It captures before and never changes the caller's ACL.
func applySQLOperation(before *pgdefaultprivilege.Desired, operation *pgdefaultprivilege.Operation) (*pgdefaultprivilege.Desired, error) {
	state := before.Canonical()
	privileges := names(operation.Privileges)
	switch operation.Action {
	case pgdefaultprivilege.GrantRights:
		applySQLGrant(state, operation, privileges)
	case pgdefaultprivilege.RevokeRights:
		if err := applySQLRevoke(state, privileges); err != nil {
			return nil, err
		}
	case pgdefaultprivilege.RevokeGrantOptions:
		if err := applySQLOptionRevoke(state, operation.Identity.ObjectType, privileges); err != nil {
			return nil, err
		}
	}
	state = state.Canonical()
	if err := pgdefaultprivilege.ValidateDesired(state); err != nil {
		return nil, err
	}
	return state, nil
}

func applySQLGrant(state *pgdefaultprivilege.Desired, operation *pgdefaultprivilege.Operation, privileges []string) {
	all := slices.Contains(privileges, "ALL")
	if !all && slices.Contains(state.Revoked, "ALL") {
		state.Revoked = pgprivilege.All(operation.Identity.ObjectType)
	}
	state.Revoked = slices.DeleteFunc(state.Revoked, func(name string) bool {
		return all || slices.Contains(privileges, name)
	})
	for _, name := range privileges {
		state.Privileges = append(state.Privileges, pgdefaultprivilege.Grant{Privilege: name, WithOption: operation.WithOption})
	}
}

func applySQLRevoke(state *pgdefaultprivilege.Desired, privileges []string) error {
	all := slices.Contains(privileges, "ALL")
	if !all && slices.ContainsFunc(state.Privileges, func(grant pgdefaultprivilege.Grant) bool { return grant.Privilege == "ALL" }) {
		return fmt.Errorf("cannot revoke individual default privileges from a declared ALL without a server-specific privilege set; declare the individual grants")
	}
	state.Privileges = slices.DeleteFunc(state.Privileges, func(grant pgdefaultprivilege.Grant) bool {
		return all || slices.Contains(privileges, grant.Privilege)
	})
	if all {
		state.Revoked = []string{"ALL"}
	} else {
		state.Revoked = append(state.Revoked, privileges...)
	}
	return nil
}

func applySQLOptionRevoke(state *pgdefaultprivilege.Desired, objectType string, privileges []string) error {
	all := slices.Contains(privileges, "ALL")
	allIndex := slices.IndexFunc(state.Privileges, func(grant pgdefaultprivilege.Grant) bool { return grant.Privilege == "ALL" })
	if all {
		privileges = pgprivilege.All(objectType)
	}
	if !all && allIndex >= 0 && state.Privileges[allIndex].WithOption {
		return fmt.Errorf("cannot revoke individual grant options from a declared ALL WITH GRANT OPTION without a server-specific privilege set; declare the individual grants")
	}
	for _, name := range privileges {
		index := slices.IndexFunc(state.Privileges, func(grant pgdefaultprivilege.Grant) bool { return grant.Privilege == name })
		if index < 0 && allIndex < 0 {
			return fmt.Errorf("REVOKE GRANT OPTION FOR %s names a default privilege this schema does not grant; declare the GRANT without WITH GRANT OPTION", name)
		}
	}
	for i := range state.Privileges {
		if all || slices.Contains(privileges, state.Privileges[i].Privilege) {
			state.Privileges[i].WithOption = false
		}
	}
	return nil
}
