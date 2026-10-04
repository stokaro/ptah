package ydbacl

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// ValidateDeclared refuses the declared users, groups, memberships and grants
// YDB cannot hold, and returns nil for every other dialect.
//
// It runs where a declaration is validated before anything renders or plans,
// so the refusal arrives before a server is changed. Each problem is reported,
// joined, in name order, so a schema with three mistakes is one run rather
// than three.
func ValidateDeclared(dialect string, database *schemamodel.Database) error {
	if platform.NormalizeDialect(dialect) != platform.YDB || database == nil {
		return nil
	}
	declared := make(map[string]schemamodel.Role, len(database.Roles))
	for _, role := range database.Roles {
		declared[role.Name] = role
	}
	problems := make([]error, 0)
	for _, role := range slices.SortedFunc(slices.Values(database.Roles), byRoleName) {
		problems = append(problems, roleProblems(role, declared)...)
	}
	views := make(map[string]bool, len(database.Views))
	for _, view := range database.Views {
		views[strings.TrimSpace(view.Name)] = true
	}
	for _, grant := range database.Grants {
		problems = append(problems, grantProblems("grant", grant, views)...)
	}
	for _, revoked := range database.RevokedGrants {
		problems = append(problems, grantProblems("revoke", revoked, views)...)
	}
	for _, defaultPrivilege := range database.DefaultPrivileges {
		problems = append(problems, fmt.Errorf("default privilege for %q in %q: YDB has no default privileges; "+
			"a permission granted on a directory is inherited by every object created in it, "+
			"so grant it on the directory instead", defaultPrivilege.Grantor, defaultPrivilege.Schema))
	}
	return errors.Join(problems...)
}

func byRoleName(a, b schemamodel.Role) int { return cmp.Compare(a.Name, b.Name) }

// roleProblems reports what a declared role asks of YDB that a user or a group
// cannot hold.
func roleProblems(role schemamodel.Role, declared map[string]schemamodel.Role) []error {
	kind := "user"
	if role.Group {
		kind = "group"
	}
	var problems []error
	if err := CheckName(role.Name); err != nil {
		problems = append(problems, fmt.Errorf("%s %q: %w; measured, other names answer `Name is not allowed`",
			kind, role.Name, err))
	}
	for _, attribute := range []struct {
		name     string
		declared bool
	}{
		{"superuser", role.Superuser},
		{"createdb", role.CreateDB},
		{"createrole", role.CreateRole},
		{"replication", role.Replication},
	} {
		if attribute.declared {
			problems = append(problems, fmt.Errorf("%s %q declares %s: a YDB %s carries no such attribute; "+
				"grant a permission, or make it a member of a group that holds one, such as ADMINS",
				kind, role.Name, attribute.name, kind))
		}
	}
	if role.Group && role.Login {
		problems = append(problems, fmt.Errorf("group %q declares login: a YDB group never logs in", role.Name))
	}
	if role.Group && role.Password != "" {
		// The value is not interpolated: it is a credential, and this error
		// travels to stderr and into whatever collects it.
		problems = append(problems, fmt.Errorf("group %q declares a password: a YDB group never logs in",
			role.Name))
	}
	for _, group := range role.MemberOf {
		problems = append(problems, membershipProblems(role, kind, group, declared)...)
	}
	return problems
}

// membershipProblems reports a membership YDB cannot hold or Ptah cannot plan.
//
// A group the schema does not declare is accepted: YDB's own groups, such as
// DATA-READERS, are created by the cluster, and joining one is how a user gets
// the access it stands for. The server refuses a group that does not exist
// (`Group not found`) when the plan runs.
func membershipProblems(role schemamodel.Role, kind, group string, declared map[string]schemamodel.Role) []error {
	group = strings.TrimSpace(group)
	switch group {
	case "":
		return []error{fmt.Errorf("%s %q names an empty group in member_of", kind, role.Name)}
	case role.Name:
		// Measured: YDB accepts `ALTER GROUP g ADD USER g`, and the group then
		// lists itself as a member, which grants nothing and reads as a cycle.
		return []error{fmt.Errorf("%s %q names itself in member_of", kind, role.Name)}
	}
	if target, ok := declared[group]; ok && !target.Group {
		return []error{fmt.Errorf("%s %q is a member of %q, which the schema declares as a user: "+
			"only a YDB group has members", kind, role.Name, group)}
	}
	return nil
}

// grantProblems reports what a declared grant or revoke asks of YDB that its
// access lists cannot hold. verb names which of the two it is, and views are
// the declared views by name.
//
// A grant on a view is refused because the reader does not read a view's
// permission entries: a plan would make the grant, never see it, and make it
// again on every run.
func grantProblems(verb string, grant schemamodel.Grant, views map[string]bool) []error {
	grant.Canonicalize()
	subject := fmt.Sprintf("%s to %q", verb, grant.Role)
	var problems []error
	if strings.EqualFold(grant.Role, "PUBLIC") {
		problems = append(problems, fmt.Errorf("%s: YDB has no PUBLIC; every user is a member of the "+
			"cluster's all-users group, USERS on a default cluster, so grant to that group instead", subject))
	}
	if targets := grantTargets(grant); targets != 1 {
		problems = append(problems, fmt.Errorf("%s names %d targets: a YDB grant is on the database "+
			"(on_database), a directory (on_schema) or a table (on_table)", subject, targets))
	}
	switch {
	case strings.TrimSpace(grant.OnSequence) != "":
		problems = append(problems, fmt.Errorf("%s on sequence %q: a YDB grant is on the database, "+
			"a directory or a table", subject, grant.OnSequence))
	case strings.TrimSpace(grant.OnRoutine) != "":
		problems = append(problems, fmt.Errorf("%s on routine %q: YDB has no routines", subject, grant.OnRoutine))
	}
	if views[strings.TrimSpace(grant.OnTable)] {
		problems = append(problems, fmt.Errorf("%s on view %q: Ptah does not read a YDB view's permissions yet, "+
			"so a plan could not see the grant it made; grant on the view's directory, whose permissions the "+
			"objects in it inherit", subject, grant.OnTable))
	}
	if len(grant.Columns) > 0 {
		problems = append(problems, fmt.Errorf("%s on columns %s of %q: a YDB permission is on a whole object",
			subject, strings.Join(grant.Columns, ", "), grant.OnTable))
	}
	if grant.WithOption {
		// Measured: WITH GRANT OPTION adds a second entry, ydb.access.grant,
		// and REVOKE GRANT OPTION FOR SELECT removes it together with SELECT,
		// so an option on one permission cannot be read back or taken away.
		problems = append(problems, fmt.Errorf("%s declares with_option: YDB records it as a permission of "+
			"its own, %s, beside the one it was written with, and REVOKE GRANT OPTION FOR takes both; "+
			"declare the GRANT privilege instead", subject, GrantPermission))
	}
	if len(grant.Privileges) == 0 {
		problems = append(problems, fmt.Errorf("%s names no privilege", subject))
	}
	for _, privilege := range grant.Privileges {
		if _, ok := Permission(privilege); !ok {
			problems = append(problems, fmt.Errorf("%s names privilege %q, which is not a YDB permission: "+
				"name one as YDB does, such as ydb.granular.select_row, or as GRANT spells it, such as SELECT ROW",
				subject, privilege))
		}
	}
	return problems
}

// grantTargets counts the targets a grant names.
func grantTargets(grant schemamodel.Grant) int {
	count := 0
	for _, named := range []bool{
		grant.OnDatabase,
		strings.TrimSpace(grant.OnSchema) != "",
		strings.TrimSpace(grant.OnTable) != "",
		strings.TrimSpace(grant.OnSequence) != "",
		strings.TrimSpace(grant.OnRoutine) != "",
	} {
		if named {
			count++
		}
	}
	return count
}
