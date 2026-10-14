package ydbsql

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// appendPrincipal applies a statement that changes a YDB user or group, and
// refuses a CREATE USER or CREATE GROUP that repeats a principal the load
// already declares. The shared conversion records a principal the statement
// does not repeat.
func appendPrincipal(scope sqlschema.Scope, statement ast.Node) (bool, error) {
	switch node := statement.(type) {
	case *ast.CreateRoleNode:
		return refuseRepeatedPrincipal(scope, node)
	case *ast.AlterRoleNode:
		return true, alterUser(scope, node)
	case *ast.GrantRoleMembershipNode:
		return true, changeMembership(scope, node.Role, node.Member, "ADD")
	case *ast.RevokeRoleMembershipNode:
		return true, changeMembership(scope, node.Role, node.Member, "DROP")
	default:
		return false, nil
	}
}

// refuseRepeatedPrincipal refuses a principal declared twice, before the
// model's finalization could merge the two declarations.
func refuseRepeatedPrincipal(scope sqlschema.Scope, node *ast.CreateRoleNode) (bool, error) {
	name := scope.Name(node.Name)
	for _, source := range []*schemamodel.Database{scope.Database(), scope.Base()} {
		if source == nil {
			continue
		}
		for _, declared := range source.Roles {
			if declared.Name == name {
				return true, fmt.Errorf("%w: CREATE ROLE %q repeats a role already declared in this desired schema", sqlschema.ErrUnmodeledStatement, name)
			}
		}
	}
	return false, nil
}

func declaredPrincipal(scope sqlschema.Scope, name string) *schemamodel.Role {
	name = scope.Name(name)
	for _, source := range []*schemamodel.Database{scope.Database(), scope.Base()} {
		if source == nil {
			continue
		}
		for i := range source.Roles {
			if source.Roles[i].Name == name {
				return &source.Roles[i]
			}
		}
	}
	return nil
}

func alterUser(scope sqlschema.Scope, node *ast.AlterRoleNode) error {
	role := declaredPrincipal(scope, node.Name)
	if role == nil || role.Group {
		return fmt.Errorf("%w: ALTER USER requires a user declared earlier in this desired schema", sqlschema.ErrUnmodeledStatement)
	}
	for _, operation := range node.Operations {
		switch typed := operation.(type) {
		case *ast.SetLoginOperation:
			role.Login = typed.Login
		case *ast.SetPasswordOperation:
			// An absent password means unmanaged, not empty. Accepting a reset would
			// leave a live password unchanged while claiming the schema applied it.
			if typed.Password == "" {
				return fmt.Errorf("%w: ALTER USER cannot request an empty password in a desired schema; omitted passwords are unmanaged", sqlschema.ErrUnmodeledStatement)
			}
			role.Password = typed.Password
		default:
			return fmt.Errorf("%w: unsupported ALTER USER option", sqlschema.ErrUnmodeledStatement)
		}
	}
	return nil
}

func changeMembership(scope sqlschema.Scope, groupName, memberName, clause string) error {
	group := scope.Name(groupName)
	member := declaredPrincipal(scope, memberName)
	if member == nil {
		return fmt.Errorf("%w: a group membership requires a member declared earlier in this desired schema", sqlschema.ErrUnmodeledStatement)
	}
	target := declaredPrincipal(scope, groupName)
	if clause == "DROP" && target == nil {
		return fmt.Errorf("%w: removing a membership requires a group declared earlier in this desired schema; memberships in undeclared groups are preserved", sqlschema.ErrUnmodeledStatement)
	}
	if target != nil && !target.Group {
		return fmt.Errorf("%w: group membership target %q is declared as a user", sqlschema.ErrUnmodeledStatement, group)
	}
	if group == member.Name {
		return fmt.Errorf("%w: a group cannot name itself as a member in a desired schema", sqlschema.ErrUnmodeledStatement)
	}
	if clause == "ADD" {
		if !slices.Contains(member.MemberOf, group) {
			member.MemberOf = append(member.MemberOf, group)
		}
	} else {
		member.MemberOf = slices.DeleteFunc(member.MemberOf, func(name string) bool { return name == group })
	}
	return nil
}
