package sqlschema

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

func appendYDBPrincipal(database *schemamodel.Database, document *Document, statement ast.Node, dialect string) (bool, error) {
	if platform.NormalizeDialect(dialect) != platform.YDB {
		return false, nil
	}
	switch node := statement.(type) {
	case *ast.AlterRoleNode:
		return true, alterYDBUser(database, document, node)
	case *ast.GrantRoleMembershipNode:
		return true, changeYDBMembership(database, document, node.Role, node.Member, "ADD")
	case *ast.RevokeRoleMembershipNode:
		return true, changeYDBMembership(database, document, node.Role, node.Member, "DROP")
	default:
		return false, nil
	}
}

func declaredYDBPrincipal(database *schemamodel.Database, document *Document, name string) *schemamodel.Role {
	name = roleName(platform.YDB, name)
	for _, source := range []*schemamodel.Database{database, document.base} {
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

func alterYDBUser(database *schemamodel.Database, document *Document, node *ast.AlterRoleNode) error {
	role := declaredYDBPrincipal(database, document, node.Name)
	if role == nil || role.Group {
		return fmt.Errorf("%w: ALTER USER requires a user declared earlier in this desired schema", ErrUnmodeledStatement)
	}
	for _, operation := range node.Operations {
		switch typed := operation.(type) {
		case *ast.SetLoginOperation:
			role.Login = typed.Login
		case *ast.SetPasswordOperation:
			// An absent password means unmanaged, not empty. Accepting a reset would
			// leave a live password unchanged while claiming the schema applied it.
			if typed.Password == "" {
				return fmt.Errorf("%w: ALTER USER cannot request an empty password in a desired schema; omitted passwords are unmanaged", ErrUnmodeledStatement)
			}
			role.Password = typed.Password
		default:
			return fmt.Errorf("%w: unsupported ALTER USER option", ErrUnmodeledStatement)
		}
	}
	return nil
}

func changeYDBMembership(database *schemamodel.Database, document *Document, groupName, memberName, clause string) error {
	group := roleName(platform.YDB, groupName)
	member := declaredYDBPrincipal(database, document, memberName)
	if member == nil {
		return fmt.Errorf("%w: a group membership requires a member declared earlier in this desired schema", ErrUnmodeledStatement)
	}
	target := declaredYDBPrincipal(database, document, groupName)
	if clause == "DROP" && target == nil {
		return fmt.Errorf("%w: removing a membership requires a group declared earlier in this desired schema; memberships in undeclared groups are preserved", ErrUnmodeledStatement)
	}
	if target != nil && !target.Group {
		return fmt.Errorf("%w: group membership target %q is declared as a user", ErrUnmodeledStatement, group)
	}
	if group == member.Name {
		return fmt.Errorf("%w: a group cannot name itself as a member in a desired schema", ErrUnmodeledStatement)
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
