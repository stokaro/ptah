package modelast

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
)

// appendDeclaredGrants writes whole-object grants before column grants. A
// table option revoke reaches its columns too, so per-privilege revokes stay
// beside their grants in that order. An object-wide option is taken only
// once, after every grant: MySQL refuses a second revoke on the same object,
// and a later plain grant can make an already revoked option grantable again.
func (l Lowering) appendDeclaredGrants(
	visit func(ast.Node) error, grants []schemamodel.Grant, target descriptor.Dialect,
) error {
	names := l.grantName()
	for _, columns := range []bool{false, true} {
		for _, grant := range grants {
			if (len(grant.Columns) > 0) != columns {
				continue
			}
			if target.GrantOptionPerObject {
				grant.GrantOptionRevoked = false
			}
			if err := l.appendGrantStatements(visit, grant, names); err != nil {
				return err
			}
		}
	}
	if target.GrantOptionPerObject {
		return appendObjectGrantOptionRevokes(visit, grants, names)
	}
	return nil
}

// appendGrantStatements keeps an explicit option revoke beside its grant.
// A GRANT without WITH GRANT OPTION leaves an existing option held, so the
// revoke is required even though the grant records WithOption as false.
func (l Lowering) appendGrantStatements(visit func(ast.Node) error, grant schemamodel.Grant, names grantName) error {
	nodes := []ast.Node{withGrantPath(FromGrant(grant), names)}
	if grant.GrantOptionRevoked {
		revoke := FromRevokedGrant(grant).SetGrantOptionFor(true)
		revoke.ObjectName = names.rename(revoke.ObjectType, revoke.ObjectName)
		nodes = append(nodes, revoke)
		if l.GrantOptionRevokesFirst {
			slices.Reverse(nodes)
		}
	}
	return visitDatabaseNodes(visit, nodes...)
}

// objectGrantOption identifies the grantee and securable that share one
// grant option. Columns and privileges share it, while another grantee,
// object kind, name or routine signature does not.
type objectGrantOption struct {
	role, kind, name, arguments string
}

// appendObjectGrantOptionRevokes retains the first declaration's order and
// names while combining its privileges with every option revoke on the same
// object. Column lists are removed because these targets revoke the option
// at the whole table, including all of its columns.
func appendObjectGrantOptionRevokes(visit func(ast.Node) error, grants []schemamodel.Grant, names grantName) error {
	var nodes []*ast.RevokePrivilegeNode
	byObject := make(map[objectGrantOption]*ast.RevokePrivilegeNode)
	for _, grant := range grants {
		if !grant.GrantOptionRevoked {
			continue
		}
		node := FromRevokedGrant(grant).SetGrantOptionFor(true).SetColumns(nil)
		node.ObjectName = names.rename(node.ObjectType, node.ObjectName)
		key := objectGrantOption{role: node.Role, kind: node.ObjectType, name: node.ObjectName, arguments: node.Arguments}
		if held, exists := byObject[key]; exists {
			for _, privilege := range node.Privileges {
				if !slices.Contains(held.Privileges, privilege) {
					held.Privileges = append(held.Privileges, privilege)
				}
			}
			continue
		}
		byObject[key] = node
		nodes = append(nodes, node)
	}
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}
