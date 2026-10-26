package providerhost

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ast"
	"ptah.run/internal/tableref"
)

// commonObjectChange maps a planned node of a common object beyond tables
// onto the common-object wire, and reports whether node is one. The provider
// renders the change; the host sends the typed operand and never SQL. A table
// comment is a change of the table, and a column comment one of the column.
func (t *target) commonObjectChange(node ast.Node) (*protocol.Change, bool, error) {
	operation, object, err := t.commonOperation(node)
	if operation == nil || err != nil {
		return nil, operation != nil || err != nil, err
	}
	if len(t.commonKinds()) == 0 {
		return nil, true, unsupported(fmt.Sprintf("%T on the %s engine, whose provider does not carry common objects", node, t.engine.GetId()))
	}
	return &protocol.Change{Object: object, Operation: &protocol.Change_CommonObject{CommonObject: operation}}, true, nil
}

// commonOperation is the typed operand of node and the object it changes, or
// nil for a node that changes no common object beyond tables.
func (t *target) commonOperation(node ast.Node) (*protocol.CommonObjectChange, *protocol.Reference, error) {
	for _, family := range []func(ast.Node) (*protocol.CommonObjectChange, *protocol.Reference, error){
		t.relationOperation, t.principalOperation, t.accessOperation,
	} {
		if operation, object, err := family(node); operation != nil || err != nil {
			return operation, object, err
		}
	}
	return nil, nil, nil
}

func (t *target) relation(kind, qualified string) *protocol.Reference {
	schema, name := splitQualified(qualified)
	return &protocol.Reference{Engine: t.engine.GetId(), Kind: kind, Schema: schema, Name: name}
}

// relationOperation handles the views, materialized views and sequences.
func (t *target) relationOperation(node ast.Node) (*protocol.CommonObjectChange, *protocol.Reference, error) {
	engine := t.engine.GetId()
	switch typed := node.(type) {
	case *ast.CreateViewNode:
		object := t.relation("view", typed.Name)
		view := &protocol.SQLView{Schema: object.GetSchema(), Name: object.GetName(), Body: typed.Body, WithCheck: typed.WithCheck, Comment: typed.Comment,
			Attributes: slices.Clone(typed.Attributes)}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateView{CreateView: &protocol.ViewCreation{View: view, Replace: typed.Replace}}}, object, nil
	case *ast.CreateMaterializedViewNode:
		if !typed.Facets.IsZero() {
			return nil, nil, unsupported(fmt.Sprintf("settings of materialized view %q", typed.Name))
		}
		object := t.relation("materialized_view", typed.Name)
		view := &protocol.SQLView{Schema: object.GetSchema(), Name: object.GetName(), Body: typed.Body, Materialized: true, Comment: typed.Comment}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateView{CreateView: &protocol.ViewCreation{View: view}}}, object, nil
	case *ast.DropViewNode:
		object := t.relation("view", typed.Name)
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropView{DropView: object}}, object, nil
	case *ast.DropMaterializedViewNode:
		object := t.relation("materialized_view", typed.Name)
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropView{DropView: object}}, object, nil
	case *ast.CreateSequenceNode:
		object := &protocol.Reference{Engine: engine, Kind: "sequence", Schema: typed.Schema, Name: typed.Name}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateSequence{CreateSequence: createdSequence(typed)}}, object, nil
	case *ast.AlterSequenceNode:
		object := &protocol.Reference{Engine: engine, Kind: "sequence", Schema: typed.Schema, Name: typed.Name}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_AlterSequence{AlterSequence: sequenceAlteration(object, typed)}}, object, nil
	case *ast.DropSequenceNode:
		object := &protocol.Reference{Engine: engine, Kind: "sequence", Schema: typed.Schema, Name: typed.Name}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropSequence{DropSequence: object}}, object, nil
	}
	return nil, nil, nil
}

// principalOperation handles the roles, the groups and their memberships.
func (t *target) principalOperation(node ast.Node) (*protocol.CommonObjectChange, *protocol.Reference, error) {
	engine := t.engine.GetId()
	switch typed := node.(type) {
	case *ast.CreateRoleNode:
		role := &protocol.SQLRole{Name: typed.Name, Login: typed.Login, Superuser: typed.Superuser, CreateDb: typed.CreateDB, CreateRole: typed.CreateRole,
			Inherit: typed.Inherit, Replication: typed.Replication, Comment: typed.Comment, Group: typed.Group, PasswordDeclared: typed.Password != ""}
		object := &protocol.Reference{Engine: engine, Kind: principalKinds[typed.Group], Name: typed.Name}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateRole{CreateRole: &protocol.RoleCreation{Role: role, Password: typed.Password}}}, object, nil
	case *ast.AlterRoleNode:
		alteration, err := roleAlteration(typed)
		if err != nil {
			return nil, nil, err
		}
		object := &protocol.Reference{Engine: engine, Kind: "role", Name: typed.Name}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_AlterRole{AlterRole: alteration}}, object, nil
	case *ast.DropRoleNode:
		object := &protocol.Reference{Engine: engine, Kind: principalKinds[typed.Group], Name: typed.Name}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropRole{DropRole: object}}, object, nil
	case *ast.GrantRoleMembershipNode:
		object := &protocol.Reference{Engine: engine, Kind: "group", Name: typed.Role}
		membership := &protocol.RoleMembership{Group: typed.Role, Member: typed.Member}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_AddMember{AddMember: membership}}, object, nil
	case *ast.RevokeRoleMembershipNode:
		object := &protocol.Reference{Engine: engine, Kind: "group", Name: typed.Role}
		membership := &protocol.RoleMembership{Group: typed.Role, Member: typed.Member}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_RemoveMember{RemoveMember: membership}}, object, nil
	}
	return nil, nil, nil
}

// accessOperation handles the grants, the revocations and the comments; a
// table comment is a change of the table, and a column comment one of the
// column.
func (t *target) accessOperation(node ast.Node) (*protocol.CommonObjectChange, *protocol.Reference, error) {
	engine := t.engine.GetId()
	switch typed := node.(type) {
	case *ast.GrantPrivilegeNode:
		grant, err := t.wireGrant(typed.Role, typed.Privileges, typed.ObjectType, typed.ObjectName, typed.Arguments, typed.Columns)
		if err != nil {
			return nil, nil, err
		}
		grant.Target.Database = typed.DatabasePath
		grant.WithGrantOption, grant.Comment = typed.WithOption, typed.Comment
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_Grant{Grant: grant}}, grant.GetTarget(), nil
	case *ast.RevokePrivilegeNode:
		grant, err := t.wireGrant(typed.Role, typed.Privileges, typed.ObjectType, typed.ObjectName, typed.Arguments, typed.Columns)
		if err != nil {
			return nil, nil, err
		}
		grant.Target.Database = typed.DatabasePath
		grant.WithGrantOption, grant.Comment = typed.GrantOptionFor, typed.Comment
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_Revoke{Revoke: grant}}, grant.GetTarget(), nil
	case *ast.ObjectCommentNode:
		object, err := t.commentedReference(typed)
		if err != nil {
			return nil, nil, err
		}
		return setComment(object, typed.Comment), object, nil
	case *ast.AlterTableNode:
		comment, isComment := commentOperation(typed)
		if !isComment {
			return nil, nil, nil
		}
		table, valid := tableref.Parse(typed.Name)
		if !valid {
			return nil, nil, fmt.Errorf("provider table name is not a structural reference")
		}
		object := &protocol.Reference{Engine: engine, Kind: "table", Schema: table.Schema, Name: table.Name}
		if comment.Column != "" {
			object = &protocol.Reference{Engine: engine, Kind: "column", Schema: table.Schema, Path: []string{table.Name}, Name: comment.Column}
		}
		return setComment(object, comment.Comment), object, nil
	}
	return nil, nil, nil
}

func setComment(object *protocol.Reference, text string) *protocol.CommonObjectChange {
	return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: object, Text: text}}}
}

// principalKinds is the reference kind of a role or, for true, a group.
var principalKinds = map[bool]string{false: "role", true: "group"}

func createdSequence(node *ast.CreateSequenceNode) *protocol.SQLSequence {
	return &protocol.SQLSequence{Schema: node.Schema, Name: node.Name, AsType: node.AsType, Start: copyPlanningPointer(node.Start), Increment: copyPlanningPointer(node.Increment),
		MinValue: copyPlanningPointer(node.MinValue), MaxValue: copyPlanningPointer(node.MaxValue), Cache: copyPlanningPointer(node.Cache), Cycle: node.Cycle,
		OwnedBy: node.OwnedBy, Comment: node.Comment}
}

// sequenceAlteration names the options the node sets, with their values; an
// option it leaves unset is unchanged.
func sequenceAlteration(object *protocol.Reference, node *ast.AlterSequenceNode) *protocol.SequenceAlteration {
	desired := &protocol.SQLSequence{Schema: node.Schema, Name: node.Name, AsType: node.AsType, Start: copyPlanningPointer(node.Start),
		Increment: copyPlanningPointer(node.Increment), MinValue: copyPlanningPointer(node.MinValue), MaxValue: copyPlanningPointer(node.MaxValue),
		Cache: copyPlanningPointer(node.Cache), OwnedBy: node.OwnedBy}
	var changed []string
	for _, option := range []struct {
		name string
		set  bool
	}{
		{"as_type", node.AsType != ""}, {"start", node.Start != nil}, {"increment", node.Increment != nil}, {"min_value", node.MinValue != nil},
		{"max_value", node.MaxValue != nil}, {"cache", node.Cache != nil}, {"cycle", node.Cycle != nil}, {"owned_by", node.OwnedBy != ""},
	} {
		if option.set {
			changed = append(changed, option.name)
		}
	}
	if node.Cycle != nil {
		desired.Cycle = *node.Cycle
	}
	return &protocol.SequenceAlteration{Sequence: object, Desired: desired, Changed: changed}
}

// roleAlteration names the attributes the node sets. The desired role holds
// their values; an attribute it does not name is unchanged.
func roleAlteration(node *ast.AlterRoleNode) (*protocol.RoleAlteration, error) {
	alteration := &protocol.RoleAlteration{Desired: &protocol.SQLRole{Name: node.Name}}
	desired := alteration.Desired
	for _, operation := range node.Operations {
		switch typed := operation.(type) {
		case *ast.SetLoginOperation:
			desired.Login, alteration.Changed = typed.Login, append(alteration.Changed, "login")
		case *ast.SetPasswordOperation:
			alteration.Password, alteration.Changed = typed.Password, append(alteration.Changed, "password")
			desired.PasswordDeclared = typed.Password != ""
		case *ast.SetSuperuserOperation:
			desired.Superuser, alteration.Changed = typed.Superuser, append(alteration.Changed, "superuser")
		case *ast.SetCreateDBOperation:
			desired.CreateDb, alteration.Changed = typed.CreateDB, append(alteration.Changed, "create_db")
		case *ast.SetCreateRoleOperation:
			desired.CreateRole, alteration.Changed = typed.CreateRole, append(alteration.Changed, "create_role")
		case *ast.SetInheritOperation:
			desired.Inherit, alteration.Changed = typed.Inherit, append(alteration.Changed, "inherit")
		case *ast.SetReplicationOperation:
			desired.Replication, alteration.Changed = typed.Replication, append(alteration.Changed, "replication")
		default:
			return nil, unsupported(fmt.Sprintf("role operation %T", operation))
		}
	}
	return alteration, nil
}

// wireGrant names a grant's target by the kind of object it is, as
// declaredGrant does for a declaration.
func (t *target) wireGrant(role string, privileges []string, objectType, objectName, arguments string, columns []string) (*protocol.SQLGrant, error) {
	target, err := t.grantTarget(objectType, objectName)
	if err != nil {
		return nil, err
	}
	if arguments != "" {
		target.Overload = strings.Split(arguments, ", ")
	}
	return &protocol.SQLGrant{Role: role, Privileges: slices.Clone(privileges), Target: target, Columns: slices.Clone(columns)}, nil
}

// grantTarget uses the provider's declared naming for native objects. A dot
// in a database-wide name is part of that name, not a schema separator.
func (t *target) grantTarget(objectType, objectName string) (*protocol.Reference, error) {
	target := &protocol.Reference{Engine: t.engine.GetId(), Kind: strings.ToLower(grantObjectType(objectType))}
	switch target.Kind {
	case "database":
	case "schema":
		target.Name = objectName
	default:
		if t.host != nil && t.host.selections != nil {
			grants := grantObjects{selected: t.host.selections.selected}
			if grants.OwnsGrantType(objectType) {
				identity, valid := grants.GrantObject(objectType, objectName)
				if !valid {
					return nil, unsupported("an invalid provider grant object name")
				}
				target.Schema, target.Name = identity.Schema.Source, identity.Name.Source
				return target, nil
			}
		}
		target.Schema, target.Name = splitQualified(objectName)
	}
	return target, nil
}

// commentedReference is the object a comment node names.
func (t *target) commentedReference(node *ast.ObjectCommentNode) (*protocol.Reference, error) {
	kinds := map[ast.CommentedObject]string{ast.CommentedView: "view", ast.CommentedMaterializedView: "materialized_view",
		ast.CommentedSequence: "sequence", ast.CommentedRole: "role", ast.CommentedSchema: "schema"}
	kind, known := kinds[node.Object]
	if !known {
		return nil, unsupported(fmt.Sprintf("a comment on %s %s", strings.ToLower(string(node.Object)), node.Name))
	}
	object := &protocol.Reference{Engine: t.engine.GetId(), Kind: kind, Name: node.Name}
	if kind != "role" && kind != "schema" {
		object.Schema, object.Name = splitQualified(node.Name)
	}
	return object, nil
}
