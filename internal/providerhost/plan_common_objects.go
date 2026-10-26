package providerhost

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/internal/deporder"
	"ptah.run/internal/modelast"
	"ptah.run/internal/tableref"
)

// commonObjectPlan holds the steps of the common objects beyond tables, in
// the parts of the host's sequence each takes. A provider renders every one
// of them, so the plan uses the common AST nodes and the provider decides how
// its engine spells each.
type commonObjectPlan struct {
	// before runs ahead of every table change: revocations, removed
	// memberships, and the views the plan drops or replaces by a drop.
	before []ast.Node
	// prepared runs before the tables: new and changed roles and sequences,
	// which a table or a grant may name.
	prepared []ast.Node
	// after runs once the tables are created and changed: views, memberships,
	// grants and comments.
	after []ast.Node
	// last runs after every table removal: the sequences and roles the plan
	// drops.
	last []ast.Node
}

// planCommonObjects plans the views, materialized views, sequences, roles,
// memberships, grants and comments of diff. The comparison has paired them
// with the read; this orders their steps and refuses what the common nodes
// cannot say.
func (p commonPlanner) planCommonObjects(diff *difftypes.SchemaDiff) (commonObjectPlan, error) {
	var plan commonObjectPlan
	for _, grant := range diff.GrantsRemoved {
		plan.before = append(plan.before, revokeNode(grant))
	}
	for _, grant := range diff.GrantOptionsRevoked {
		plan.before = append(plan.before, revokeNode(grant).SetGrantOptionFor(true))
	}
	for _, membership := range diff.RoleMembershipsRemoved {
		plan.before = append(plan.before, ast.NewRevokeRoleMembership(membership.Role, membership.Member))
	}
	plan.before = append(plan.before, droppedViews(diff)...)
	for _, view := range diff.MaterializedViewsRemoved {
		plan.before = append(plan.before, ast.NewDropMaterializedView(view.Name))
	}
	for _, change := range diff.MaterializedViewsModified {
		plan.before = append(plan.before, ast.NewDropMaterializedView(change.ViewName))
	}
	for _, role := range diff.RolesAdded {
		plan.prepared = append(plan.prepared, modelast.FromRole(role))
	}
	for _, change := range diff.RolesModified {
		node, err := alteredRole(change)
		if err != nil {
			return commonObjectPlan{}, err
		}
		plan.prepared = append(plan.prepared, node)
	}
	for _, sequence := range diff.SequencesAdded {
		plan.prepared = append(plan.prepared, modelast.FromSequence(sequence))
	}
	for _, change := range diff.SequencesModified {
		plan.prepared = append(plan.prepared, alteredSequence(change))
	}
	plan.after = append(plan.after, createdViews(diff)...)
	for _, view := range diff.MaterializedViewsAdded {
		plan.after = append(plan.after, modelast.FromMaterializedView(view))
	}
	for _, change := range diff.MaterializedViewsModified {
		plan.after = append(plan.after, modelast.FromMaterializedView(change.Desired))
	}
	for _, membership := range diff.RoleMembershipsAdded {
		plan.after = append(plan.after, ast.NewGrantRoleMembership(membership.Role, membership.Member))
	}
	for _, grant := range diff.GrantsAdded {
		plan.after = append(plan.after, grantNode(grant, grant.WithOption))
	}
	for _, grant := range diff.GrantOptionsAdded {
		plan.after = append(plan.after, grantNode(grant, true))
	}
	comments, err := commentNodes(diff)
	if err != nil {
		return commonObjectPlan{}, err
	}
	plan.after = append(plan.after, comments...)
	for _, sequence := range diff.SequencesRemoved {
		node := ast.NewDropSequence(sequence.Name)
		node.Schema = sequence.Schema
		plan.last = append(plan.last, node)
	}
	for _, role := range diff.RolesRemoved {
		plan.last = append(plan.last, ast.NewDropRole(role.Name).SetGroup(role.Group))
	}
	bindGrantDatabasePath(plan.before, diff.CurrentDatabasePath)
	bindGrantDatabasePath(plan.after, diff.CurrentDatabasePath)
	return plan, nil
}

// bindGrantDatabasePath supplies the session path to grants whose engine needs
// an absolute target for a root object or the database itself.
func bindGrantDatabasePath(nodes []ast.Node, database string) {
	for _, node := range nodes {
		switch access := node.(type) {
		case *ast.GrantPrivilegeNode:
			access.DatabasePath = database
		case *ast.RevokePrivilegeNode:
			access.DatabasePath = database
		}
	}
}

// droppedViews drops each view the plan removes, and each one whose query
// changed under a name another view reads, dependents first.
func droppedViews(diff *difftypes.SchemaDiff) []ast.Node {
	objects := make([]deporder.ViewLike, 0, len(diff.ViewsRemoved))
	for _, view := range diff.ViewsRemoved {
		objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	ordered := deporder.ViewLikesForCreate(objects)
	slices.Reverse(ordered)
	nodes := make([]ast.Node, 0, len(ordered))
	for _, object := range ordered {
		nodes = append(nodes, ast.NewDropView(object.Name))
	}
	return nodes
}

// createdViews creates each view the plan adds, and replaces each one whose
// query changed, a view after the views it reads. How a replacement runs is
// the provider's: its engine may replace in place or drop and create.
func createdViews(diff *difftypes.SchemaDiff) []ast.Node {
	views := make(map[string]*ast.CreateViewNode, len(diff.ViewsAdded)+len(diff.ViewsModified))
	objects := make([]deporder.ViewLike, 0, len(views))
	add := func(name string, node *ast.CreateViewNode, dependsOn []string) {
		views[name] = node
		objects = append(objects, deporder.ViewLike{Name: name, Body: node.Body, DependsOn: dependsOn})
	}
	for _, view := range diff.ViewsAdded {
		add(view.Name, modelast.FromView(view), view.DependsOn)
	}
	for _, change := range diff.ViewsModified {
		node := modelast.FromView(change.Desired)
		node.SetReplace()
		add(change.Desired.Name, node, change.Desired.DependsOn)
	}
	nodes := make([]ast.Node, 0, len(objects))
	for _, object := range deporder.ViewLikesForCreate(objects) {
		nodes = append(nodes, views[object.Name])
	}
	return nodes
}

// roleAttributes maps the comparison's name of a role attribute to its
// operation.
var roleAttributes = map[string]func(bool) ast.RoleOperation{
	"login":       func(value bool) ast.RoleOperation { return ast.NewSetLoginOperation(value) },
	"superuser":   func(value bool) ast.RoleOperation { return ast.NewSetSuperuserOperation(value) },
	"createdb":    func(value bool) ast.RoleOperation { return ast.NewSetCreateDBOperation(value) },
	"createrole":  func(value bool) ast.RoleOperation { return ast.NewSetCreateRoleOperation(value) },
	"inherit":     func(value bool) ast.RoleOperation { return ast.NewSetInheritOperation(value) },
	"replication": func(value bool) ast.RoleOperation { return ast.NewSetReplicationOperation(value) },
}

// alteredRole changes the attributes the comparison found changed. A
// principal that changes between a role and a group is not an alteration.
func alteredRole(change difftypes.RoleDiff) (*ast.AlterRoleNode, error) {
	node := ast.NewAlterRole(change.RoleName)
	for _, attribute := range slices.Sorted(maps.Keys(change.Changes)) {
		switch set, known := roleAttributes[attribute]; {
		case known:
			node.AddOperation(set(strings.HasSuffix(change.Changes[attribute], "-> true")))
		case attribute == "password":
			node.AddOperation(ast.NewSetPasswordOperation(change.Desired.Password))
		default:
			return nil, unsupported(fmt.Sprintf("changing the %s of role %s", attribute, change.RoleName))
		}
	}
	if change.Desired.Group {
		return nil, unsupported(fmt.Sprintf("changing the attributes of group %s", change.RoleName))
	}
	return node, nil
}

// alteredSequence changes the options the comparison found changed, with
// their declared values.
func alteredSequence(change difftypes.SequenceDiff) *ast.AlterSequenceNode {
	sequence := change.Desired
	node := ast.NewAlterSequence(sequence.Name)
	if sequence.Schema != "" {
		node.SetSchema(sequence.Schema)
	}
	set := func(key string, value *int64, apply func(int64) *ast.AlterSequenceNode) {
		if _, changed := change.Changes[key]; changed && value != nil {
			apply(*value)
		}
	}
	if _, changed := change.Changes["as"]; changed {
		node.SetAs(sequence.AsType)
	}
	set("start", sequence.Start, node.SetStart)
	set("increment", sequence.Increment, node.SetIncrement)
	set("minvalue", sequence.MinValue, node.SetMinValue)
	set("maxvalue", sequence.MaxValue, node.SetMaxValue)
	set("cache", sequence.Cache, node.SetCache)
	if _, changed := change.Changes["cycle"]; changed {
		node.SetCycle(sequence.Cycle)
	}
	if _, changed := change.Changes["owned_by"]; changed {
		node.SetOwnedBy(sequence.OwnedBy)
	}
	return node
}

func grantNode(grant difftypes.GrantRef, withOption bool) *ast.GrantPrivilegeNode {
	node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).SetArguments(grant.Arguments).SetWithOption(withOption)
	if grant.Column != "" {
		node.SetColumns([]string{grant.Column})
	}
	return node
}

func revokeNode(grant difftypes.GrantRef) *ast.RevokePrivilegeNode {
	node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).SetArguments(grant.Arguments)
	if grant.Column != "" {
		node.SetColumns([]string{grant.Column})
	}
	return node
}

// commentNodes sets each comment the comparison found changed: a table's or a
// column's in place, and another object's by its kind. An object the plan
// creates, or creates again, carries its own comment.
func commentNodes(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	created := make(map[difftypes.CommentedObjectKind]map[string]bool)
	mark := func(kind difftypes.CommentedObjectKind, names ...string) {
		if created[kind] == nil {
			created[kind] = make(map[string]bool)
		}
		for _, name := range names {
			created[kind][name] = true
		}
	}
	mark(difftypes.CommentedView, diff.ViewsAdded.Names()...)
	for _, change := range diff.ViewsModified {
		mark(difftypes.CommentedView, change.ViewName, change.Desired.Name)
	}
	mark(difftypes.CommentedMatView, diff.MaterializedViewsAdded.Names()...)
	for _, change := range diff.MaterializedViewsModified {
		mark(difftypes.CommentedMatView, change.ViewName, change.Desired.Name)
	}
	mark(difftypes.CommentedSequence, diff.SequencesAdded.Names()...)
	mark(difftypes.CommentedRole, diff.RolesAdded.Names()...)
	var nodes []ast.Node
	for _, table := range diff.TablesModified {
		if change := table.CommentChange; change != nil {
			nodes = append(nodes, &ast.AlterTableNode{Name: table.TableName, Operations: []ast.AlterOperation{
				&ast.SetCommentOperation{Comment: change.Desired, HasCurrent: change.Current != ""}}})
		}
		for _, column := range table.ColumnsModified {
			if change := column.CommentChange; change != nil {
				nodes = append(nodes, &ast.AlterTableNode{Name: table.TableName, Operations: []ast.AlterOperation{
					&ast.SetCommentOperation{Column: column.ColumnName, Comment: change.Desired, HasCurrent: change.Current != ""}}})
			}
		}
	}
	for _, change := range diff.ObjectCommentsChanged {
		kind, known := commentedObjects[change.Kind]
		if !known {
			return nil, unsupported(fmt.Sprintf("a comment on %s %s", change.Kind, change.Name))
		}
		if created[change.Kind][change.Name] {
			continue
		}
		nodes = append(nodes, ast.NewObjectComment(kind, change.Name, change.Desired))
	}
	return nodes, nil
}

// creationComments takes the comments off a table declaration, since the
// common table message has no place for one, and returns the steps that set
// them once the table exists: the table's, then each column's in order. The
// caller's fields are not changed.
func creationComments(declaration schemacapture.TableDeclaration) (schemacapture.TableDeclaration, []ast.Node) {
	name := tableref.CanonicalExact(declaration.Table.Schema, declaration.Table.Name)
	var steps []ast.Node
	if declaration.Table.Comment != "" {
		steps = append(steps, commentStep(name, "", declaration.Table.Comment))
		declaration.Table.Comment = ""
	}
	declaration.Fields = slices.Clone(declaration.Fields)
	for i := range declaration.Fields {
		if field := &declaration.Fields[i]; field.Comment != "" {
			steps = append(steps, commentStep(name, field.Name, field.Comment))
			field.Comment = ""
		}
	}
	return declaration, steps
}

// commentStep sets the comment of a table, or of its column when column is
// not empty, on a table that has none yet.
func commentStep(table, column, comment string) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.SetCommentOperation{Column: column, Comment: comment}}}
}

// commentedObjects are the kinds of object a comment change names that the
// common-object wire carries.
var commentedObjects = map[difftypes.CommentedObjectKind]ast.CommentedObject{
	difftypes.CommentedView:     ast.CommentedView,
	difftypes.CommentedMatView:  ast.CommentedMaterializedView,
	difftypes.CommentedSequence: ast.CommentedSequence,
	difftypes.CommentedSchema:   ast.CommentedSchema,
	difftypes.CommentedRole:     ast.CommentedRole,
}

// commonObjectMetadata describes a step that changes a common object beyond
// tables, and reports whether node is one: its footprint on the object, and
// the node as its operand. A comment on a table or a column is a step of that
// table, its captured parent.
func commonObjectMetadata(builder objectidentity.Builder, parents map[objectidentity.Key]objectidentity.ID, node ast.Node, selections *scopeSelections) (featureplan.CommonStep, bool, error) {
	if alter, isAlter := node.(*ast.AlterTableNode); isAlter {
		return tableCommentMetadata(builder, parents, alter)
	}
	step := featureplan.CommonStep{CommonObject: node}
	if selections != nil {
		var objectType, objectName string
		switch typed := node.(type) {
		case *ast.GrantPrivilegeNode:
			objectType, objectName = typed.ObjectType, typed.ObjectName
		case *ast.RevokePrivilegeNode:
			objectType, objectName = typed.ObjectType, typed.ObjectName
		}
		grants := grantObjects{selected: selections.selected}
		if grants.OwnsGrantType(objectType) {
			subject, valid := grants.GrantObject(objectType, objectName)
			if !valid {
				return step, true, unsupported("an invalid native grant object identity")
			}
			// Access changes require the object but do not alter its native
			// settings. A separate resource lets its owner order exact captured
			// access checks before grants, or account for revokes in a drop.
			access := subject
			access.Kind = "object_access"
			access.Namespace = objectidentity.NewParts(objectidentity.Part{Source: string(subject.Kind), Normalized: string(subject.Kind)})
			step.Effects = []plangraph.Effect{{Subject: subject, Action: plangraph.Read}, {Subject: access, Action: plangraph.Alter}}
			return step, true, nil
		}
	}
	for _, family := range []func(objectidentity.Builder, ast.Node) (objectidentity.ID, plangraph.Action, bool){relationEffect, principalEffect, accessEffect} {
		subject, action, matched := family(builder, node)
		if !matched {
			continue
		}
		if action != "" {
			step.Effects = []plangraph.Effect{{Subject: subject, Action: action}}
		}
		return step, true, nil
	}
	return featureplan.CommonStep{}, false, nil
}

// tableCommentMetadata describes a comment on a table or one of its columns.
func tableCommentMetadata(builder objectidentity.Builder, parents map[objectidentity.Key]objectidentity.ID, node *ast.AlterTableNode) (featureplan.CommonStep, bool, error) {
	comment, isComment := commentOperation(node)
	if !isComment {
		return featureplan.CommonStep{}, false, nil
	}
	ref, valid := tableref.Parse(node.Name)
	parent, found := parents[builder.TableParts(ref.Schema, ref.Name).Key()]
	if !valid || !found {
		return featureplan.CommonStep{}, true, fmt.Errorf("%w: a comment has no captured table", schemaext.ErrInvalidValue)
	}
	subject := parent
	if comment.Column != "" {
		subject = planningChild(parent, builder.ColumnParts(parent.Schema.Source, parent.Name.Source, comment.Column))
	}
	return featureplan.CommonStep{CommonObject: node, Parent: parent, Effects: []plangraph.Effect{{Subject: subject, Action: plangraph.Alter}}}, true, nil
}

func qualifiedRelation(builder objectidentity.Builder, kind objectidentity.Kind, qualified string) objectidentity.ID {
	schema, name := splitQualified(qualified)
	return builder.SchemaScopedParts(kind, schema, name)
}

// relationEffect is the footprint of a view, materialized view or sequence
// step.
func relationEffect(builder objectidentity.Builder, node ast.Node) (objectidentity.ID, plangraph.Action, bool) {
	switch typed := node.(type) {
	case *ast.CreateViewNode:
		if typed.Replace {
			return qualifiedRelation(builder, objectidentity.KindView, typed.Name), plangraph.Alter, true
		}
		return qualifiedRelation(builder, objectidentity.KindView, typed.Name), plangraph.Create, true
	case *ast.DropViewNode:
		return qualifiedRelation(builder, objectidentity.KindView, typed.Name), plangraph.Drop, true
	case *ast.CreateMaterializedViewNode:
		return qualifiedRelation(builder, objectidentity.KindMatView, typed.Name), plangraph.Create, true
	case *ast.DropMaterializedViewNode:
		return qualifiedRelation(builder, objectidentity.KindMatView, typed.Name), plangraph.Drop, true
	case *ast.CreateSequenceNode:
		return builder.SchemaScopedParts(objectidentity.KindSequence, typed.Schema, typed.Name), plangraph.Create, true
	case *ast.AlterSequenceNode:
		return builder.SchemaScopedParts(objectidentity.KindSequence, typed.Schema, typed.Name), plangraph.Alter, true
	case *ast.DropSequenceNode:
		return builder.SchemaScopedParts(objectidentity.KindSequence, typed.Schema, typed.Name), plangraph.Drop, true
	}
	return objectidentity.ID{}, "", false
}

// principalEffect is the footprint of a role, group or membership step.
func principalEffect(builder objectidentity.Builder, node ast.Node) (objectidentity.ID, plangraph.Action, bool) {
	switch typed := node.(type) {
	case *ast.CreateRoleNode:
		return builder.Role(typed.Name), plangraph.Create, true
	case *ast.AlterRoleNode:
		return builder.Role(typed.Name), plangraph.Alter, true
	case *ast.DropRoleNode:
		return builder.Role(typed.Name), plangraph.Drop, true
	case *ast.GrantRoleMembershipNode:
		return builder.Role(typed.Role), plangraph.Alter, true
	case *ast.RevokeRoleMembershipNode:
		return builder.Role(typed.Role), plangraph.Alter, true
	}
	return objectidentity.ID{}, "", false
}

// accessEffect is the footprint of a grant, revocation or comment step on the
// object it changes. One whose object no common identity names has none, and
// keeps its place in the sequence.
func accessEffect(builder objectidentity.Builder, node ast.Node) (objectidentity.ID, plangraph.Action, bool) {
	var subject objectidentity.ID
	known := false
	switch typed := node.(type) {
	case *ast.GrantPrivilegeNode:
		subject, known = grantedObject(builder, typed.ObjectType, typed.ObjectName)
	case *ast.RevokePrivilegeNode:
		subject, known = grantedObject(builder, typed.ObjectType, typed.ObjectName)
	case *ast.ObjectCommentNode:
		subject, known = commentedObject(builder, typed)
	default:
		return objectidentity.ID{}, "", false
	}
	if !known {
		return objectidentity.ID{}, "", true
	}
	return subject, plangraph.Alter, true
}

// commentOperation is the comment a common ALTER of one operation sets.
func commentOperation(node *ast.AlterTableNode) (*ast.SetCommentOperation, bool) {
	if len(node.Operations) != 1 {
		return nil, false
	}
	comment, isComment := node.Operations[0].(*ast.SetCommentOperation)
	return comment, isComment
}

// grantedObject is the object whose privileges a grant changes, for the
// object types a common identity names.
func grantedObject(builder objectidentity.Builder, objectType, objectName string) (objectidentity.ID, bool) {
	schema, name := splitQualified(objectName)
	switch strings.ToUpper(objectType) {
	case "TABLE":
		return builder.TableParts(schema, name), true
	case "SEQUENCE":
		return builder.SchemaScopedParts(objectidentity.KindSequence, schema, name), true
	case "SCHEMA":
		return builder.SchemaScopedParts(objectidentity.KindSchema, "", objectName), true
	}
	return objectidentity.ID{}, false
}

// commentedObject is the object a comment node names.
func commentedObject(builder objectidentity.Builder, node *ast.ObjectCommentNode) (objectidentity.ID, bool) {
	schema, name := splitQualified(node.Name)
	switch node.Object {
	case ast.CommentedView:
		return builder.SchemaScopedParts(objectidentity.KindView, schema, name), true
	case ast.CommentedMaterializedView:
		return builder.SchemaScopedParts(objectidentity.KindMatView, schema, name), true
	case ast.CommentedSequence:
		return builder.SchemaScopedParts(objectidentity.KindSequence, schema, name), true
	case ast.CommentedRole:
		return builder.Role(node.Name), true
	case ast.CommentedSchema:
		return builder.SchemaScopedParts(objectidentity.KindSchema, "", node.Name), true
	}
	return objectidentity.ID{}, false
}
