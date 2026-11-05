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
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
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
	// memberships, and the views and materialized views the plan drops or
	// replaces by a drop. An entry on a table, a view or a materialized view
	// the plan drops is not revoked; see droppedRelations.
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
	views := viewReplacement{inPlace: p.options.Capabilities.Has(capability.CreateOrReplaceView)}
	semantics := diff.EffectiveIdentifierSemantics(p.target)
	plan.before = droppedRelations(diff, views, semantics).revocations(diff, semantics)
	for _, membership := range diff.RoleMembershipsRemoved {
		plan.before = append(plan.before, ast.NewRevokeRoleMembership(membership.Role, membership.Member))
	}
	plan.before = append(plan.before, views.dropped(diff)...)
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
	plan.after = append(plan.after, views.created(diff)...)
	for _, view := range diff.MaterializedViewsAdded {
		plan.after = append(plan.after, modelast.FromMaterializedView(view))
	}
	for _, change := range diff.MaterializedViewsModified {
		plan.after = append(plan.after, modelast.FromMaterializedView(change.Desired))
	}
	for _, membership := range diff.RoleMembershipsAdded {
		plan.after = append(plan.after, ast.NewGrantRoleMembership(membership.Role, membership.Member))
	}
	for _, grant := range slices.Concat(diff.GrantsAdded, views.regrants(diff, semantics)) {
		plan.after = append(plan.after, grantNode(grant, grant.WithOption))
	}
	for _, grant := range diff.GrantOptionsAdded {
		plan.after = append(plan.after, grantNode(grant, true))
	}
	comments, err := p.changedComments(diff)
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

// viewReplacement is how a target replaces a view whose query changed: in
// place, on a target with [capability.CreateOrReplaceView], and otherwise by
// dropping the view and creating it again.
type viewReplacement struct {
	inPlace bool
}

// dropped drops each view the plan removes, dependents first. On a target
// that cannot replace a view in place it drops each view whose query changed
// too, to create it again (created).
func (r viewReplacement) dropped(diff *difftypes.SchemaDiff) []ast.Node {
	objects := make([]deporder.ViewLike, 0, len(diff.ViewsRemoved)+len(diff.ViewsModified))
	for _, view := range diff.ViewsRemoved {
		objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	if !r.inPlace {
		for _, change := range diff.ViewsModified {
			objects = append(objects, deporder.ViewLike{Name: change.ViewName, Body: change.PreviousBody})
		}
	}
	ordered := deporder.ViewLikesForCreate(objects)
	slices.Reverse(ordered)
	nodes := make([]ast.Node, 0, len(ordered))
	for _, object := range ordered {
		nodes = append(nodes, ast.NewDropView(object.Name))
	}
	return nodes
}

// created creates each view the plan adds and each one whose query changed, a
// view after the views it reads. A target that replaces in place gets a
// replacement, which the provider writes in the statement its engine has for
// it. On any other target dropped dropped the view, and it is created again.
func (r viewReplacement) created(diff *difftypes.SchemaDiff) []ast.Node {
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
		if r.inPlace {
			node.SetReplace()
		}
		add(change.Desired.Name, node, change.Desired.DependsOn)
	}
	nodes := make([]ast.Node, 0, len(objects))
	for _, object := range deporder.ViewLikesForCreate(objects) {
		nodes = append(nodes, views[object.Name])
	}
	return nodes
}

// regrants are the permission entries each view and each materialized view
// the plan drops to create again held, which the plan grants again on the
// object that replaces it. An engine keeps a view's entries in the view's own
// access list, so the drop takes them with the view: measured on YDB 25.1.4.7
// and 26.2.1.14, a view created again holds no entry. A target that replaces
// a view in place keeps them; a materialized view is always dropped and
// created again (planCommonObjects). An entry the plan revokes or grants
// anyway is left out.
func (r viewReplacement) regrants(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []difftypes.GrantRef {
	recreated := make(map[string]bool, len(diff.ViewsModified)+len(diff.MaterializedViewsModified))
	if !r.inPlace {
		for _, change := range diff.ViewsModified {
			recreated[semantics.TableIdentityKey(change.ViewName)] = true
		}
	}
	for _, change := range diff.MaterializedViewsModified {
		recreated[semantics.TableIdentityKey(change.ViewName)] = true
	}
	if len(recreated) == 0 {
		return nil
	}
	planned := make(map[string]bool, len(diff.GrantsRemoved)+len(diff.GrantsAdded))
	for _, grant := range slices.Concat(diff.GrantsRemoved, diff.GrantsAdded) {
		planned[grantIdentity(grant, semantics)] = true
	}
	var again []difftypes.GrantRef
	for _, grant := range diff.CurrentGrants {
		if grant.OnRelation() && recreated[semantics.TableIdentityKey(grant.ObjectName)] && !planned[grantIdentity(grant, semantics)] {
			again = append(again, grant)
		}
	}
	return again
}

// relationDrops are the identity keys of the tables, views and materialized
// views whose drop the plan carries.
type relationDrops map[string]bool

// droppedRelations collects the relationDrops of diff: the tables it removes,
// the views and materialized views it removes, the materialized views it
// changes, which it drops to create again, and the views it drops to create
// again on a target that cannot replace one in place. A drop takes the
// object's access list with it, so a REVOKE of an entry there only changes an
// object the next step removes. Measured on YDB 25.1 and 26.2, a REVOKE ahead
// of the DROP VIEW, or of a table's drop, applies, and the drop alone leaves
// the same state: no entry, and nothing to plan next. A view replaced in place
// keeps its entries, and a REVOKE there is the only step that removes one.
func droppedRelations(diff *difftypes.SchemaDiff, views viewReplacement, semantics identifier.Semantics) relationDrops {
	dropped := make(relationDrops, len(diff.TablesRemoved)+len(diff.ViewsRemoved)+len(diff.ViewsModified)+
		len(diff.MaterializedViewsRemoved)+len(diff.MaterializedViewsModified))
	for _, name := range slices.Concat(diff.TablesRemoved.Names(), diff.ViewsRemoved.Names(), diff.MaterializedViewsRemoved.Names()) {
		dropped[semantics.QualifiedTableIdentityKey(name)] = true
	}
	for _, change := range diff.MaterializedViewsModified {
		dropped[semantics.QualifiedTableIdentityKey(change.ViewName)] = true
	}
	if !views.inPlace {
		for _, change := range diff.ViewsModified {
			dropped[semantics.QualifiedTableIdentityKey(change.ViewName)] = true
		}
	}
	return dropped
}

// holds reports whether grant is an entry on a table or view the plan drops.
func (d relationDrops) holds(grant difftypes.GrantRef, semantics identifier.Semantics) bool {
	return grant.OnRelation() && d[semantics.QualifiedTableIdentityKey(grant.ObjectName)]
}

// revocations revokes each entry the comparison removes and each grant
// option it takes back, other than those on an object the plan drops.
func (d relationDrops) revocations(diff *difftypes.SchemaDiff, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, grant := range diff.GrantsRemoved {
		if !d.holds(grant, semantics) {
			nodes = append(nodes, revokeNode(grant))
		}
	}
	for _, grant := range diff.GrantOptionsRevoked {
		if !d.holds(grant, semantics) {
			nodes = append(nodes, revokeNode(grant).SetGrantOptionFor(true))
		}
	}
	return nodes
}

// grantIdentity is what makes two references one permission entry: the
// role, the privilege, the object and the column it is limited to.
func grantIdentity(grant difftypes.GrantRef, semantics identifier.Semantics) string {
	object := grant.ObjectName
	if grant.OnRelation() {
		object = semantics.TableIdentityKey(object)
	}
	return strings.Join([]string{grant.Role, strings.ToLower(grant.Privilege), strings.ToUpper(grant.ObjectType), object, grant.Arguments, grant.Column}, "\x00")
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

// changedComments are the comment steps of diff: those of commentNodes; for
// a provider that owns the lifecycle of its tables and carries comments on
// the wire, those of removedColumnComments; and for one that also owns the
// indexes of its tables, those of indexCommentNodes.
func (p commonPlanner) changedComments(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	nodes, err := commentNodes(diff)
	if err != nil || !p.comments || !p.ownsColumns() {
		return nodes, err
	}
	nodes = append(nodes, removedColumnComments(diff)...)
	if !p.ownsIndexes() {
		return nodes, nil
	}
	indexes, err := p.indexCommentNodes(diff)
	if err != nil {
		return nil, err
	}
	return append(nodes, indexes...), nil
}

// removedColumnComments removes the comment of each column a table the plan
// keeps drops, which an engine that keeps comments apart from the column, as
// YDB keeps them in its table's attributes, would otherwise keep under the
// column's name, and give to a column added under that name later. The step
// is a comment step like any other, so it runs in the plan's comment window,
// before the removals.
func removedColumnComments(diff *difftypes.SchemaDiff) []ast.Node {
	var nodes []ast.Node
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			if column.Comment != "" {
				nodes = append(nodes, &ast.AlterTableNode{Name: table.TableName, Operations: []ast.AlterOperation{
					&ast.SetCommentOperation{Column: column.Name, HasCurrent: true}}})
			}
		}
	}
	return nodes
}

// indexCommentNodes sets the comment of each index of a table the plan keeps
// or creates whose comment has to change, once the tables and their indexes
// are in place: an index the plan adds takes the comment its declaration
// states, and an index the comparison found with another comment takes the
// declared one. A renamed index's comment moves to its new name, and an index
// the plan drops loses its comment, which an engine that keeps comments apart
// from the index would otherwise keep. One step names each index once, with
// the comment it ends with. An index of a table the plan drops needs none.
func (p commonPlanner) indexCommentNodes(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
	removed := make(map[objectidentity.Key]bool, len(diff.TablesRemoved))
	for _, name := range diff.TablesRemoved.Names() {
		key, err := tableKey(builder, name)
		if err != nil {
			return nil, err
		}
		removed[key] = true
	}
	type indexKey struct {
		table objectidentity.Key
		name  string
	}
	type target struct {
		table, name, comment string
	}
	var order []indexKey
	final := make(map[indexKey]target)
	set := func(key indexKey, value target) {
		if _, seen := final[key]; !seen {
			order = append(order, key)
		}
		final[key] = value
	}
	for _, change := range diff.IndexCommentsChanged {
		table, err := tableKey(builder, change.TableName)
		if err != nil {
			return nil, err
		}
		if removed[table] {
			continue
		}
		if change.From != "" && change.Current != "" {
			set(indexKey{table, change.From}, target{change.TableName, change.From, ""})
		}
		set(indexKey{table, change.Name}, target{change.TableName, change.Name, change.Desired})
	}
	for _, change := range diff.IndexesAdded {
		table, err := tableKey(builder, change.TableName)
		if err != nil {
			return nil, err
		}
		key := indexKey{table, change.Index.Name}
		if _, recorded := final[key]; removed[table] || !recorded && change.Index.Comment == "" {
			continue
		}
		set(key, target{change.TableName, change.Index.Name, change.Index.Comment})
	}
	nodes := make([]ast.Node, 0, len(order))
	for _, key := range order {
		comment := final[key]
		nodes = append(nodes, ast.NewObjectComment(ast.CommentedIndex, comment.name, comment.comment).SetTable(comment.table))
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
	if comment, isComment := node.(*ast.ObjectCommentNode); isComment && comment.Object == ast.CommentedIndex {
		return indexCommentMetadata(builder, parents, comment)
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
	if table, granted := tableGrant(builder, node); granted {
		step.Effects = tableGrantEffects(table)
		if parent, captured := parents[table.Key()]; captured {
			step.Parent = parent
		}
		return step, true, nil
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

// tableGrant is the table a grant or revocation on a table changes the
// privileges of, and reports whether node is one.
func tableGrant(builder objectidentity.Builder, node ast.Node) (objectidentity.ID, bool) {
	var objectType, objectName string
	switch typed := node.(type) {
	case *ast.GrantPrivilegeNode:
		objectType, objectName = typed.ObjectType, typed.ObjectName
	case *ast.RevokePrivilegeNode:
		objectType, objectName = typed.ObjectType, typed.ObjectName
	default:
		return objectidentity.ID{}, false
	}
	if !strings.EqualFold(objectType, "TABLE") {
		return objectidentity.ID{}, false
	}
	return grantedObject(builder, objectType, objectName)
}

// tableGrantEffects is the footprint of a grant or revocation on table: the
// step reads the table and changes its access, a resource of its own. Altering
// the table itself would make the step a second writer of a table the plan
// creates or alters through the owner of the table lifecycle, and the plan
// graph refuses two writers of one subject. The resource is not a native
// object's object_access, which a provider reads as a grant on an object it
// selected. A grant on a schema or a sequence keeps its alteration of the
// object, the footprint a provider reads it by.
func tableGrantEffects(table objectidentity.ID) []plangraph.Effect {
	access := table
	access.Kind = "access"
	access.Namespace = objectidentity.NewParts(objectidentity.Part{Source: string(table.Kind), Normalized: string(table.Kind)})
	return []plangraph.Effect{{Subject: table, Action: plangraph.Read}, {Subject: access, Action: plangraph.Alter}}
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
	return commentStepMetadata(node, parent, subject), true, nil
}

// commentStepMetadata is the footprint of a comment on object, a table, a
// column or an index of the table parent: the step reads the table and
// changes the object's comment. The comment is a resource of its own, apart
// from the object: an owner of the table lifecycle creates the table, a
// column or an index, and a step of another owner that wrote the same object
// would be a second writer of it. The comment of an index the plan drops is
// cleared after the index is gone, which an effect on the index could not
// follow.
func commentStepMetadata(node ast.Node, parent, object objectidentity.ID) featureplan.CommonStep {
	comment := object
	comment.Kind = "comment"
	comment.Namespace = objectidentity.NewParts(objectidentity.Part{Source: string(object.Kind), Normalized: string(object.Kind)})
	return featureplan.CommonStep{CommonObject: node, Parent: parent,
		Effects: []plangraph.Effect{{Subject: parent, Action: plangraph.Read}, {Subject: comment, Action: plangraph.Alter}}}
}

// indexCommentMetadata describes a comment on an index, a step of its table;
// see commentStepMetadata.
func indexCommentMetadata(builder objectidentity.Builder, parents map[objectidentity.Key]objectidentity.ID, node *ast.ObjectCommentNode) (featureplan.CommonStep, bool, error) {
	ref, valid := tableref.Parse(node.Table)
	parent, found := parents[builder.TableParts(ref.Schema, ref.Name).Key()]
	if !valid || !found {
		return featureplan.CommonStep{}, true, fmt.Errorf("%w: an index comment has no captured table", schemaext.ErrInvalidValue)
	}
	index := planningChild(parent, builder.IndexParts(parent.Schema.Source, parent.Name.Source, node.Name))
	return commentStepMetadata(node, parent, index), true, nil
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
// object it changes, other than a grant on a table (tableGrantEffects). One
// whose object no common identity names has none, and keeps its place in the
// sequence.
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
