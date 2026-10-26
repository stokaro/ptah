// Package atlaschange names the change a planned statement makes, in the words
// the Atlas CLI writes above each statement of a migration file:
// `create "users" table`, `modify "users" table`, `drop index "i" from table:
// "t"`.
//
// The community binary's PostgreSQL planner is the source of the wording for
// tables, columns, indexes, enum types, schemas and comments, and it is used on
// every engine; the binary words a few changes differently on SQLite and
// MySQL (stokaro/ptah#4960). The objects it does not plan, such as views,
// routines, triggers, sequences and roles, take the same pattern:
// `create "v" view`, `modify "v" view` for an in-place replace, and
// `drop "v" view`.
//
// A rollback file writes each statement under `reverse: ` and the change it
// undoes, which is the forward change's comment. A rollback here is planned on
// its own rather than by inverting the forward plan, so [Undone] reads that
// comment off the rollback statement itself: a DROP TABLE undoes
// `create "users" table`. A rollback may rebuild an object the forward plan
// replaced in place, such as a view the forward plan changed with CREATE OR
// REPLACE VIEW and the rollback drops and creates; [Rollback] names both of its
// statements after the forward `modify "v" view`.
package atlaschange

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/tableref"
)

// Of returns the comment naming the change node makes. It reports false for a
// node that makes no change Atlas would name, such as a comment of the plan's
// own, raw SQL, or an owner operation.
func Of(node ast.Node) (string, bool) {
	c, ok := describe(node)
	if !ok {
		return "", false
	}
	return c.String(), true
}

// Undone returns the comment of the forward change that node, a statement of a
// rollback, undoes. It reports false where [Of] does.
func Undone(node ast.Node) (string, bool) {
	c, ok := describe(node)
	if !ok {
		return "", false
	}
	return c.inverse().String(), true
}

// Rollback returns the namer for the statements of a rollback planned against
// forward: [Undone], except that a statement that drops or creates an object
// forward only modifies undoes that modification.
//
// Objects compare by kind and name without case, and by the schema, or for an
// object named within its table, by the table, where both statements name
// one, so a view of one schema is not taken for a view of the same name in
// another.
func Rollback(forward []ast.Node) func(ast.Node) (string, bool) {
	type object struct{ kind, name string }
	type seen struct {
		change change
		scope  scope
	}
	modified := make(map[object][]seen)
	rebuilt := make(map[object][]scope)
	for _, node := range forward {
		c, ok := describe(node)
		if !ok {
			continue
		}
		key := object{c.kind, strings.ToLower(c.name)}
		switch c.verb {
		case modify:
			modified[key] = append(modified[key], seen{c, scopeOf(node)})
		case create, drop:
			rebuilt[key] = append(rebuilt[key], scopeOf(node))
		}
	}
	return func(node ast.Node) (string, bool) {
		c, ok := describe(node)
		if !ok {
			return "", false
		}
		if c.verb != create && c.verb != drop {
			return c.inverse().String(), true
		}
		key := object{c.kind, strings.ToLower(c.name)}
		at := scopeOf(node)
		if slices.ContainsFunc(rebuilt[key], at.overlaps) {
			return c.inverse().String(), true
		}
		for _, forward := range modified[key] {
			if forward.scope.overlaps(at) {
				return forward.change.String(), true
			}
		}
		return c.inverse().String(), true
	}
}

// scope is what qualifies the name of an object a statement changes: the
// schema of an object named within a schema, and the table, with its schema,
// of one named within its table. An empty part is one the statement does not
// name.
type scope struct{ schema, table string }

// overlaps reports whether s and other may name the same place: every part
// both name is equal.
func (s scope) overlaps(other scope) bool {
	return (s.schema == "" || other.schema == "" || s.schema == other.schema) &&
		(s.table == "" || other.table == "" || s.table == other.table)
}

// scopeOf is the scope of the object node changes.
func scopeOf(node ast.Node) scope {
	switch n := node.(type) {
	case *ast.CreateViewNode:
		return schemaScope(n.Name)
	case *ast.DropViewNode:
		return schemaScope(n.Name)
	case *ast.CreateMaterializedViewNode:
		return schemaScope(n.Name)
	case *ast.DropMaterializedViewNode:
		return schemaScope(n.Name)
	case *ast.CreateFunctionNode:
		return schemaScope(n.Name)
	case *ast.DropFunctionNode:
		return schemaScope(n.Name)
	case *ast.CreateTableNode:
		return schemaScope(n.Name)
	case *ast.DropTableNode:
		if names := n.TableNames(); len(names) > 0 {
			return schemaScope(names[0])
		}
	case *ast.AlterTableNode:
		return schemaScope(n.Name)
	case *ast.CreateTriggerNode:
		return tableScope(n.Table)
	case *ast.DropTriggerNode:
		return tableScope(n.Table)
	case *ast.CreatePolicyNode:
		return tableScope(n.Table)
	case *ast.DropPolicyNode:
		return tableScope(n.Table)
	}
	return scope{}
}

// schemaScope is the scope of an object named name within a schema.
func schemaScope(name string) scope {
	if ref, ok := tableref.Parse(name); ok && ref.Qualified {
		return scope{schema: strings.ToLower(ref.Schema)}
	}
	return scope{}
}

// tableScope is the scope of an object named within table.
func tableScope(table string) scope {
	ref, ok := tableref.Parse(table)
	if !ok {
		return scope{}
	}
	at := scope{table: strings.ToLower(ref.Name)}
	if ref.Qualified {
		at.schema = strings.ToLower(ref.Schema)
	}
	return at
}

type verb int

const (
	create verb = iota
	modify
	drop
	rename
	comment
	addValue
	refresh
	grant
	revoke
)

// object kinds whose wording differs from the default pattern.
const (
	kindTable  = "table"
	kindColumn = "column"
	kindIndex  = "index"
	kindEnum   = "enum type"
	kindSchema = "schema"
)

// change is one named change: what is done, to which kind of object, and the
// names the wording needs.
type change struct {
	verb verb
	kind string
	name string
	// table is the table an index or a commented column belongs to.
	table string
	// from and to are the two names of a rename; to is also the grantee of a
	// grant and the member of a membership.
	from, to string
	// others are the other tables one DROP TABLE names, which the wording
	// names after name, one change each.
	others []string
}

func (c change) inverse() change {
	switch c.verb {
	case create:
		c.verb = drop
	case drop:
		c.verb = create
	case rename:
		c.from, c.to = c.to, c.from
	case grant:
		c.verb = revoke
	case revoke:
		c.verb = grant
	}
	return c
}

func (c change) String() string {
	if len(c.others) > 0 {
		names := append([]string{c.name}, c.others...)
		parts := make([]string, 0, len(names))
		for _, name := range names {
			one := c
			one.name, one.others = name, nil
			parts = append(parts, one.String())
		}
		return strings.Join(parts, ", ")
	}
	switch c.kind {
	case kindTable:
		return c.tableWording()
	case kindColumn:
		if c.verb == rename {
			return fmt.Sprintf("rename a column from %q to %q", c.from, c.to)
		}
		return fmt.Sprintf("set comment to column: %q on table: %q", c.name, c.table)
	case kindIndex:
		return c.indexWording()
	case kindEnum:
		return c.enumWording()
	case kindSchema:
		return c.schemaWording()
	}
	switch c.verb {
	case create:
		return fmt.Sprintf("create %q %s", c.name, c.kind)
	case drop:
		return fmt.Sprintf("drop %q %s", c.name, c.kind)
	case rename:
		return fmt.Sprintf("rename a %s from %q to %q", c.kind, c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to %s: %q", c.kind, c.name)
	case refresh:
		return fmt.Sprintf("refresh %q %s", c.name, c.kind)
	case grant:
		return fmt.Sprintf("grant %s to %q", c.name, c.to)
	case revoke:
		return fmt.Sprintf("revoke %s from %q", c.name, c.to)
	default:
		return fmt.Sprintf("modify %q %s", c.name, c.kind)
	}
}

func (c change) tableWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("create %q table", c.name)
	case drop:
		return fmt.Sprintf("drop %q table", c.name)
	case rename:
		return fmt.Sprintf("rename a table from %q to %q", c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to table: %q", c.name)
	default:
		return fmt.Sprintf("modify %q table", c.name)
	}
}

func (c change) indexWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("create index %q to table: %q", c.name, c.table)
	case drop:
		return fmt.Sprintf("drop index %q from table: %q", c.name, c.table)
	case rename:
		return fmt.Sprintf("rename an index from %q to %q", c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to index: %q on table: %q", c.name, c.table)
	default:
		return fmt.Sprintf("modify index %q on table: %q", c.name, c.table)
	}
}

func (c change) enumWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("create enum type %q", c.name)
	case drop:
		return fmt.Sprintf("drop enum type %q", c.name)
	case rename:
		return fmt.Sprintf("rename an enum from %q to %q", c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to enum type: %q", c.name)
	case addValue:
		return fmt.Sprintf("add value to enum type: %q", c.name)
	default:
		return fmt.Sprintf("modify enum type %q", c.name)
	}
}

func (c change) schemaWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("Add new schema named %q", c.name)
	case drop:
		return fmt.Sprintf("Drop schema named %q", c.name)
	default:
		return fmt.Sprintf("set comment to schema: %q", c.name)
	}
}

func describe(node ast.Node) (change, bool) {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return change{verb: create, kind: kindTable, name: bare(n.Name)}, true
	case *ast.DropTableNode:
		names := n.TableNames()
		if len(names) == 0 {
			return change{}, false
		}
		others := make([]string, 0, len(names)-1)
		for _, name := range names[1:] {
			others = append(others, bare(name))
		}
		return change{verb: drop, kind: kindTable, name: bare(names[0]), others: others}, true
	case *ast.AlterTableNode:
		return describeAlterTable(n), true
	case *ast.AlterTableEnableRLSNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.AlterTableDisableRLSNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.AlterTableForceRLSNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.IndexNode:
		return change{verb: create, kind: kindIndex, name: bare(n.Name), table: bare(n.Table)}, true
	case *ast.DropIndexNode:
		return change{verb: drop, kind: kindIndex, name: bare(n.Name), table: bare(n.Table)}, true
	case *ast.AlterIndexNode:
		if n.NewName == "" {
			return change{}, false
		}
		return change{verb: rename, kind: kindIndex, from: bare(n.Name), to: bare(n.NewName)}, true
	case *ast.EnumNode:
		return change{verb: create, kind: kindEnum, name: bare(n.Name)}, true
	case *ast.DropTypeNode:
		return change{verb: drop, kind: kindEnum, name: bare(n.Name)}, true
	case *ast.AlterTypeNode:
		return describeAlterType(n), true
	case *ast.CreateTypeNode:
		return change{verb: create, kind: "type", name: bare(n.Name)}, true
	case *ast.CreateSchemaNode:
		return change{verb: create, kind: kindSchema, name: n.Name}, true
	default:
		return describeObject(node)
	}
}

// describeObject names the changes to the objects the community binary does
// not plan.
func describeObject(node ast.Node) (change, bool) {
	if c, ok := describeRelative(node); ok {
		return c, true
	}
	switch n := node.(type) {
	case *ast.CreateFunctionNode:
		return change{verb: createdVerb[n.Replace], kind: routineKinds[n.IsProcedure()], name: bare(n.Name)}, true
	case *ast.DropFunctionNode:
		return change{verb: drop, kind: routineKinds[n.IsProcedure()], name: bare(n.Name)}, true
	case *ast.CreateSequenceNode:
		return change{verb: create, kind: "sequence", name: n.Name}, true
	case *ast.AlterSequenceNode:
		return change{verb: modify, kind: "sequence", name: n.Name}, true
	case *ast.AlterSerialSequenceNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.DropSequenceNode:
		return change{verb: drop, kind: "sequence", name: n.Name}, true
	case *ast.CreateRoleNode:
		return change{verb: create, kind: "role", name: n.Name}, true
	case *ast.AlterRoleNode:
		return change{verb: modify, kind: "role", name: n.Name}, true
	case *ast.DropRoleNode:
		return change{verb: drop, kind: "role", name: n.Name}, true
	default:
		return describeAccess(node)
	}
}

// describeRelative names the changes to the views and to the objects that
// belong to a table or a view.
func describeRelative(node ast.Node) (change, bool) {
	switch n := node.(type) {
	case *ast.CreateViewNode:
		return change{verb: createdVerb[n.Replace], kind: "view", name: bare(n.Name)}, true
	case *ast.DropViewNode:
		return change{verb: drop, kind: "view", name: bare(n.Name)}, true
	case *ast.CreateMaterializedViewNode:
		return change{verb: create, kind: "materialized view", name: bare(n.Name)}, true
	case *ast.DropMaterializedViewNode:
		return change{verb: drop, kind: "materialized view", name: bare(n.Name)}, true
	case *ast.RefreshMaterializedViewNode:
		return change{verb: refresh, kind: "materialized view", name: bare(n.Name)}, true
	case *ast.CreateTriggerNode:
		return change{verb: createdVerb[n.Replace], kind: "trigger", name: n.Name}, true
	case *ast.DropTriggerNode:
		return change{verb: drop, kind: "trigger", name: n.Name}, true
	case *ast.CreatePolicyNode:
		return change{verb: createdVerb[n.Replace], kind: "policy", name: n.Name}, true
	case *ast.DropPolicyNode:
		return change{verb: drop, kind: "policy", name: n.Name}, true
	default:
		return change{}, false
	}
}

// describeAccess names grants, revocations and comments.
func describeAccess(node ast.Node) (change, bool) {
	switch n := node.(type) {
	case *ast.GrantPrivilegeNode:
		return change{verb: grant, name: privileges(n.Privileges, n.ObjectName), to: n.Role}, true
	case *ast.RevokePrivilegeNode:
		return change{verb: revoke, name: privileges(n.Privileges, n.ObjectName), to: n.Role}, true
	case *ast.GrantRoleMembershipNode:
		return change{verb: grant, name: fmt.Sprintf("role %q", n.Role), to: n.Member}, true
	case *ast.RevokeRoleMembershipNode:
		return change{verb: revoke, name: fmt.Sprintf("role %q", n.Role), to: n.Member}, true
	case *ast.ObjectCommentNode:
		return describeObjectComment(n), true
	default:
		return change{}, false
	}
}

// createdVerb is the verb of a CREATE: a modification when the statement
// replaces the object in place.
var createdVerb = map[bool]verb{false: create, true: modify}

// routineKinds names a routine by whether it is a procedure.
var routineKinds = map[bool]string{false: "function", true: "procedure"}

// describeAlterTable names an ALTER TABLE: a rename or a comment alone is
// named as such, and anything else modifies the table.
func describeAlterTable(n *ast.AlterTableNode) change {
	table := bare(n.Name)
	if len(n.Operations) == 1 {
		switch op := n.Operations[0].(type) {
		case *ast.RenameColumnOperation:
			return change{verb: rename, kind: kindColumn, from: op.OldName, to: op.NewName, table: table}
		case *ast.RenameTableOperation:
			return change{verb: rename, kind: kindTable, from: table, to: bare(op.NewName)}
		case *ast.RenameIndexOperation:
			return change{verb: rename, kind: kindIndex, from: bare(op.From), to: bare(op.To)}
		case *ast.SetCommentOperation:
			if op.Column != "" {
				return change{verb: comment, kind: kindColumn, name: op.Column, table: table}
			}
			return change{verb: comment, kind: kindTable, name: table}
		}
	}
	return change{verb: modify, kind: kindTable, name: table}
}

func describeAlterType(n *ast.AlterTypeNode) change {
	name := bare(n.Name)
	if len(n.Operations) == 1 {
		switch op := n.Operations[0].(type) {
		case *ast.AddEnumValueOperation:
			return change{verb: addValue, kind: kindEnum, name: name}
		case *ast.RenameTypeOperation:
			return change{verb: rename, kind: kindEnum, from: name, to: bare(op.NewName)}
		}
	}
	return change{verb: modify, kind: kindEnum, name: name}
}

func describeObjectComment(n *ast.ObjectCommentNode) change {
	switch n.Object {
	case ast.CommentedIndex:
		return change{verb: comment, kind: kindIndex, name: bare(n.Name), table: bare(n.Table)}
	case ast.CommentedSchema:
		return change{verb: comment, kind: kindSchema, name: n.Name}
	case ast.CommentedType:
		return change{verb: comment, kind: "type", name: bare(n.Name)}
	}
	return change{verb: comment, kind: strings.ToLower(string(n.Object)), name: bare(n.Name)}
}

// privileges names what a grant gives: its privileges, and the object they are
// on where it names one.
func privileges(names []string, object string) string {
	list := strings.Join(names, ", ")
	if object == "" {
		return list
	}
	return fmt.Sprintf("%s on %q", list, bare(object))
}

// bare is a name without its schema, which is how the community binary names
// an object in a comment.
func bare(name string) string {
	if ref, ok := tableref.Parse(name); ok {
		return ref.Name
	}
	return name
}
