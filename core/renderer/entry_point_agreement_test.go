package renderer_test

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/internal/astrouteguard"
)

// TestEntryPoints_AnswerEveryNodeAlike holds the three ways into a renderer to
// one answer: VisitNode followed by Output, Render, and RenderSQL. A node
// either renders the same text through all three or is refused by all three
// with the same sentinels and the same message.
//
// The entry points share the preparation that carries every refusal the
// renderer core owns. When each kept a switch of its own, a kind added to one
// was missed by the other: a CREATE GROUP built by hand was refused by Render
// on a target without groups and rendered by VisitNode as a plain role, its
// group flag dropped.
//
// Every node kind is driven, derived from core/ast, as its zero value and as a
// nil of its own type; the rows after them are the values a zero value cannot
// describe.
func TestEntryPoints_AnswerEveryNodeAlike(t *testing.T) {
	cases := entryPointCases(qt.New(t))

	for _, dialect := range renderedDialects() {
		for _, name := range slices.Sorted(maps.Keys(cases)) {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				node := cases[name]

				visited := visitAnswer(c, dialect, node)

				c.Assert(renderAnswer(c, dialect, node), qt.DeepEquals, visited)
				c.Assert(renderSQLAnswer(dialect, node), qt.DeepEquals, visited)

				// Only a fragment the core knows may answer that it needs its
				// parent: the core checks that answer against the parent
				// statement, and a part of a statement it does not know as a
				// fragment would reach the answer unchecked.
				c.Assert(claimsItsParent(visited.Message, node) && !isFragment(node), qt.IsFalse,
					qt.Commentf("%T answers %q", node, visited.Message))
			})
		}
	}
}

// TestEntryPoints_RefuseAGroupWithoutTheCapability pins the case the shared
// preparation exists for, on the entry point that dropped it.
func TestEntryPoints_RefuseAGroupWithoutTheCapability(t *testing.T) {
	c := qt.New(t)

	r, err := renderer.NewRendererWithCapabilities("postgres", capability.ForDialect("postgres"))
	c.Assert(err, qt.IsNil)
	err = r.VisitNode(ast.NewCreateRole("readers").SetGroup(true))

	c.Assert(err, qt.ErrorMatches, `CREATE GROUP readers, which requires target capability group_principals, unavailable on this postgres target`)
	c.Assert(r.Output(), qt.Equals, "")
}

// isFragment reports whether node is one of the parts of a statement the
// renderer core checks against the statement that carries it.
func isFragment(node ast.Node) bool {
	switch node.(type) {
	case ast.AlterOperation, ast.TypeDefinition, ast.TypeOperation:
		return true
	default:
		return false
	}
}

// answer is what an entry point returned, reduced to what the comparison
// holds equal.
type answer struct {
	Output    string
	Message   string
	Sentinels []string
}

func visitAnswer(c *qt.C, dialect string, node ast.Node) answer {
	c.Helper()
	r, err := renderer.NewRendererWithCapabilities(dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	err = r.VisitNode(node)
	return answer{Output: r.Output(), Message: fmt.Sprint(err), Sentinels: sentinels(err)}
}

func renderAnswer(c *qt.C, dialect string, node ast.Node) answer {
	c.Helper()
	r, err := renderer.NewRendererWithCapabilities(dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	output, err := r.Render(node)
	return answer{Output: output, Message: fmt.Sprint(err), Sentinels: sentinels(err)}
}

func renderSQLAnswer(dialect string, node ast.Node) answer {
	output, err := renderer.RenderSQL(dialect, node)
	return answer{Output: output, Message: fmt.Sprint(err), Sentinels: sentinels(err)}
}

// entryPointCases is every node kind as its zero value and as a typed nil,
// keyed by a readable name, followed by the named rows.
func entryPointCases(c *qt.C) map[string]ast.Node {
	c.Helper()
	cases := make(map[string]ast.Node)
	for kind, node := range nodeKindZeroValues() {
		cases[kind] = node
		cases[kind+"(nil)"] = reflect.Zero(reflect.TypeOf(node)).Interface().(ast.Node)
	}
	cases["no node"] = nil
	cases["CREATE GROUP"] = ast.NewCreateRole("readers").SetGroup(true)
	cases["DROP GROUP"] = ast.NewDropRole("readers").SetGroup(true)
	cases["GRANT on the database"] = ast.NewGrantPrivilege("readers", "DATABASE", "app", []string{"CONNECT"})
	cases["REVOKE on the database"] = ast.NewRevokePrivilege("readers", "DATABASE", "app", []string{"CONNECT"})
	cases["a list whose statement is refused"] = &ast.StatementList{Statements: []ast.Node{
		ast.NewCreateRole("readers").SetGroup(true),
	}}
	// The core passes a type definition that its CREATE TYPE renders, and the
	// dialect then refuses it as a part of a statement: a refusal that arises
	// while rendering a list, after the preparation accepted it.
	cases["a list whose statement the dialect refuses"] = &ast.StatementList{Statements: []ast.Node{
		ast.NewEnumTypeDef("active", "inactive"),
	}}
	cases["a list holding a list"] = &ast.StatementList{Statements: []ast.Node{
		&ast.StatementList{Statements: []ast.Node{&ast.CommentNode{Text: "inner"}}},
	}}
	return cases
}

// TestEntryPoints_CoverEveryNodeKind keeps the zero-value table equal to the
// node kinds core/ast declares, so a kind added later is driven through the
// entry points rather than passing unmeasured.
func TestEntryPoints_CoverEveryNodeKind(t *testing.T) {
	c := qt.New(t)

	root, err := astrouteguard.ModuleRoot()
	c.Assert(err, qt.IsNil)
	kinds, err := astrouteguard.NodeKinds(root)
	c.Assert(err, qt.IsNil)

	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, kind.Name)
	}
	c.Assert(slices.Sorted(maps.Keys(nodeKindZeroValues())), qt.DeepEquals, names)
}

// nodeKindZeroValues is one zero value of every node kind, keyed by its type
// name.
func nodeKindZeroValues() map[string]ast.Node {
	return map[string]ast.Node{
		"AddChangefeedOperation":                  &ast.AddChangefeedOperation{},
		"AddColumnOperation":                      &ast.AddColumnOperation{},
		"AddConstraintOperation":                  &ast.AddConstraintOperation{},
		"AddEnumValueOperation":                   &ast.AddEnumValueOperation{},
		"AddIndexOperation":                       &ast.AddIndexOperation{},
		"AddSkippingIndexOperation":               &ast.AddSkippingIndexOperation{},
		"AlterAsyncReplicationNode":               &ast.AlterAsyncReplicationNode{},
		"AlterChangefeedTopicOperation":           &ast.AlterChangefeedTopicOperation{},
		"AlterColumnOperation":                    &ast.AlterColumnOperation{},
		"AlterCoordinationNodeNode":               &ast.AlterCoordinationNodeNode{},
		"AlterGeneratedColumnExpressionOperation": &ast.AlterGeneratedColumnExpressionOperation{},
		"AlterIndexNode":                          &ast.AlterIndexNode{},
		"AlterIndexVisibilityOperation":           &ast.AlterIndexVisibilityOperation{},
		"AlterMaterializedViewRefreshNode":        &ast.AlterMaterializedViewRefreshNode{},
		"AlterRoleNode":                           &ast.AlterRoleNode{},
		"AlterSequenceNode":                       &ast.AlterSequenceNode{},
		"AlterSerialSequenceNode":                 &ast.AlterSerialSequenceNode{},
		"AlterTableDisableRLSNode":                &ast.AlterTableDisableRLSNode{},
		"AlterTableEnableRLSNode":                 &ast.AlterTableEnableRLSNode{},
		"AlterTableForceRLSNode":                  &ast.AlterTableForceRLSNode{},
		"AlterTableNode":                          &ast.AlterTableNode{},
		"AlterTopicNode":                          &ast.AlterTopicNode{},
		"AlterTransferNode":                       &ast.AlterTransferNode{},
		"AlterTypeNode":                           &ast.AlterTypeNode{},
		"ColumnNode":                              &ast.ColumnNode{},
		"CommentNode":                             &ast.CommentNode{},
		"CompositeAttributeOperation":             &ast.CompositeAttributeOperation{},
		"CompositeTypeDef":                        &ast.CompositeTypeDef{},
		"ConstraintNode":                          &ast.ConstraintNode{},
		"CreateAsyncReplicationNode":              &ast.CreateAsyncReplicationNode{},
		"CreateContinuousAggregateNode":           &ast.CreateContinuousAggregateNode{},
		"CreateCoordinationNodeNode":              &ast.CreateCoordinationNodeNode{},
		"CreateDatabaseNode":                      &ast.CreateDatabaseNode{},
		"CreateFunctionNode":                      &ast.CreateFunctionNode{},
		"CreateHypertableNode":                    &ast.CreateHypertableNode{},
		"CreateMaterializedViewNode":              &ast.CreateMaterializedViewNode{},
		"CreatePolicyNode":                        &ast.CreatePolicyNode{},
		"CreateRoleNode":                          &ast.CreateRoleNode{},
		"CreateSchemaNode":                        &ast.CreateSchemaNode{},
		"CreateSequenceNode":                      &ast.CreateSequenceNode{},
		"CreateSynonymNode":                       &ast.CreateSynonymNode{},
		"CreateTableNode":                         &ast.CreateTableNode{},
		"CreateTopicNode":                         &ast.CreateTopicNode{},
		"CreateTransferNode":                      &ast.CreateTransferNode{},
		"CreateTriggerNode":                       &ast.CreateTriggerNode{},
		"CreateTypeNode":                          &ast.CreateTypeNode{},
		"CreateViewNode":                          &ast.CreateViewNode{},
		"DefaultPrivilegeNode":                    &ast.DefaultPrivilegeNode{},
		"DomainConstraintOperation":               &ast.DomainConstraintOperation{},
		"DomainDefaultOperation":                  &ast.DomainDefaultOperation{},
		"DomainNotNullOperation":                  &ast.DomainNotNullOperation{},
		"DomainTypeDef":                           &ast.DomainTypeDef{},
		"DropAsyncReplicationNode":                &ast.DropAsyncReplicationNode{},
		"DropChangefeedOperation":                 &ast.DropChangefeedOperation{},
		"DropColumnOperation":                     &ast.DropColumnOperation{},
		"DropConstraintOperation":                 &ast.DropConstraintOperation{},
		"DropContinuousAggregateNode":             &ast.DropContinuousAggregateNode{},
		"DropCoordinationNodeNode":                &ast.DropCoordinationNodeNode{},
		"DropExtensionNode":                       &ast.DropExtensionNode{},
		"DropFunctionNode":                        &ast.DropFunctionNode{},
		"DropIndexNode":                           &ast.DropIndexNode{},
		"DropMaterializedViewNode":                &ast.DropMaterializedViewNode{},
		"DropPolicyNode":                          &ast.DropPolicyNode{},
		"DropRoleNode":                            &ast.DropRoleNode{},
		"DropRowDeletionPolicyOperation":          &ast.DropRowDeletionPolicyOperation{},
		"DropSequenceNode":                        &ast.DropSequenceNode{},
		"DropSynonymNode":                         &ast.DropSynonymNode{},
		"DropTableNode":                           &ast.DropTableNode{},
		"DropTopicNode":                           &ast.DropTopicNode{},
		"DropTransferNode":                        &ast.DropTransferNode{},
		"DropTriggerNode":                         &ast.DropTriggerNode{},
		"DropTypeNode":                            &ast.DropTypeNode{},
		"DropViewNode":                            &ast.DropViewNode{},
		"EnumNode":                                &ast.EnumNode{},
		"EnumTypeDef":                             &ast.EnumTypeDef{},
		"ExtendedPropertyNode":                    &ast.ExtendedPropertyNode{},
		"ExtensionNode":                           &ast.ExtensionNode{},
		"GrantPrivilegeNode":                      &ast.GrantPrivilegeNode{},
		"GrantRoleMembershipNode":                 &ast.GrantRoleMembershipNode{},
		"IndexNode":                               &ast.IndexNode{},
		"ModifyColumnOperation":                   &ast.ModifyColumnOperation{},
		"ModifyTTLOperation":                      &ast.ModifyTTLOperation{},
		"MySQLRoutineNode":                        &ast.MySQLRoutineNode{},
		"ObjectCommentNode":                       &ast.ObjectCommentNode{},
		"OpaqueRoutineNode":                       &ast.OpaqueRoutineNode{},
		"PostgresDoBlockNode":                     &ast.PostgresDoBlockNode{},
		"PostgresRoutineNode":                     &ast.PostgresRoutineNode{},
		"RangeTypeDef":                            &ast.RangeTypeDef{},
		"RawSQLNode":                              &ast.RawSQLNode{},
		"RefreshMaterializedViewNode":             &ast.RefreshMaterializedViewNode{},
		"RenameColumnOperation":                   &ast.RenameColumnOperation{},
		"RenameConstraintOperation":               &ast.RenameConstraintOperation{},
		"RenameEnumValueOperation":                &ast.RenameEnumValueOperation{},
		"RenameIndexOperation":                    &ast.RenameIndexOperation{},
		"RenameTableOperation":                    &ast.RenameTableOperation{},
		"RenameTypeOperation":                     &ast.RenameTypeOperation{},
		"ReplaceIndexOperation":                   &ast.ReplaceIndexOperation{},
		"ResetRowTTLOperation":                    &ast.ResetRowTTLOperation{},
		"RevokeDefaultPrivilegeNode":              &ast.RevokeDefaultPrivilegeNode{},
		"RevokePrivilegeNode":                     &ast.RevokePrivilegeNode{},
		"RevokeRoleMembershipNode":                &ast.RevokeRoleMembershipNode{},
		"SQLServerRoutineNode":                    &ast.SQLServerRoutineNode{},
		"SetCommentOperation":                     &ast.SetCommentOperation{},
		"SetConstraintCommentOperation":           &ast.SetConstraintCommentOperation{},
		"SetIndexPartitioningOperation":           &ast.SetIndexPartitioningOperation{},
		"SetRowDeletionPolicyOperation":           &ast.SetRowDeletionPolicyOperation{},
		"SetRowTTLOperation":                      &ast.SetRowTTLOperation{},
		"SetYDBColumnFamiliesOperation":           &ast.SetYDBColumnFamiliesOperation{},
		"SetYDBTablePartitioningOperation":        &ast.SetYDBTablePartitioningOperation{},
		"StatementList":                           &ast.StatementList{},
		"UpsertNode":                              &ast.UpsertNode{},
		"ValidateConstraintOperation":             &ast.ValidateConstraintOperation{},
	}
}
