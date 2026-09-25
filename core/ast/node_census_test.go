package ast_test

import (
	"errors"
	"go/types"
	"reflect"
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"
	"golang.org/x/tools/go/packages"

	"ptah.run/core/ast"
)

// nodeKindFloor is the number of node kinds this census describes. It is a
// floor, not an equality: a reader that stopped matching reports a small set
// rather than an error, and a census that silently shrank to nothing would
// otherwise read as success. Growth is expected and lands in nodeCensus below;
// a drop is a decision, and lowering this number is how that decision is
// recorded.
const nodeKindFloor = 82

// censusRow is one node kind the census accepts.
//
// Only the instance is carried. What the visitor should see is derived from the
// node's own type, because every kind hands the visitor itself -- and a table
// restating that per row would be 82 copies of one rule, able to disagree with
// it.
type censusRow struct {
	// node is the instance the census accepts. It is the zero value except
	// where a comment on the row says why a zero value cannot describe the
	// kind.
	node any
}

// nodeCensus is the classification every ast.Node implementation is held to.
// TestNodeCensus_CoversEveryTypeImplementingNode reads the node kinds out of
// the package source and compares them with this table, so a kind added
// without a row here fails rather than passing unmeasured.
var nodeCensus = []censusRow{
	// Statements and their parts. A zero value describes each: the census reads
	// no field off the node it accepts.
	{node: &ast.AlterMaterializedViewRefreshNode{}},
	{node: &ast.AlterRoleNode{}},
	{node: &ast.AlterSequenceNode{}},
	{node: &ast.AlterTableDisableRLSNode{}},
	{node: &ast.AlterTableEnableRLSNode{}},
	{node: &ast.AlterTableForceRLSNode{}},
	{node: &ast.AlterTableNode{}},
	{node: &ast.AlterTypeNode{}},
	{node: &ast.ColumnNode{}},
	{node: &ast.CommentNode{}},
	{node: &ast.ConstraintNode{}},
	{node: &ast.CreateContinuousAggregateNode{}},
	{node: &ast.CreateDatabaseNode{}},
	{node: &ast.CreateFunctionNode{}},
	{node: &ast.CreateHypertableNode{}},
	{node: &ast.CreateMaterializedViewNode{}},
	{node: &ast.CreatePolicyNode{}},
	{node: &ast.CreateRoleNode{}},
	{node: &ast.CreateSchemaNode{}},
	{node: &ast.CreateSequenceNode{}},
	{node: &ast.CreateSynonymNode{}},
	{node: &ast.CreateTableNode{}},
	{node: &ast.CreateTriggerNode{}},
	{node: &ast.CreateTypeNode{}},
	{node: &ast.CreateViewNode{}},
	{node: &ast.DefaultPrivilegeNode{}},
	{node: &ast.DropContinuousAggregateNode{}},
	{node: &ast.DropExtensionNode{}},
	{node: &ast.DropFunctionNode{}},
	{node: &ast.DropIndexNode{}},
	{node: &ast.DropMaterializedViewNode{}},
	{node: &ast.DropPolicyNode{}},
	{node: &ast.DropRoleNode{}},
	{node: &ast.DropSequenceNode{}},
	{node: &ast.DropSynonymNode{}},
	{node: &ast.DropTableNode{}},
	{node: &ast.DropTriggerNode{}},
	{node: &ast.DropTypeNode{}},
	{node: &ast.DropViewNode{}},
	{node: &ast.EnumNode{}},
	{node: &ast.ExtendedPropertyNode{}},
	{node: &ast.ExtensionNode{}},
	{node: &ast.GrantPrivilegeNode{}},
	{node: &ast.IndexNode{}},
	{node: &ast.RawSQLNode{}},
	{node: &ast.RefreshMaterializedViewNode{}},
	{node: &ast.RevokeDefaultPrivilegeNode{}},
	{node: &ast.RevokePrivilegeNode{}},
	{node: &ast.UpsertNode{}},

	// The ALTER TABLE operations that hold a node of their own. Each row fills
	// that node in, because an operation without one describes a shape
	// production never produces, and a renderer reached with it dereferences
	// what the row left nil.
	{node: &ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "id"}}},
	{node: &ast.ModifyColumnOperation{Column: &ast.ColumnNode{Name: "id"}}},
	{node: &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Name: "pk"}}},
	{node: &ast.ValidateConstraintOperation{ConstraintName: "pk"}},

	// The routines, whose body is preserved verbatim. The node carries the
	// dialect and the kind that selected it, and the visitor is handed both.
	{node: &ast.MySQLRoutineNode{}},
	{node: &ast.OpaqueRoutineNode{}},
	{node: &ast.PostgresDoBlockNode{}},
	{node: &ast.PostgresRoutineNode{}},
	{node: &ast.SQLServerRoutineNode{}},

	// The remaining ALTER TABLE operations. Each is rendered inside the ALTER
	// statement that carries it, and a renderer reached with one standing alone
	// refuses it rather than emitting a fragment.
	{node: &ast.AddIndexOperation{}},
	{node: &ast.AddSkippingIndexOperation{}},
	{node: &ast.AlterGeneratedColumnExpressionOperation{}},
	{node: &ast.AlterColumnOperation{}},
	{node: &ast.DropColumnOperation{}},
	{node: &ast.DropConstraintOperation{}},
	{node: &ast.DropRowDeletionPolicyOperation{}},
	{node: &ast.ModifyTTLOperation{}},
	{node: &ast.RenameColumnOperation{}},
	{node: &ast.RenameConstraintOperation{}},
	{node: &ast.RenameTableOperation{}},
	{node: &ast.ResetRowTTLOperation{}},
	{node: &ast.SetCommentOperation{}},
	{node: &ast.SetRowDeletionPolicyOperation{}},
	{node: &ast.SetRowTTLOperation{}},

	// The type definitions and the ALTER TYPE operations. Each is rendered
	// inside the CREATE TYPE or ALTER TYPE statement that carries it.
	{node: &ast.AddEnumValueOperation{}},
	{node: &ast.CompositeAttributeOperation{}},
	{node: &ast.CompositeTypeDef{}},
	{node: &ast.DomainConstraintOperation{}},
	{node: &ast.DomainDefaultOperation{}},
	{node: &ast.DomainNotNullOperation{}},
	{node: &ast.DomainTypeDef{}},
	{node: &ast.EnumTypeDef{}},
	{node: &ast.RangeTypeDef{}},
	{node: &ast.RenameEnumValueOperation{}},
	{node: &ast.RenameTypeOperation{}},

	// The container. It reaches the visitor like any other node and walks
	// nothing; what a visitor that descends sees is pinned in
	// statement_list_dispatch_test.go.
	{node: &ast.StatementList{}},
}

// acceptOutcome is everything one Accept call did that a visitor can observe:
// how many Visitor methods it reached, which one it reached last, the type of
// the node that method received, whether that node was the accepted node
// itself, and the error Accept returned.
type acceptOutcome struct {
	Calls    int
	Method   string
	NodeType string
	Self     bool
	Err      string
}

// wantOutcome is the observation this row describes when the visitor answers
// every call with answer.
//
// One shape for all 82 kinds: Accept reaches VisitNode once, hands it this node,
// and returns what the visitor said.
func (r censusRow) wantOutcome(answer string) acceptOutcome {
	return acceptOutcome{
		Calls:    1,
		Method:   censusMethodName(r.node.(ast.Node)),
		NodeType: censusNodeTypeName(r),
		Self:     true,
		Err:      answer,
	}
}

// censusRefusal is the answer the recording visitor gives when the census is
// measuring what Accept does with an error.
const censusRefusal = "census visitor refused"

// TestNodeCensus_CoversEveryTypeImplementingNode reads the node kinds out of
// this package's source with go/types and holds nodeCensus to them. The
// classification below is only worth what its coverage is worth: a kind nobody
// wrote a row for would otherwise be a kind nothing describes.
func TestNodeCensus_CoversEveryTypeImplementingNode(t *testing.T) {
	c := qt.New(t)

	declared := loadNodeTypes(c).pointer

	c.Assert(len(declared) >= nodeKindFloor, qt.IsTrue, qt.Commentf(
		"the source walk found %d node kinds and the floor is %d; a walk that stopped matching reports a short list, not an error",
		len(declared), nodeKindFloor))
	c.Assert(declared, qt.DeepEquals, censusNodeTypeNames())
}

// TestNodeCensus_NoValueTypeImplementsNode pins that Accept is declared on a
// pointer receiver everywhere. A dispatcher that switches on value types
// compiles and matches nothing, which is the cheapest way to lose a node kind
// without a build error.
func TestNodeCensus_NoValueTypeImplementsNode(t *testing.T) {
	c := qt.New(t)

	c.Assert(loadNodeTypes(c).value, qt.HasLen, 0)
}

// TestNodeCensus_AcceptReachesTheClassifiedHandler is the behavioral half of
// the census: every node kind is accepted by a recording visitor, and what the
// visitor saw is compared with the row that classifies the kind. A change to
// which node reaches the visitor shows up here as a diff, not as silence.
func TestNodeCensus_AcceptReachesTheClassifiedHandler(t *testing.T) {
	for _, row := range nodeCensus {
		t.Run(censusNodeTypeName(row), func(t *testing.T) {
			c := qt.New(t)

			node := censusNode(c, row)

			c.Assert(observeAccept(c, node, nil), qt.DeepEquals, row.wantOutcome(""))
			c.Assert(observeAccept(c, node, errors.New(censusRefusal)), qt.DeepEquals, row.wantOutcome(row.refusal()))
		})
	}
}

// dispatchTally counts what a recording visitor saw across every node kind,
// grouped by what Accept did rather than by what the table says it should do.
type dispatchTally struct {
	ReachedOwnMethod     int
	ReachedAnotherMethod int
	ReachedNoMethod      int
}

// TestNodeCensus_DispatchShapeTally states the whole dispatch surface as three
// measured numbers.
//
// Every kind reaching its own handler is the property the type switch rests on:
// a renderer dispatches on the node it is handed, so a kind that handed the
// visitor something else would be rendered as that something else, and a kind
// that handed it nothing would render as nothing at all.
func TestNodeCensus_DispatchShapeTally(t *testing.T) {
	c := qt.New(t)

	c.Assert(observeDispatchTally(c), qt.DeepEquals, dispatchTally{
		ReachedOwnMethod:     len(nodeCensus),
		ReachedAnotherMethod: 0,
		ReachedNoMethod:      0,
	})
}

// observeDispatchTally accepts every node kind in the census and groups the
// results by what the visitor saw.
func observeDispatchTally(c *qt.C) dispatchTally {
	c.Helper()

	var tally dispatchTally
	for _, row := range nodeCensus {
		out := observeAccept(c, censusNode(c, row), nil)
		switch {
		case out.Calls == 0:
			tally.ReachedNoMethod++
		case out.Self:
			tally.ReachedOwnMethod++
		default:
			tally.ReachedAnotherMethod++
		}
	}

	return tally
}

// refusal is the error text Accept returns when the visitor refuses.
//
// Every kind dispatches, so every kind carries the visitor's answer back to the
// caller unchanged. A kind that swallowed it would leave a renderer's refusal
// invisible to whoever called Accept.
func (r censusRow) refusal() string { return censusRefusal }

// observeAccept accepts node with a recording visitor that answers every call
// with answer, and reports what the visitor saw.
func observeAccept(c *qt.C, node ast.Node, answer error) acceptOutcome {
	c.Helper()

	visitor := &censusVisitor{err: answer}
	err := node.Accept(visitor)

	return summarizeAccept(node, err, visitor.calls)
}

// summarizeAccept reduces one Accept call to the outcome the census compares.
func summarizeAccept(node ast.Node, err error, calls []censusCall) acceptOutcome {
	out := acceptOutcome{Calls: len(calls)}
	if err != nil {
		out.Err = err.Error()
	}
	if len(calls) == 0 {
		return out
	}

	last := calls[len(calls)-1]
	out.Method = last.method
	out.NodeType = reflect.TypeOf(last.node).Elem().Name()
	out.Self = last.node == any(node)

	return out
}

// singleCall accepts node with a recording visitor and returns the one call
// Accept made, failing when Accept made any other number of them.
func singleCall(c *qt.C, node ast.Node) censusCall {
	c.Helper()

	visitor := &censusVisitor{}
	c.Assert(node.Accept(visitor), qt.IsNil)
	c.Assert(visitor.calls, qt.HasLen, 1)

	return visitor.calls[0]
}

// censusNode reports the row's instance as an ast.Node. The field is typed any
// so that a type which stops implementing ast.Node is reported here, by the
// census, instead of failing the package's whole test build.
func censusNode(c *qt.C, row censusRow) ast.Node {
	c.Helper()

	node, ok := row.node.(ast.Node)
	c.Assert(ok, qt.IsTrue, qt.Commentf("%T does not implement ast.Node", row.node))

	return node
}

// censusNodeTypeName is the name of the type a row describes.
func censusNodeTypeName(row censusRow) string {
	return reflect.TypeOf(row.node).Elem().Name()
}

// censusNodeTypeNames is the sorted set of type names nodeCensus describes.
func censusNodeTypeNames() []string {
	names := make([]string, 0, len(nodeCensus))
	for _, row := range nodeCensus {
		names = append(names, censusNodeTypeName(row))
	}
	sort.Strings(names)

	return names
}

// loadedNodeTypes is what a go/types walk over this package's source reports:
// the named types whose pointer implements ast.Node, and the named types whose
// value does.
type loadedNodeTypes struct {
	pointer []string
	value   []string
}

// loadNodeTypes type-checks this package from source and classifies every
// named type in it against ast.Node. Reflection cannot enumerate a package's
// types, so the census reads them the way the compiler does.
func loadNodeTypes(c *qt.C) loadedNodeTypes {
	c.Helper()

	cfg := &packages.Config{Mode: packages.NeedName |
		packages.NeedFiles |
		packages.NeedCompiledGoFiles |
		packages.NeedImports |
		packages.NeedDeps |
		packages.NeedTypes |
		packages.NeedSyntax |
		packages.NeedTypesInfo}
	pkgs, err := packages.Load(cfg, ".")
	c.Assert(err, qt.IsNil)
	c.Assert(pkgs, qt.HasLen, 1)

	pkg := pkgs[0]
	// A package that failed to load enumerates nothing, and an empty
	// enumeration reads exactly like a census that found no node kinds.
	c.Assert(pkg.Errors, qt.HasLen, 0)
	c.Assert(pkg.PkgPath, qt.Equals, "ptah.run/core/ast")

	nodeObj := pkg.Types.Scope().Lookup("Node")
	c.Assert(nodeObj, qt.IsNotNil)
	node, ok := nodeObj.Type().Underlying().(*types.Interface)
	c.Assert(ok, qt.IsTrue)

	found := classifyAgainstNode(pkg.Types.Scope(), node)
	sort.Strings(found.pointer)
	sort.Strings(found.value)

	return found
}

// classifyAgainstNode splits the concrete named types declared in scope by
// whether their pointer, or their value, implements node.
func classifyAgainstNode(scope *types.Scope, node *types.Interface) loadedNodeTypes {
	var found loadedNodeTypes
	for _, name := range scope.Names() {
		typeName, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || typeName.IsAlias() {
			continue
		}
		named, ok := typeName.Type().(*types.Named)
		if !ok {
			continue
		}
		if _, isInterface := named.Underlying().(*types.Interface); isInterface {
			continue
		}
		if types.Implements(types.NewPointer(named), node) {
			found.pointer = append(found.pointer, name)
		}
		if types.Implements(named, node) {
			found.value = append(found.value, name)
		}
	}

	return found
}
