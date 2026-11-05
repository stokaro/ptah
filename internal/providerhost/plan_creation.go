package providerhost

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
	"ptah.run/internal/tableref"
)

// pendingTableCreation gives the captured declaration a graph footprint,
// without a host SQL interpretation. A provider must replace its graph step before
// a plan can leave this package. Rendering it is always an error.
type pendingTableCreation struct{ name string }

func (n *pendingTableCreation) Accept(ast.Visitor) error {
	return unsupported("unclaimed provider table creation " + n.name)
}

// creationNodes creates a table: the provider's step for an owned lifecycle,
// or the common creation, each followed by the steps that set its comments on
// an engine that carries them on the common-object wire. An owner that
// creates the table receives the declaration with its comments, and the
// comment steps are what sets them, as for a common creation: the wire is
// where an engine that lists comments among its common objects takes them.
func (p commonPlanner) creationNodes(creation difftypes.TableCreation) ([]ast.Node, error) {
	allowed := []string{"Name", "Table", "Fields", "OwnedObjects", "FeatureCoverage", "DependsOn"}
	if !p.commonTables {
		allowed = append(allowed, "Enums", "Constraints")
	}
	if err := onlyFields(creation, allowed...); err != nil {
		return nil, err
	}
	declaration := schemacapture.TableDeclaration{Table: creation.Table, Fields: creation.Fields}
	var comments []ast.Node
	if p.comments {
		declaration, comments = creationComments(declaration)
	}
	if !p.commonTables {
		return append([]ast.Node{&pendingTableCreation{name: tableref.CanonicalExact(creation.Table.Schema, creation.Table.Name)}}, comments...), nil
	}
	node, err := commonTable(declaration)
	if err != nil {
		return nil, err
	}
	return append([]ast.Node{node}, comments...), nil
}

// pendingTableDrop stands in for the removal of a table whose provider owns
// the table lifecycle. The host has no common-table interpretation to render a
// DROP with, so only the provider's graph step can. Rendering it is always an
// error.
type pendingTableDrop struct{ name string }

func (n *pendingTableDrop) Accept(ast.Visitor) error {
	return unsupported("unclaimed provider table removal " + n.name)
}

// pendingColumnChange stands in for a column addition or removal on a table
// whose provider owns the table lifecycle. The host has no common-table
// interpretation to render it with, so only the provider's graph step can. The
// operand keeps the full captured field, so the provider sees facets, defaults
// and comments. Rendering it is always an error.
type pendingColumnChange struct {
	table  string
	column string
	// added is nil for a removal.
	added *ast.ColumnNode
}

func (n *pendingColumnChange) Accept(ast.Visitor) error {
	if n.added == nil {
		return unsupported(fmt.Sprintf("unclaimed provider removal of column %s from table %s", n.column, n.table))
	}
	return unsupported(fmt.Sprintf("unclaimed provider addition of column %s to table %s", n.column, n.table))
}

// rejectUnclaimedChanges refuses a plan in which a provider left a creation,
// a removal, a column change or an index change as the host's placeholder,
// naming the first one.
func rejectUnclaimedChanges(nodes []ast.Node) error {
	for _, node := range nodes {
		switch pending := node.(type) {
		case *pendingTableCreation:
			return pending.Accept(nil)
		case *pendingTableDrop:
			return pending.Accept(nil)
		case *pendingColumnChange:
			return pending.Accept(nil)
		case *pendingIndexChange:
			return pending.Accept(nil)
		case *pendingIndexRename:
			return pending.Accept(nil)
		}
	}
	return nil
}
