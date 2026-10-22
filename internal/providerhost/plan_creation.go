package providerhost

import (
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

func (p commonPlanner) creationNode(creation difftypes.TableCreation) (ast.Node, error) {
	allowed := []string{"Name", "Table", "Fields", "OwnedObjects", "FeatureCoverage", "DependsOn"}
	if !p.commonTables {
		allowed = append(allowed, "Enums", "Constraints")
	}
	if err := onlyFields(creation, allowed...); err != nil {
		return nil, err
	}
	if !p.commonTables {
		return &pendingTableCreation{name: tableref.CanonicalExact(creation.Table.Schema, creation.Table.Name)}, nil
	}
	return commonTable(schemacapture.TableDeclaration{Table: creation.Table, Fields: creation.Fields})
}

func rejectUnclaimedCreations(nodes []ast.Node) error {
	for _, node := range nodes {
		if pending, ok := node.(*pendingTableCreation); ok {
			return pending.Accept(nil)
		}
	}
	return nil
}
