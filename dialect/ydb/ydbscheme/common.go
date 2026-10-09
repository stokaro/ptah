package ydbscheme

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/schemavalidation"
)

// CommonEffects describes shared path occupancy for a local common AST node.
// The native migration and declaration hosts use the same resource identities.
// It does not claim complete query or runtime effects; unrecognized nodes have
// unknown footprints. A process adapter exchanges the resulting metadata in a
// batch, not the Go AST node or a per-node remote call.
func CommonEffects(builder objectidentity.Builder, node ast.Node) ([]plangraph.Effect, error) {
	use := commonSchemeUse(node)
	if use.action == "" {
		return nil, nil
	}
	ref := builder.Table(use.name)
	if ref.Name.Source == "" || ref.Name.Normalized == "" {
		return nil, (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
			Code: schemavalidation.InvalidSchema, Kind: "schema", Object: use.name, Message: "YDB scheme operation requires an object name",
		}}}).Err(platform.YDB)
	}
	physical := ObjectPath(use.name)
	schema, name := "", physical
	if slash := strings.LastIndex(physical, "/"); slash >= 0 {
		schema, name = physical[:slash], physical[slash+1:]
	}
	effects := []plangraph.Effect{{Subject: Path(schema, name), Action: use.action}}
	if use.table {
		effects = append(effects, plangraph.Effect{Subject: ref, Action: use.action})
	}
	return effects, nil
}

type schemeUse struct {
	name   string
	action plangraph.Action
	table  bool
}

func commonSchemeUse(node ast.Node) schemeUse {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return schemeUse{n.Name, plangraph.Create, true}
	case *ast.DropTableNode:
		return schemeUse{n.Name, plangraph.Drop, true}
	case *ast.AlterTableNode:
		return schemeUse{n.Name, plangraph.Alter, true}
	case *ast.CreateViewNode:
		if n.Replace {
			return schemeUse{n.Name, plangraph.Alter, false}
		}
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.DropViewNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	case *ast.CreateTopicNode:
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.AlterTopicNode:
		return schemeUse{n.Name, plangraph.Alter, false}
	case *ast.DropTopicNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	case *ast.CreateSecretNode:
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.AlterSecretNode:
		return schemeUse{n.Name, plangraph.Alter, false}
	case *ast.DropSecretNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	default:
		return externalSchemeUse(node)
	}
}

func externalSchemeUse(node ast.Node) schemeUse {
	switch n := node.(type) {
	case *ast.CreateExternalDataSourceNode:
		if n.Replace {
			return schemeUse{n.Name, plangraph.Alter, false}
		}
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.DropExternalDataSourceNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	case *ast.CreateExternalTableNode:
		if n.Replace {
			return schemeUse{n.Name, plangraph.Alter, false}
		}
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.DropExternalTableNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	case *ast.CreateAsyncReplicationNode:
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.AlterAsyncReplicationNode:
		return schemeUse{n.Name, plangraph.Alter, false}
	case *ast.DropAsyncReplicationNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	case *ast.CreateTransferNode:
		return schemeUse{n.Name, plangraph.Create, false}
	case *ast.AlterTransferNode:
		return schemeUse{n.Name, plangraph.Alter, false}
	case *ast.DropTransferNode:
		return schemeUse{n.Name, plangraph.Drop, false}
	default:
		return schemeUse{}
	}
}
