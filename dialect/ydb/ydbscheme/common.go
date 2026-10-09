package ydbscheme

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbsecret"
)

// CommonEffects describes scheme paths, principals and secret reads of a common
// AST node.
// The native migration and declaration hosts use the same resource identities.
// It does not claim complete query or runtime effects; unrecognized nodes have
// unknown footprints. A process adapter exchanges the resulting metadata in a
// batch, not the Go AST node or a per-node remote call.
//
// root is the absolute path of the database the statement runs in, such as
// /local, or empty when it is not known. A secret path the statement writes
// absolute is read relative to root, and one outside root is refused, since
// no statement of the database can read it. With an empty root an absolute
// secret path reads nothing.
func CommonEffects(builder objectidentity.Builder, root string, node ast.Node) ([]plangraph.Effect, error) {
	if name, action := principalUse(node); action != "" {
		ref := builder.Role(name)
		if ref.Name.Source == "" || ref.Name.Normalized == "" {
			return nil, invalidCommonName("role", name)
		}
		return []plangraph.Effect{{Subject: ref, Action: action}}, nil
	}
	use := commonSchemeUse(node)
	if use.action == "" {
		return nil, nil
	}
	ref := builder.Table(use.name)
	if ref.Name.Source == "" || ref.Name.Normalized == "" {
		return nil, invalidCommonName("schema", use.name)
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
	reads, err := secretReads(root, node)
	if err != nil {
		return nil, err
	}
	return append(effects, reads...), nil
}

// secretReads names each YDB secret a statement reads by its path when it
// runs: the _SECRET_PATH options of an external data source and the
// credentials of an async replication or a transfer. YDB looks the secret up
// when the object is created or its connection changes (`secret ... not
// found`), so a secret's owner orders its creation before these reads. YDB
// stores the path absolute, so an absolute path under root names the same
// secret as the relative one. A path outside root is refused; a path that
// cannot name a secret is left out.
func secretReads(root string, node ast.Node) ([]plangraph.Effect, error) {
	var paths []string
	switch n := node.(type) {
	case *ast.CreateExternalDataSourceNode:
		for _, option := range slices.Sorted(maps.Keys(n.Options)) {
			if strings.HasSuffix(strings.ToUpper(option), "_SECRET_PATH") {
				paths = append(paths, n.Options[option])
			}
		}
	case *ast.CreateAsyncReplicationNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	case *ast.AlterAsyncReplicationNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	case *ast.CreateTransferNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	case *ast.AlterTransferNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	}
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, written := range paths {
		relative, known, err := secretPathUnder(root, written)
		if err != nil {
			return nil, err
		}
		if !known {
			continue
		}
		ref := ydbsecret.Ref(ydbsecret.SplitPath(relative))
		if ydbsecret.ValidateIdentity(ref) != nil || seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		effects = append(effects, plangraph.Effect{Subject: ref, Action: plangraph.Read})
	}
	return effects, nil
}

// secretPathUnder returns the secret path written relative to the database
// root. It reports false for an empty path and for an absolute one when the
// root is not known, and refuses an absolute path outside root.
func secretPathUnder(root, written string) (string, bool, error) {
	written = strings.TrimSpace(written)
	switch {
	case written == "":
		return "", false, nil
	case !strings.HasPrefix(written, "/"):
		return written, true, nil
	case strings.Trim(root, "/") == "":
		return "", false, nil
	}
	database := "/" + strings.Trim(root, "/")
	relative, under := strings.CutPrefix(path.Clean(written), database+"/")
	if !under {
		return "", false, (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
			Code: schemavalidation.InvalidSchema, Kind: "secret", Object: written,
			Message: fmt.Sprintf("secret path %q is outside the database %s, so no statement of it can read the secret", written, database),
		}}}).Err(platform.YDB)
	}
	return relative, true, nil
}

func connectionSecretPaths(connection ast.ReplicationConnectionSpec) []string {
	return []string{connection.TokenSecretPath, connection.PasswordSecretPath}
}

func invalidCommonName(kind, name string) error {
	message := "YDB scheme operation requires an object name"
	if kind == "role" {
		message = "YDB principal operation requires an object name"
	}
	return (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
		Code: schemavalidation.InvalidSchema, Kind: kind, Object: name, Message: message,
	}}}).Err(platform.YDB)
}

// Workload classifiers refer to users and groups by their database identity,
// never by scheme paths. Capturing the footprint lets their owner order routing
// changes around principal creation and removal without receiving host AST nodes.
func principalUse(node ast.Node) (string, plangraph.Action) {
	switch principal := node.(type) {
	case *ast.CreateRoleNode:
		return principal.Name, plangraph.Create
	case *ast.AlterRoleNode:
		return principal.Name, plangraph.Alter
	case *ast.DropRoleNode:
		return principal.Name, plangraph.Drop
	default:
		return "", ""
	}
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
