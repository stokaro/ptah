package ydbscheme

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
)

// CommonEffects describes scheme paths, principals, and the secret, topic and
// external data source reads of a common AST node.
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
	effects = append(effects, reads...)
	effects = append(effects, topicReads(root, node)...)
	if table, ok := node.(*ast.CreateTableNode); ok && table.YDBColumnTable != nil {
		effects = append(effects, TieredTTLReads(root, table.YDBColumnTable.TTL)...)
	}
	return effects, nil
}

// TieredTTLReads names the external data sources a column table's tiered TTL
// moves rows to, by their paths read against root: a statement that creates
// the table with the policy, or sets it, reads each one, so the data source's
// owner creates it before and drops it after. A tier that deletes rows reads
// nothing, and neither does a path outside root, one written absolute where
// root is not known, or one that cannot name a data source.
func TieredTTLReads(root string, policy *ast.YDBTieredTTLSpec) []plangraph.Effect {
	if policy == nil {
		return nil
	}
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, tier := range policy.Tiers {
		ref, ok := ydbexternal.ResolveSource(root, tier.ExternalSource)
		if !ok || seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		effects = append(effects, plangraph.Effect{Subject: ref, Action: plangraph.Read})
	}
	return effects
}

// secretReads names each YDB secret a statement reads by its path when it
// runs: the credentials of an async replication or a transfer. See
// [SecretPathReads].
func secretReads(root string, node ast.Node) ([]plangraph.Effect, error) {
	var paths []string
	switch n := node.(type) {
	case *ast.CreateAsyncReplicationNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	case *ast.AlterAsyncReplicationNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	case *ast.CreateTransferNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	case *ast.AlterTransferNode:
		paths = connectionSecretPaths(n.Spec.Connection)
	}
	return SecretPathReads(root, paths...)
}

// SecretPathReads is a read of each YDB secret paths name, as a statement that
// names a secret by its path reads it when it runs: YDB looks the secret up
// when an async replication or a transfer is created or its connection
// changes (`secret ... not found`), so a secret's owner orders its creation
// before these reads. YDB stores the path absolute, so an absolute path under
// root, the absolute path of the database, names the same secret as the
// relative one. A path outside root is refused; an empty path, one written
// absolute where root is not known, and one that cannot name a secret read
// nothing Ptah manages. Each secret is read once.
func SecretPathReads(root string, paths ...string) ([]plangraph.Effect, error) {
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, written := range paths {
		if strings.TrimSpace(written) == "" {
			continue
		}
		ref, err := ydbsecret.ResolvePath(root, written)
		if errors.Is(err, ydbsecret.ErrOutsideDatabase) {
			return nil, (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
				Code: schemavalidation.InvalidSchema, Kind: "secret", Object: written,
				Message: fmt.Sprintf("secret path %q is outside the database %s, so no statement of it can read the secret",
					written, "/"+strings.Trim(root, "/")),
			}}}).Err(platform.YDB)
		}
		if err != nil || seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		effects = append(effects, plangraph.Effect{Subject: ref, Action: plangraph.Read})
	}
	return effects, nil
}

// topicReads names the topic of this database a transfer reads, by its path.
// See [TopicPathReads].
func topicReads(root string, node ast.Node) []plangraph.Effect {
	var source string
	switch n := node.(type) {
	case *ast.CreateTransferNode:
		if n.Spec.Connection.ConnectionString == "" {
			source = n.Spec.Source
		}
	case *ast.AlterTransferNode:
		if n.Spec.Connection.ConnectionString == "" {
			source = n.Spec.Source
		}
	case *ast.DropTransferNode:
		source = n.Topic
	}
	return TopicPathReads(root, source)
}

// TopicPathReads is a read of the standalone topic of this database at source,
// as a transfer of a topic in its own database reads it: the topic's owner
// creates or changes a topic before a statement that reads it, and drops one
// after. A source written absolute is read against root, as a secret's path
// is; an empty one, one outside root, and one written absolute where root is
// not known name no topic this plan manages and read nothing.
func TopicPathReads(root, source string) []plangraph.Effect {
	if strings.TrimSpace(source) == "" {
		return nil
	}
	ref, err := ydbtopic.ResolvePath(root, source)
	if err != nil {
		return nil
	}
	return []plangraph.Effect{{Subject: ref, Action: plangraph.Read}}
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
	default:
		return externalSchemeUse(node)
	}
}

func externalSchemeUse(node ast.Node) schemeUse {
	switch n := node.(type) {
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
