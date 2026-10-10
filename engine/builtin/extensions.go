package builtin

import (
	"sync"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/engine/builtin/internal/dialects/postgres"
	"ptah.run/internal/ydbextensions"
)

// cockroachDBRegistry is built once. A registry holds nothing a render changes,
// and the renderer is built per statement, so building it per renderer built
// the same handlers for every statement of a plan.
var cockroachDBRegistry = sync.OnceValues(crdbrender.Registry)

// renderOwners is what a target's feature owners contribute to rendering it,
// selected in [ownersFor], the one place at this composition boundary that
// names them.
type renderOwners struct {
	// extensions builds the owners' handler registry; nil means none.
	extensions func() (renderer.Extensions, error)
	// tableStorage renders owned CREATE TABLE facets; nil means none.
	tableStorage func(target string, caps capability.Capabilities, table string, facets schemaext.Facets) (string, error)
}

// ownersFor selects a target's feature owners. Neutral contracts and
// non-owning backends know no payload types.
func ownersFor(dialect string) renderOwners {
	switch platform.NormalizeDialect(dialect) {
	case platform.YDB:
		return renderOwners{extensions: ydbextensions.Registry}
	case platform.ClickHouse:
		return renderOwners{extensions: chrender.Registry}
	case platform.CockroachDB:
		return renderOwners{extensions: cockroachDBRegistry, tableStorage: crdbrender.CreateTableClause}
	default:
		return renderOwners{}
	}
}

// extensionsFor is the handler registry of a target's owners.
func extensionsFor(dialect string) (renderer.Extensions, error) {
	owners := ownersFor(dialect)
	if owners.extensions == nil {
		return renderer.Extensions{}, nil
	}
	return owners.extensions()
}

// postgresOwners is what a PostgreSQL-wire target's owners hand the shared
// renderer, which consumes it without naming any owner.
func postgresOwners(dialect string) (postgres.Owners, error) {
	registry, err := extensionsFor(dialect)
	if err != nil {
		return postgres.Owners{}, err
	}
	return postgres.Owners{Extensions: registry, TableStorage: ownersFor(dialect).tableStorage}, nil
}

func prepareExtensionStatement(dialect string, caps capability.Capabilities, node *ast.ExtensionStatement) (ast.Node, error) {
	registry, err := extensionsFor(dialect)
	if err != nil {
		return nil, err
	}
	payload, err := registry.Prepare(renderer.ExtensionContext{Target: dialect, Capabilities: caps}, ast.StatementExtension, node.Payload)
	if err != nil {
		return nil, err
	}
	return &ast.ExtensionStatement{Payload: payload}, nil
}

func prepareExtensionAlter(dialect string, caps capability.Capabilities, parent *ast.AlterTableNode, node *ast.ExtensionAlterOperation) (ast.AlterOperation, error) {
	if node == nil {
		return nil, nilNodeError(dialect, "extension alter-table operation")
	}
	registry, err := extensionsFor(dialect)
	if err != nil {
		return nil, err
	}
	payload, err := registry.Prepare(renderer.ExtensionContext{Target: dialect, Capabilities: caps, Parent: parent}, ast.AlterExtension, node.Payload)
	if err != nil {
		return nil, err
	}
	return &ast.ExtensionAlterOperation{Payload: payload}, nil
}
