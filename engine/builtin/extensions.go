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
	"ptah.run/dialect/spanner/spannerrender"
	"ptah.run/dialect/timescaledb/tsrender"
	"ptah.run/engine/builtin/internal/dialects/postgres"
	"ptah.run/feature/pgpolicy/policyrender"
	"ptah.run/internal/ydbextensions"
)

// The PostgreSQL-family registries are built once. A registry holds nothing a
// render changes, and the renderer is built per statement, so building it per
// renderer built the same handlers for every statement of a plan.
var (
	postgresFamilyRegistry = sync.OnceValues(func() (renderer.Extensions, error) {
		return renderer.NewExtensions(postgresFamilyHandlers()...)
	})
	cockroachDBRegistry = sync.OnceValues(func() (renderer.Extensions, error) {
		return renderer.NewExtensions(postgresFamilyHandlers(crdbrender.Handlers()...)...)
	})
	spannerRegistry = sync.OnceValues(func() (renderer.Extensions, error) {
		return renderer.NewExtensions(postgresFamilyHandlers(spannerrender.Handlers()...)...)
	})
)

// postgresFamilyHandlers is the owners every PostgreSQL-family target
// composes, TimescaleDB and row security, followed by the target's own.
func postgresFamilyHandlers(own ...renderer.ExtensionHandler) []renderer.ExtensionHandler {
	return append(append(tsrender.Handlers(), policyrender.Handlers()...), own...)
}

// lowerPostgresFamilyFacets lowers the table facets of the owners every
// PostgreSQL-family target composes: the create_hypertable call first, then
// the row-security switches.
func lowerPostgresFamilyFacets(table string, facets schemaext.Facets) ([]ast.ExtensionPayload, schemaext.Facets, error) {
	hypertable, rest, err := tsrender.LowerTableFacets(table, facets)
	if err != nil {
		return nil, schemaext.Facets{}, err
	}
	security, rest, err := policyrender.LowerTableFacets(table, rest)
	if err != nil {
		return nil, schemaext.Facets{}, err
	}
	return append(hypertable, security...), rest, nil
}

// renderOwners is what a target's feature owners contribute to rendering it,
// selected in [ownersFor], the one place at this composition boundary that
// names them.
type renderOwners struct {
	// extensions builds the owners' handler registry; nil means none.
	extensions func() (renderer.Extensions, error)
	// tableStorage renders the clause owned facets add to a CREATE TABLE;
	// nil means none.
	tableStorage func(target string, caps capability.Capabilities, table string, facets schemaext.Facets) (string, error)
	// lowerTableFacets turns the owned facets a CREATE TABLE cannot carry into
	// the statements after it; nil means none.
	lowerTableFacets func(table string, facets schemaext.Facets) ([]ast.ExtensionPayload, schemaext.Facets, error)
}

// ownersFor selects a target's feature owners. Neutral contracts and
// non-owning backends know no payload types. TimescaleDB and row security are
// owners on every PostgreSQL-family target, and their renderers refuse or skip
// what a target without the capability cannot hold; row-level TTL is
// CockroachDB's alone, the row deletion policy Spanner's, and the security
// policy SQL Server's.
func ownersFor(dialect string) renderOwners {
	switch platform.NormalizeDialect(dialect) {
	case platform.YDB:
		return renderOwners{extensions: ydbextensions.Registry}
	case platform.ClickHouse:
		return renderOwners{extensions: chrender.Registry}
	case platform.SQLServer:
		return renderOwners{extensions: mssqlRegistry}
	case platform.CockroachDB:
		return renderOwners{extensions: cockroachDBRegistry, tableStorage: crdbrender.CreateTableClause,
			lowerTableFacets: lowerPostgresFamilyFacets}
	case platform.Spanner:
		return renderOwners{extensions: spannerRegistry, tableStorage: spannerrender.CreateTableClause,
			lowerTableFacets: lowerPostgresFamilyFacets}
	case platform.Postgres, platform.YugabyteDB:
		return renderOwners{extensions: postgresFamilyRegistry, lowerTableFacets: lowerPostgresFamilyFacets}
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
	owners := ownersFor(dialect)
	return postgres.Owners{Extensions: registry, TableStorage: owners.tableStorage, LowerTableFacets: owners.lowerTableFacets}, nil
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
