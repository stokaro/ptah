package builtin

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/internal/ydbextensions"
)

// extensionsFor selects owner registrations at the built-in composition
// boundary. Neutral contracts and non-owning backends know no payload types.
func extensionsFor(dialect string) (renderer.Extensions, error) {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		return ydbextensions.Registry()
	}
	if platform.NormalizeDialect(dialect) == platform.ClickHouse {
		return chrender.Registry()
	}
	if platform.NormalizeDialect(dialect) == platform.CockroachDB {
		return crdbrender.Registry()
	}
	return renderer.Extensions{}, nil
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
