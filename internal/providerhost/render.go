package providerhost

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/internal/tableref"
)

func (t *target) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	if request.Target != t.engine.GetId() {
		return renderer.Result{}, fmt.Errorf("provider rendering target changed")
	}
	if err := request.Capabilities.Validate(); err != nil {
		return renderer.Result{}, err
	}
	capabilities := make(map[string]bool)
	for name, value := range request.Capabilities.All() {
		capabilities[string(name)] = value
	}
	changes := make([]*protocol.Change, 0, len(request.Nodes))
	for i, node := range request.Nodes {
		change, err := t.change(node)
		if err != nil {
			return renderer.Result{}, err
		}
		change.Id = strconv.Itoa(i)
		if native := change.GetNative(); native != nil {
			native.Value.Id = change.Id
		}
		changes = append(changes, change)
	}
	if len(changes) == 0 {
		return renderer.Result{Complete: true}, nil
	}
	result, err := t.host.client.Render(ctx, &protocol.RenderRequest{Engine: t.engine.GetId(), Changes: changes, TargetFeatures: t.engine.GetFeatures(), Capabilities: capabilities})
	if err != nil {
		return renderer.Result{}, err
	}
	fragments := make([]string, 0, len(result.GetUnits()))
	for _, unit := range result.GetUnits() {
		if _, sql := unit.GetBody().(*protocol.Unit_Sql); !sql {
			return renderer.Result{}, unsupported("native execution units")
		}
		if !strings.HasSuffix(strings.TrimSpace(unit.GetSql()), ";") {
			return renderer.Result{}, unsupported("unterminated SQL execution units")
		}
		fragments = append(fragments, unit.GetSql())
	}
	return renderer.Result{Complete: true, Fragments: fragments, ExecutableUnits: true}, nil
}

func (t *target) reference(name string) (*protocol.Reference, error) {
	ref, ok := tableref.Parse(name)
	if !ok {
		return nil, fmt.Errorf("provider table name is not a structural reference")
	}
	return &protocol.Reference{Engine: t.engine.GetId(), Schema: ref.Schema, Name: ref.Name, Kind: "table"}, nil
}

func (t *target) change(node ast.Node) (*protocol.Change, error) {
	switch node := node.(type) {
	case *ast.ExtensionStatement:
		if node == nil {
			return nil, unsupported("nil native statement")
		}
		return t.nativeOperation(node.Payload, protocol.NativeOperation_STATEMENT, nil)
	case *ast.CreateTableNode:
		if err := t.requireCommonTables(); err != nil {
			return nil, err
		}
		if node == nil {
			return nil, unsupported("nil table nodes")
		}
		if err := onlyFields(*node, "Name", "Columns"); err != nil {
			return nil, err
		}
		ref, err := t.reference(node.Name)
		if err != nil {
			return nil, err
		}
		table := &protocol.Table{Identity: ref}
		for _, column := range node.Columns {
			converted, err := wireColumn(column)
			if err != nil {
				return nil, err
			}
			table.Columns = append(table.Columns, converted)
		}
		return &protocol.Change{Object: ref, Operation: &protocol.Change_CreateTable{CreateTable: table}}, nil
	case *ast.DropTableNode:
		if err := t.requireCommonTables(); err != nil {
			return nil, err
		}
		if node == nil {
			return nil, unsupported("nil table nodes")
		}
		if err := onlyFields(*node, "Name"); err != nil {
			return nil, err
		}
		ref, err := t.reference(node.Name)
		if err != nil {
			return nil, err
		}
		return &protocol.Change{Object: ref, Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}}, nil
	case *ast.AlterTableNode:
		if node == nil {
			return nil, unsupported("nil table nodes")
		}
		return t.alter(node)
	default:
		return nil, unsupported(fmt.Sprintf("AST node %T", node))
	}
}

func (t *target) alter(node *ast.AlterTableNode) (*protocol.Change, error) {
	if err := onlyFields(*node, "Name", "Operations"); err != nil {
		return nil, err
	}
	if len(node.Operations) != 1 {
		return nil, unsupported("combined table alterations")
	}
	ref, err := t.reference(node.Name)
	if err != nil {
		return nil, err
	}
	change := &protocol.Change{Object: ref}
	switch operation := node.Operations[0].(type) {
	case *ast.ExtensionAlterOperation:
		if operation == nil {
			return nil, unsupported("nil native alteration")
		}
		return t.nativeOperation(operation.Payload, protocol.NativeOperation_ALTER_TABLE, ref)
	case *ast.AddColumnOperation:
		if err := t.requireCommonTables(); err != nil {
			return nil, err
		}
		if operation == nil {
			return nil, unsupported("nil alteration")
		}
		if err := onlyFields(*operation, "Column"); err != nil {
			return nil, err
		}
		column, err := wireColumn(operation.Column)
		if err != nil {
			return nil, err
		}
		change.Operation = &protocol.Change_AddColumn{AddColumn: column}
	case *ast.DropColumnOperation:
		if err := t.requireCommonTables(); err != nil {
			return nil, err
		}
		if operation == nil {
			return nil, unsupported("nil alteration")
		}
		if err := onlyFields(*operation, "ColumnName"); err != nil {
			return nil, err
		}
		change.Operation = &protocol.Change_DropColumn{DropColumn: operation.ColumnName}
	default:
		return nil, unsupported(fmt.Sprintf("table alteration %T", operation))
	}
	return change, nil
}

func wireColumn(column *ast.ColumnNode) (*protocol.Column, error) {
	if column == nil {
		return nil, unsupported("nil columns")
	}
	if err := onlyFields(*column, "Name", "Type", "Nullable", "Primary"); err != nil {
		return nil, err
	}
	return &protocol.Column{Name: column.Name, Type: column.Type, Nullable: column.Nullable, PrimaryKey: column.Primary}, nil
}
