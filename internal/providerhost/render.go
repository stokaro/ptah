package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
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
	remote := &protocol.RenderRequest{Engine: t.engine.GetId(), Changes: changes, TargetFeatures: t.engine.GetFeatures(), Capabilities: capabilities}
	// A provider that did not negotiate text units renders as it always does,
	// and a native unit in a text consumer is refused below.
	if request.Form == renderer.FormText && t.negotiated(provider.TextUnitsFeature) {
		remote.Form = protocol.RenderRequest_TEXT
	}
	result, err := t.host.client.Render(ctx, remote)
	if err != nil {
		return renderer.Result{}, err
	}
	if refusal := result.GetRefusal(); refusal != nil {
		return renderer.Result{}, t.textRefusal(request.Nodes, refusal)
	}
	fragments := make([]renderer.Fragment, 0, len(result.GetUnits()))
	for i, unit := range result.GetUnits() {
		fragment, err := t.renderedFragment(changes[i], unit)
		if err != nil {
			return renderer.Result{}, err
		}
		fragment.Inputs, err = t.renderedInputs(ctx, unit)
		if err != nil {
			return renderer.Result{}, err
		}
		fragments = append(fragments, fragment)
	}
	return renderer.Result{Complete: true, Fragments: fragments, ExecutableUnits: true}, nil
}

// textRefusal reports a change the provider performs only through a native
// call, at the node the plan holds, naming the engine and the provider's
// reason. The SDK accepts a refusal only in the text form, naming a change of
// the request.
func (t *target) textRefusal(nodes []ast.Node, refusal *protocol.RenderRefusal) error {
	index, err := strconv.Atoi(refusal.GetChangeId())
	if err != nil || index < 0 || index >= len(nodes) {
		return fmt.Errorf("%w: text refusal names no change of the batch", provider.ErrProtocol)
	}
	return fmt.Errorf("%w: the %s provider has no text form for %s: %s", renderer.ErrNativeSQL, t.engine.GetId(), nodeSummary(nodes[index]), refusal.GetMessage())
}

// nodeSummary names what a plan node changes, for a message: a native
// operation by its kind and subject, a common table change by its table.
func nodeSummary(node ast.Node) string {
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		return payloadSummary(typed.Payload)
	case *ast.AlterTableNode:
		if len(typed.Operations) == 1 {
			if native, ok := typed.Operations[0].(*ast.ExtensionAlterOperation); ok {
				return payloadSummary(native.Payload)
			}
		}
		return tableref.Phrase(typed.Name)
	case *ast.CreateTableNode:
		return tableref.Phrase(typed.Name)
	case *ast.DropTableNode:
		return tableref.Phrase(typed.Name)
	}
	return fmt.Sprintf("a %T", node)
}

func payloadSummary(payload ast.ExtensionPayload) string {
	if payload == nil {
		return "a native operation"
	}
	if source, ok := payload.(schemaext.EffectSource); ok {
		if effect := source.Effect(); effect.Reason != "" {
			return fmt.Sprintf("operation %s (%s)", payload.Kind(), effect.Reason)
		}
	}
	return fmt.Sprintf("operation %s", payload.Kind())
}

// Native execution retains the captured operation, including its semantic
// receipt. A different envelope needs a new owner assessment before it can
// inherit that operation's risk, dependencies, or transaction requirements.
func (t *target) renderedFragment(change *protocol.Change, unit *protocol.Unit) (renderer.Fragment, error) {
	if _, sql := unit.GetBody().(*protocol.Unit_Sql); sql {
		if !strings.HasSuffix(strings.TrimSpace(unit.GetSql()), ";") {
			return renderer.Fragment{}, unsupported("unterminated SQL execution units")
		}
		return renderer.Fragment{SQL: unit.GetSql()}, nil
	}
	captured := change.GetNative().GetValue()
	if captured == nil || unit.GetNative() == nil || !proto.Equal(captured.GetValue(), unit.GetNative()) {
		return renderer.Fragment{}, unsupported("native execution without the captured operation envelope")
	}
	contract := t.host.models[modelKey{kind: schemaext.Kind(unit.GetNative().GetKind()), representation: schemaext.Operation}]
	if contract == nil || contract.engines[t.engine.GetId()] == nil {
		return renderer.Fragment{}, unsupported("native execution without the selected operation model")
	}
	model, err := contract.resolved(modelWire{Format: 1, Engine: t.engine.GetId(), Features: slices.Clone(t.engine.GetFeatures())}, captured)
	if err != nil {
		return renderer.Fragment{}, err
	}
	return renderer.Fragment{Native: &resolvedOperation{model: model}}, nil
}

func (t *target) reference(name string) (*protocol.Reference, error) {
	ref, ok := tableref.Parse(name)
	if !ok {
		return nil, fmt.Errorf("provider table name is not a structural reference")
	}
	return &protocol.Reference{Engine: t.engine.GetId(), Schema: ref.Schema, Name: ref.Name, Kind: "table"}, nil
}

func (t *target) change(node ast.Node) (*protocol.Change, error) {
	if change, common, err := t.commonObjectChange(node); common {
		return change, err
	}
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
