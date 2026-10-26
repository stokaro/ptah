package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

type operation struct {
	Action      string        `json:"action"`
	Transaction string        `json:"transaction,omitempty"`
	To          *string       `json:"to,omitempty"`
	Column      *addedColumn  `json:"column,omitempty"`
	DropColumn  string        `json:"dropColumn,omitempty"`
	Rebuild     *rebuiltTable `json:"rebuild,omitempty"`
}

// rebuiltTable is the table a rebuild writes: every column of the
// declaration, and the columns whose rows it copies from the table it
// replaces.
type rebuiltTable struct {
	Columns []rebuiltColumn `json:"columns"`
	Copy    []string        `json:"copy"`
}

type rebuiltColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primaryKey"`
}

func (t *rebuiltTable) validate() error {
	if t == nil || len(t.Columns) == 0 {
		return fmt.Errorf("rebuild requires the columns of its table")
	}
	declared := make(map[string]bool, len(t.Columns))
	for _, column := range t.Columns {
		if column.Name == "" || strings.ContainsRune(column.Name, 0) || declared[column.Name] || (column.PrimaryKey && column.Nullable) {
			return fmt.Errorf("rebuild requires distinct, complete columns")
		}
		if err := (&addedColumn{Name: column.Name, Type: column.Type, Nullable: new(column.Nullable)}).validate(); err != nil {
			return err
		}
		declared[column.Name] = true
	}
	for _, name := range t.Copy {
		if !declared[name] {
			return fmt.Errorf("rebuild copies only columns it declares")
		}
	}
	return nil
}

type addedColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable *bool  `json:"nullable"`
}

func (col *addedColumn) validate() error {
	if col == nil || col.Name == "" || strings.ContainsRune(col.Name, 0) || col.Nullable == nil {
		return fmt.Errorf("add-column requires a complete column")
	}
	switch col.Type {
	case "INTEGER", "TEXT", "REAL", "BLOB":
		return nil
	default:
		return fmt.Errorf("unsupported added column type")
	}
}

func normalize(input *protocol.ValueInput) (*protocol.NormalizedValue, error) {
	if input.GetRepresentation() != protocol.ValueInput_OPERATION || input.GetValue().GetKind() != operationKind {
		return nil, fmt.Errorf("unknown operation")
	}
	canonical, err := modeljson.Canonical(input.GetValue().GetPayload())
	if err != nil {
		return nil, err
	}
	var op operation
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&op); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil {
		return nil, err
	}
	if _, present := fields["to"]; present && op.Action != "rename" {
		return nil, fmt.Errorf("only rename accepts a target")
	}
	if op.Transaction != "" && op.Transaction != "required" {
		return nil, fmt.Errorf("unknown transaction requirement")
	}
	if _, present := fields["column"]; present && op.Action != "add-column" {
		return nil, fmt.Errorf("only add-column accepts a column")
	}
	if _, present := fields["dropColumn"]; present && op.Action != "drop-column" {
		return nil, fmt.Errorf("only drop-column accepts a column name")
	}
	if _, present := fields["rebuild"]; present && op.Action != "rebuild" {
		return nil, fmt.Errorf("only rebuild accepts a table")
	}
	impact := protocol.ChangeSemantics_ADDITIVE
	switch op.Action {
	case "rebuild":
		if err := op.Rebuild.validate(); err != nil {
			return nil, err
		}
		// A rebuild copies the rows and swaps the tables in steps a
		// concurrent write can fall between.
		impact = protocol.ChangeSemantics_BEHAVIORAL
	case "drop-column":
		if op.DropColumn == "" || strings.ContainsRune(op.DropColumn, 0) {
			return nil, fmt.Errorf("drop-column requires a column name")
		}
		impact = protocol.ChangeSemantics_DESTRUCTIVE
	case "add-column":
		if err := op.Column.validate(); err != nil {
			return nil, err
		}
	case "create":
		if op.To != nil {
			return nil, fmt.Errorf("create cannot rename")
		}
	case "drop":
		if op.To != nil {
			return nil, fmt.Errorf("drop cannot rename")
		}
		impact = protocol.ChangeSemantics_DESTRUCTIVE
	case "rename":
		if op.To == nil || *op.To == "" || strings.ContainsRune(*op.To, 0) {
			return nil, fmt.Errorf("rename requires a target name")
		}
		impact = protocol.ChangeSemantics_BEHAVIORAL
	default:
		return nil, fmt.Errorf("unknown action")
	}
	ref := input.GetValue().GetOwner()
	if ref.GetKind() != "table" || ref.GetName() == "" || ref.GetDatabase() != "" || ref.GetCatalog() != "" ||
		len(ref.GetPath()) != 0 || len(ref.GetOverload()) != 0 || (ref.GetSchema() != "" && ref.GetSchema() != "main") || strings.ContainsRune(ref.GetName(), 0) {
		return nil, fmt.Errorf("fixture operation requires a common table")
	}
	value := proto.CloneOf(input.GetValue())
	value.Payload = canonical
	return &protocol.NormalizedValue{Id: input.GetId(), Value: value, NormalizedOwner: proto.CloneOf(ref),
		CanonicalFormat: "table-operation-v1", CanonicalProjection: bytes.Clone(canonical),
		OperationSemantics: &protocol.OperationSemantics{Impact: impact, Reason: "Table " + op.Action, Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "The operation changes no grants"}}, nil
}

func (p *owner) NormalizeValues(ctx context.Context, r *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range r.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input.GetRepresentation() == protocol.ValueInput_DESIRED && input.GetValue().GetKind() == declarationKind {
			value, err := normalizeDeclaration(input)
			if err != nil { return nil, err }
			result.Values = append(result.Values, value)
			continue
		}
		if input.GetRepresentation() == protocol.ValueInput_CHANGE {
			value, err := normalizeChange(input)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		if input.GetRepresentation() == protocol.ValueInput_OPERATION {
			value, err := normalize(input)
			if err != nil {
				return nil, err
			}
			result.Values = append(result.Values, value)
			continue
		}
		values, err := p.Provider.NormalizeValues(ctx, &protocol.NormalizeValuesRequest{Engine: r.GetEngine(), TargetFeatures: r.GetTargetFeatures(), Values: []*protocol.ValueInput{input}})
		if err != nil {
			return nil, err
		}
		result.Values = append(result.Values, values.GetValues()...)
	}
	return result, nil
}

// Render writes each change as one unit. In the text form every unit is one
// statement the fixture's analyzer reads and its executor runs read back from
// a file; an operation whose SQL is not such a statement, a rebuild or a
// rename, refuses the batch.
func (p *owner) Render(ctx context.Context, r *protocol.RenderRequest) (*protocol.RenderResult, error) {
	text := r.GetForm() == protocol.RenderRequest_TEXT
	result := &protocol.RenderResult{Complete: true}
	for _, change := range r.GetChanges() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if change.GetNative() == nil {
			rendered, err := p.Provider.Render(ctx, &protocol.RenderRequest{Engine: r.GetEngine(), TargetFeatures: r.GetTargetFeatures(), Changes: []*protocol.Change{change}})
			if err != nil {
				return nil, err
			}
			result.Units = append(result.Units, rendered.GetUnits()...)
			continue
		}
		transactional, known := r.GetCapabilities()["transactional_ddl"]
		if !known || transactional != p.transactionalDDL {
			return nil, fmt.Errorf("fixture requires its selected transactional DDL profile")
		}
		sql, err := renderNative(change.GetNative())
		if err != nil {
			return nil, err
		}
		if text {
			if analysis, err := p.AnalyzeSQL(ctx, &protocol.AnalyzeSQLRequest{Engine: r.GetEngine(), Sources: []string{sql}}); err != nil || len(analysis.GetFiles()[0].GetStatements()) != 1 {
				return &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: change.GetId(), Reason: protocol.RenderRefusal_NO_TEXT_FORM,
					Message: "the fixture runs a table " + operationAction(change.GetNative()) + " only as a native operation"}}, nil
			}
		}
		if !text && (p.nativeUnits || p.rebuilds && rebuildOperation(change.GetNative())) {
			result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Native{Native: proto.CloneOf(change.GetNative().GetValue().GetValue())}})
		} else {
			result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: sql}})
		}
	}
	return result, nil
}

func renderNative(native *protocol.NativeOperation) (string, error) {
	captured := native.GetValue()
	value, err := normalize(&protocol.ValueInput{Id: captured.GetId(), Representation: protocol.ValueInput_OPERATION, Value: captured.GetValue()})
	if err != nil {
		return "", err
	}
	if !proto.Equal(value, captured) {
		return "", fmt.Errorf("operation semantics changed")
	}
	var op operation
	if err := json.Unmarshal(value.GetValue().GetPayload(), &op); err != nil {
		return "", err
	}
	subject := value.GetValue().GetOwner()
	name := quote(subject.GetName())
	switch op.Action {
	case "add-column":
		if native.GetRole() != protocol.NativeOperation_ALTER_TABLE || !proto.Equal(native.GetParent(), subject) {
			return "", fmt.Errorf("add-column requires its actual table parent")
		}
		sql := "ALTER TABLE " + name + " ADD COLUMN " + quote(op.Column.Name) + " " + op.Column.Type
		if !*op.Column.Nullable {
			sql += " NOT NULL"
		}
		return sql + ";", nil
	case "drop-column":
		if native.GetRole() != protocol.NativeOperation_ALTER_TABLE || !proto.Equal(native.GetParent(), subject) {
			return "", fmt.Errorf("drop-column requires its actual table parent")
		}
		return "ALTER TABLE " + name + " DROP COLUMN " + quote(op.DropColumn) + ";", nil
	case "rename":
		if native.GetRole() != protocol.NativeOperation_ALTER_TABLE || !proto.Equal(native.GetParent(), subject) {
			return "", fmt.Errorf("rename requires its actual table parent")
		}
		return "ALTER TABLE " + name + " RENAME TO " + quote(*op.To) + ";", nil
	case "rebuild":
		if native.GetRole() != protocol.NativeOperation_STATEMENT {
			return "", fmt.Errorf("standalone table operation required")
		}
		return renderRebuild(subject.GetName(), op.Rebuild), nil
	case "create", "drop":
		if native.GetRole() != protocol.NativeOperation_STATEMENT {
			return "", fmt.Errorf("standalone table operation required")
		}
		if op.Action == "create" {
			return "CREATE TABLE " + name + ` ("id" INTEGER NOT NULL PRIMARY KEY);`, nil
		}
		return "DROP TABLE " + name + ";", nil
	default:
		return "", fmt.Errorf("unknown action")
	}
}

// rebuildOperation reports whether native rebuilds a table. A rebuild runs as
// a native unit: its statements are the fixture's own, which only its
// executor reads.
func rebuildOperation(native *protocol.NativeOperation) bool {
	return operationAction(native) == "rebuild"
}

// operationAction is the action a native table operation performs, or empty
// when its payload is not one.
func operationAction(native *protocol.NativeOperation) string {
	var op operation
	if json.Unmarshal(native.GetValue().GetValue().GetPayload(), &op) != nil {
		return ""
	}
	return op.Action
}

// renderRebuild writes the new table under a scratch name, copies the kept
// columns, drops the old table and gives the new one its name. SQLite keeps
// the definition with the new name quoted, which is the text the fixture's
// inspection reconstructs.
func renderRebuild(table string, rebuild *rebuiltTable) string {
	scratch := table + "__rebuild"
	columns := make([]string, len(rebuild.Columns))
	for i, column := range rebuild.Columns {
		columns[i] = quote(column.Name) + " " + column.Type
		if !column.Nullable {
			columns[i] += " NOT NULL"
		}
		if column.PrimaryKey {
			columns[i] += " PRIMARY KEY"
		}
	}
	copied := make([]string, len(rebuild.Copy))
	for i, name := range rebuild.Copy {
		copied[i] = quote(name)
	}
	statements := []string{"CREATE TABLE " + quote(scratch) + " (" + strings.Join(columns, ", ") + ");"}
	if len(copied) > 0 {
		list := strings.Join(copied, ", ")
		statements = append(statements, "INSERT INTO "+quote(scratch)+" ("+list+") SELECT "+list+" FROM "+quote(table)+";")
	}
	statements = append(statements, "DROP TABLE "+quote(table)+";", "ALTER TABLE "+quote(scratch)+" RENAME TO "+quote(table)+";")
	return strings.Join(statements, " ")
}

func quote(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
