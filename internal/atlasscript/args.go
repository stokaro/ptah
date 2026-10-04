package atlasscript

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

// Column is one column a keyset iterator carries: its name in the result set
// and the type the script declared for it.
type Column struct {
	Name string
	// Type is the declared type name: int, number, string or bool.
	Type string
}

// columnTypes are the type identifiers a cursor or batch column takes.
var columnTypes = map[string]cty.Type{
	"int":    cty.Number,
	"number": cty.Number,
	"string": cty.String,
	"bool":   cty.Bool,
}

// argFunctions are the functions an args element may call: jsonencode turns
// the page into the one value a placeholder takes, and length counts it.
var argFunctions = map[string]function.Function{
	"jsonencode": stdlib.JSONEncodeFunc,
	"length":     stdlib.LengthFunc,
}

// argScope says where an args list is written, which decides what its elements
// may read. The names follow lexical scope: the iterator's next query reads the
// cursor as cursor.<col>, and a loop's do body reads the page through
// iterator.keyset.
type argScope int

const (
	// scopeConstant is an exec or query script's step, or the iterator's init
	// query: nothing has been read yet, so an element is a constant.
	scopeConstant argScope = iota
	// scopeNext is the iterator's next query, which reads the cursor.
	scopeNext
	// scopeBody is a step inside a loop's do body.
	scopeBody
)

// pageShape is what a scope can read, as declared: the cursor and batch
// columns of the loop's iterator.
type pageShape struct {
	cursor []Column
	batch  []Column
}

// parseColumns reads a cursor or batch block's columns in source order.
//
// The order matters for the report only; values are matched to the result set
// by name. Each value is a bare type identifier, and anything else is refused,
// because a type nobody declared is a conversion nobody can check.
func parseColumns(block *hclsyntax.Block) ([]Column, error) {
	columns := make([]Column, 0, len(block.Body.Attributes))
	for _, name := range attributeNamesInSourceOrder(block) {
		attr := block.Body.Attributes[name]
		traversal, ok := attr.Expr.(*hclsyntax.ScopeTraversalExpr)
		typeName := ""
		if ok && len(traversal.Traversal) == 1 {
			typeName = traversal.Traversal.RootName()
		}
		if _, known := columnTypes[typeName]; !known {
			return nil, &ParseError{Range: attr.SrcRange, Message: fmt.Sprintf(
				"%s column %s needs a type: int, number, string or bool", block.Type, name)}
		}
		columns = append(columns, Column{Name: name, Type: typeName})
	}
	return columns, nil
}

// parseArgs reads an args list and checks each element against what its scope
// can read.
//
// An element is evaluated here against the declared shape, with every value
// read from the database standing in as an unknown of its declared type. That
// is what turns a misspelled column, a name from the wrong scope or a list
// bound to one placeholder into a refusal before the script touches a
// database, rather than into a value bound as an empty string.
func parseArgs(attr *hclsyntax.Attribute, scope argScope, shape pageShape) ([]hcl.Expression, error) {
	list, ok := attr.Expr.(*hclsyntax.TupleConsExpr)
	if !ok {
		return nil, &ParseError{Range: attr.SrcRange, Message: "args must be a list"}
	}
	evaluation := shape.context(scope)
	args := make([]hcl.Expression, 0, len(list.Exprs))
	for _, expr := range list.Exprs {
		if err := checkReferences(expr, scope); err != nil {
			return nil, err
		}
		value, diags := expr.Value(evaluation)
		if diags.HasErrors() {
			return nil, &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
				"args element %s: %s", exprText(expr), diagnosticText(diags))}
		}
		if value.IsKnown() && value.IsNull() {
			return nil, &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
				"args element %s binds no value: write the constant, or read a column the page carries",
				exprText(expr))}
		}
		if !bindable(value.Type()) {
			return nil, &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
				"args element %s is %s, and a placeholder takes one value: bind it through jsonencode(...)",
				exprText(expr), value.Type().FriendlyName())}
		}
		args = append(args, expr)
	}
	return args, nil
}

// checkReferences refuses a reference its scope does not hold, naming the
// spelling that scope does read.
//
// HCL would report "There is no variable named cursor" for the commonest
// mistake, a do body writing the bare cursor.<col> the iterator's next query
// uses. That is accurate and does not help, so the refusal says which name
// reads it from where it was written.
func checkReferences(expr hclsyntax.Expression, scope argScope) error {
	for _, traversal := range expr.Variables() {
		root := traversal.RootName()
		text := traversalText(traversal)
		switch scope {
		case scopeConstant:
			return &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
				"args element %s is a reference, and nothing here has been read for it to name: "+
					"only a loop's do body reads iterator.keyset and self, and only the iterator's next query reads cursor",
				text)}
		case scopeNext:
			if root != "cursor" {
				return &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
					"args element %s names nothing the next query reads: it reads the last row of the previous page as cursor.<col>",
					text)}
			}
		case scopeBody:
			if root == "cursor" {
				return &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
					"args element %s names the cursor the way only the iterator's next query does: "+
						"inside do it is iterator.keyset.%s, the last row of the page",
					text, text)}
			}
			if root != "iterator" && root != "self" {
				return &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
					"args element %s names nothing a do body reads: "+
						"it reads iterator.keyset.cursor.<col>, iterator.keyset.batch[*].<col> and self.index",
					text)}
			}
		}
	}
	return nil
}

// bindable reports whether a value of type t fills one placeholder.
func bindable(t cty.Type) bool {
	return t == cty.DynamicPseudoType || t.IsPrimitiveType()
}

// context is the evaluation context parseArgs checks a scope's elements in:
// the declared shape, with unknowns where rows will be.
func (p pageShape) context(scope argScope) *hcl.EvalContext {
	cursor := unknownRow(p.cursor)
	switch scope {
	case scopeNext:
		return &hcl.EvalContext{
			Variables: map[string]cty.Value{"cursor": cursor},
			Functions: argFunctions,
		}
	case scopeBody:
		// One row stands for the page: a splat over it checks the column
		// names, which an unknown list would not.
		batch := cty.ListVal([]cty.Value{unknownRow(p.batchColumns())})
		return bodyContext(cursor, batch, cty.UnknownVal(cty.Number))
	default:
		return &hcl.EvalContext{Functions: argFunctions}
	}
}

// batchColumns is the page a do body reads, which defaults to the cursor.
func (p pageShape) batchColumns() []Column {
	if len(p.batch) > 0 {
		return p.batch
	}
	return p.cursor
}

// bodyContext is what a do body reads for one page.
func bodyContext(cursor, batch, index cty.Value) *hcl.EvalContext {
	return &hcl.EvalContext{
		Variables: map[string]cty.Value{
			"iterator": cty.ObjectVal(map[string]cty.Value{
				"keyset": cty.ObjectVal(map[string]cty.Value{
					"cursor": cursor,
					"batch":  batch,
				}),
			}),
			"self": cty.ObjectVal(map[string]cty.Value{"index": index}),
		},
		Functions: argFunctions,
	}
}

func unknownRow(columns []Column) cty.Value {
	attributes := make(map[string]cty.Value, len(columns))
	for _, column := range columns {
		attributes[column.Name] = cty.UnknownVal(columnTypes[column.Type])
	}
	return cty.ObjectVal(attributes)
}

// constantContext evaluates the args of a scope that reads nothing.
func constantContext() *hcl.EvalContext {
	return &hcl.EvalContext{Functions: argFunctions}
}

// bindArgs evaluates args in evaluation and returns the values a driver binds.
func bindArgs(args []hcl.Expression, evaluation *hcl.EvalContext) ([]any, error) {
	bound := make([]any, 0, len(args))
	for _, expr := range args {
		value, diags := expr.Value(evaluation)
		if diags.HasErrors() {
			return nil, fmt.Errorf("args element %s: %s", exprText(expr), diagnosticText(diags))
		}
		goValue, err := goValueOf(value)
		if err != nil {
			return nil, fmt.Errorf("args element %s: %w", exprText(expr), err)
		}
		bound = append(bound, goValue)
	}
	return bound, nil
}

// goValueOf is the driver value for one evaluated element.
//
// A whole number binds as an int64 and any other number as a float64, a
// string as a string and a bool as a bool. Binding the text of each, as a
// string, is what an engine that types its parameters refuses: YDB answers a
// string bound to an Int64 column with a type error. A null read from the
// database binds as SQL NULL.
func goValueOf(value cty.Value) (any, error) {
	if value.IsNull() {
		return nil, nil
	}
	if !value.IsKnown() {
		return nil, errors.New("the value is not known")
	}
	switch value.Type() {
	case cty.String:
		return value.AsString(), nil
	case cty.Bool:
		return value.True(), nil
	case cty.Number:
		number := value.AsBigFloat()
		if number.IsInt() {
			if whole, accuracy := number.Int64(); accuracy == big.Exact {
				return whole, nil
			}
		}
		float, _ := number.Float64()
		return float, nil
	default:
		return nil, fmt.Errorf("it is %s, and a placeholder takes one value", value.Type().FriendlyName())
	}
}

// rowValue reads the named columns of one scanned row into an object, each
// converted to its declared type.
//
// The columns are matched by name, as the result set reports them, because a
// SELECT that lists its columns in another order than the cursor would
// otherwise carry the wrong value forward. An exact name wins; failing that,
// one name that differs only in case is taken, since Oracle reports an
// unquoted alias in upper case.
func rowValue(columns []Column, names []string, row []any) (cty.Value, error) {
	attributes := make(map[string]cty.Value, len(columns))
	for _, column := range columns {
		position, err := columnPosition(names, column.Name)
		if err != nil {
			return cty.NilVal, err
		}
		value, err := ctyValueOf(row[position], column)
		if err != nil {
			return cty.NilVal, err
		}
		attributes[column.Name] = value
	}
	return cty.ObjectVal(attributes), nil
}

func columnPosition(names []string, name string) (int, error) {
	folded := -1
	for position, candidate := range names {
		if candidate == name {
			return position, nil
		}
		if strings.EqualFold(candidate, name) {
			if folded >= 0 {
				return -1, fmt.Errorf("column %q matches two columns of the query result by case", name)
			}
			folded = position
		}
	}
	if folded < 0 {
		return -1, fmt.Errorf("column %q not found in query result", name)
	}
	return folded, nil
}

// ctyValueOf converts one scanned value to the column's declared type.
//
// Drivers disagree about how a value comes back -- an integer as int64 or as
// the bytes of its text, a boolean as a bool or as an int64 -- so the reading
// is over those shapes. A value that does not read as the declared type is an
// error naming the column, rather than a guess carried into the next query.
func ctyValueOf(value any, column Column) (cty.Value, error) {
	declared := columnTypes[column.Type]
	if value == nil {
		return cty.NullVal(declared), nil
	}
	var converted cty.Value
	var err error
	switch column.Type {
	case "int", "number":
		converted, err = numberValue(value)
		if err == nil && column.Type == "int" && !converted.AsBigFloat().IsInt() {
			err = fmt.Errorf("%s is not a whole number", converted.AsBigFloat().Text('g', -1))
		}
	case "bool":
		converted, err = boolValue(value)
	default:
		converted = cty.StringVal(textOf(value))
	}
	if err != nil {
		return cty.NilVal, fmt.Errorf("column %q is declared %s: %w", column.Name, column.Type, err)
	}
	return converted, nil
}

func numberValue(value any) (cty.Value, error) {
	switch typed := value.(type) {
	case int64:
		return cty.NumberIntVal(typed), nil
	case int32:
		return cty.NumberIntVal(int64(typed)), nil
	case int:
		return cty.NumberIntVal(int64(typed)), nil
	case uint64:
		return cty.NumberUIntVal(typed), nil
	case float64:
		return cty.NumberFloatVal(typed), nil
	case float32:
		return cty.NumberFloatVal(float64(typed)), nil
	case []byte, string:
		parsed, err := cty.ParseNumberVal(strings.TrimSpace(textOf(typed)))
		if err != nil {
			return cty.NilVal, fmt.Errorf("%q is not a number", textOf(typed))
		}
		return parsed, nil
	default:
		return cty.NilVal, fmt.Errorf("a %T is not a number", value)
	}
}

func boolValue(value any) (cty.Value, error) {
	switch typed := value.(type) {
	case bool:
		return cty.BoolVal(typed), nil
	case int64:
		return cty.BoolVal(typed != 0), nil
	case []byte, string:
		text := strings.TrimSpace(textOf(typed))
		switch text {
		case "t", "f":
			return cty.BoolVal(text == "t"), nil
		}
		parsed, err := strconv.ParseBool(text)
		if err != nil {
			return cty.NilVal, fmt.Errorf("%q is not a boolean", text)
		}
		return cty.BoolVal(parsed), nil
	default:
		return cty.NilVal, fmt.Errorf("a %T is not a boolean", value)
	}
}

// textOf is a scanned value as text. A time is written in RFC 3339, the
// spelling every engine Ptah drives reads back as a timestamp.
func textOf(value any) string {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case string:
		return typed
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(typed)
	}
}

// exprText spells an element the way it was written, for a refusal.
func exprText(expr hcl.Expression) string {
	syntax, ok := expr.(hclsyntax.Expression)
	if !ok {
		return "?"
	}
	switch typed := syntax.(type) {
	case *hclsyntax.ScopeTraversalExpr:
		return traversalText(typed.Traversal)
	case *hclsyntax.SplatExpr:
		each := ""
		if relative, ok := typed.Each.(*hclsyntax.RelativeTraversalExpr); ok {
			each = traversalText(relative.Traversal)
		}
		return exprText(typed.Source) + "[*]" + each
	case *hclsyntax.FunctionCallExpr:
		return typed.Name + "(...)"
	case *hclsyntax.LiteralValueExpr:
		return literalText(typed.Val)
	}
	variables := syntax.Variables()
	if len(variables) > 0 {
		return traversalText(variables[0])
	}
	return fmt.Sprintf("at column %d", syntax.Range().Start.Column)
}

// literalText spells a literal value the way HCL writes it.
func literalText(value cty.Value) string {
	switch {
	case value.IsNull():
		return "null"
	case value.Type() == cty.String:
		return strconv.Quote(value.AsString())
	case value.Type() == cty.Number:
		return value.AsBigFloat().Text('g', -1)
	case value.Type() == cty.Bool:
		return strconv.FormatBool(value.True())
	default:
		return value.Type().FriendlyName()
	}
}

func diagnosticText(diags hcl.Diagnostics) string {
	for _, diag := range diags {
		if diag.Severity != hcl.DiagError {
			continue
		}
		if diag.Detail != "" {
			return diag.Detail
		}
		return diag.Summary
	}
	return diags.Error()
}
