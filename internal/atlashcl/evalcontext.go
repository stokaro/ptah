package atlashcl

import (
	"encoding/csv"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/ext/tryfunc"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	ctyyaml "github.com/zclconf/go-cty-yaml"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

const (
	variableBlockType = "variable"
	localsBlockType   = "locals"
)

// varNamespace and localNamespace are the two root names an expression can use
// to reach the evaluation context. Everything else a schema file writes as a
// traversal -- schema.x, column.c, enum.status, a bare type keyword -- is a
// reference this package resolves from source text, not a value.
const (
	varNamespace   = "var"
	localNamespace = "local"
)

// newEvalContext builds the evaluation context an HCL schema file is parsed
// against: the `var.` namespace from its `variable` blocks, the `local.`
// namespace from its `locals` blocks, and the function set.
//
// Before this existed every attribute was evaluated against a nil context, so
// `var.status` could not resolve and the raw source text "var.status" was
// written into the schema IR and out as DDL -- issue #926.
func newEvalContext(
	body *hclsyntax.Body,
	vars []string,
	varValues map[string]string,
	printLine func(string),
) (*hcl.EvalContext, error) {
	overrides, err := parseVarOverrides(vars)
	if err != nil {
		return nil, err
	}
	// Applied after the flag text, per [Options.VarValues]: a decoded value is
	// the more specific statement of the two, and it must not be appended to an
	// existing entry the way a repeated --var occurrence is.
	for name, value := range varValues {
		overrides[name] = cty.StringVal(value)
	}
	ctx := &hcl.EvalContext{
		Variables: make(map[string]cty.Value),
		Functions: schemaFunctions(printLine),
	}
	if err := bindVariables(ctx, body, overrides); err != nil {
		return nil, err
	}
	return bindLocals(ctx, body)
}

// parseVarOverrides turns --var values into the override map.
//
// One occurrence carries comma-separated assignments, matching the pinned Atlas
// community binary v1.3.0: `--var a=1,b=2` sets both. Repeating the flag for one
// name collects a list, which then fails the variable's declared scalar type the
// way that binary fails it ("variable \"v\": string required").
func parseVarOverrides(vars []string) (map[string]cty.Value, error) {
	overrides := make(map[string]cty.Value)
	for _, raw := range vars {
		values, err := csv.NewReader(strings.NewReader(raw)).Read()
		if err != nil {
			return nil, fmt.Errorf("parse --var %q: %w", raw, err)
		}
		for _, assignment := range values {
			if err := applyVarOverride(overrides, assignment); err != nil {
				return nil, err
			}
		}
	}
	return overrides, nil
}

func applyVarOverride(overrides map[string]cty.Value, assignment string) error {
	name, text, ok := strings.Cut(assignment, "=")
	if !ok {
		return fmt.Errorf("--var must use name=value, got %q", assignment)
	}
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("--var %q has an empty name", assignment)
	}
	value := cty.StringVal(text)
	if existing, found := overrides[name]; found {
		value = appendVarValue(existing, value)
	}
	overrides[name] = value
	return nil
}

func appendVarValue(existing, value cty.Value) cty.Value {
	if existing.Type().IsListType() {
		return cty.ListVal(append(existing.AsValueSlice(), value))
	}
	return cty.ListVal([]cty.Value{existing, value})
}

// schemaVariable is one parsed top-level `variable` block.
type schemaVariable struct {
	name       string
	typ        cty.Type
	defValue   cty.Value
	hasDefault bool
}

func bindVariables(ctx *hcl.EvalContext, body *hclsyntax.Body, overrides map[string]cty.Value) error {
	values := make(map[string]cty.Value)
	for _, block := range body.Blocks {
		if block.Type != variableBlockType {
			continue
		}
		variable, err := parseVariableBlock(ctx, block)
		if err != nil {
			return err
		}
		value, err := variableValue(variable, overrides)
		if err != nil {
			return err
		}
		// Last declaration wins, with no duplicate diagnostic: measured on the
		// pinned binary, two `variable "v"` blocks parse at exit 0 and the
		// second block's default is the one that reaches the DDL.
		values[variable.name] = value
	}
	if len(values) > 0 {
		ctx.Variables[varNamespace] = cty.ObjectVal(values)
	}
	return nil
}

// variableAttrs are the attributes a schema-file `variable` block accepts.
//
// `sensitive` is deliberately absent. It is valid in an atlas.hcl PROJECT file
// but the pinned binary refuses it in a schema file ("An argument named
// \"sensitive\" is not expected here"), so accepting it here would be a
// looser-than-oracle break in a spot no schema needs.
var variableAttrs = []string{"default", "description", "type"}

func parseVariableBlock(ctx *hcl.EvalContext, block *hclsyntax.Block) (schemaVariable, error) {
	if len(block.Labels) != 1 {
		return schemaVariable{}, fmt.Errorf(
			"parse HCL schema at %s: variable block requires exactly one label",
			block.TypeRange.String(),
		)
	}
	if len(block.Body.Blocks) > 0 {
		nested := block.Body.Blocks[0]
		return schemaVariable{}, fmt.Errorf(
			"parse HCL schema at %s: unsupported variable block %q",
			nested.TypeRange.String(), nested.Type,
		)
	}
	for _, name := range sortedAttrNames(block.Body.Attributes) {
		if !slices.Contains(variableAttrs, name) {
			return schemaVariable{}, fmt.Errorf(
				"parse HCL schema at %s: unsupported variable attribute %q",
				block.Body.Attributes[name].NameRange.String(), name,
			)
		}
	}
	return variableFromAttrs(ctx, block)
}

func variableFromAttrs(ctx *hcl.EvalContext, block *hclsyntax.Block) (schemaVariable, error) {
	variable := schemaVariable{name: block.Labels[0]}
	typeAttr, ok := block.Body.Attributes["type"]
	if !ok {
		// Measured: the pinned binary refuses a schema-file variable with no
		// type ("The argument \"type\" is required"). Accepting one is how Ptah
		// used to exit 0 on a file that binary refuses.
		return schemaVariable{}, fmt.Errorf(
			"parse HCL schema at %s: variable %q requires a type",
			block.TypeRange.String(), variable.name,
		)
	}
	typ, err := variableType(variable.name, typeAttr)
	if err != nil {
		return schemaVariable{}, err
	}
	variable.typ = typ

	defaultAttr, ok := block.Body.Attributes["default"]
	if !ok {
		return variable, nil
	}
	value, diags := defaultAttr.Expr.Value(ctx)
	if diags.HasErrors() {
		return schemaVariable{}, fmt.Errorf(
			"parse HCL schema: variable %q default: %s", variable.name, diags.Error(),
		)
	}
	converted, err := convert.Convert(value, variable.typ)
	if err != nil {
		return schemaVariable{}, fmt.Errorf("variable %q: %s", variable.name, err)
	}
	variable.defValue = converted
	variable.hasDefault = true
	return variable, nil
}

func variableValue(variable schemaVariable, overrides map[string]cty.Value) (cty.Value, error) {
	override, ok := overrides[variable.name]
	if ok {
		converted, err := convert.Convert(override, variable.typ)
		if err != nil {
			return cty.NilVal, fmt.Errorf("variable %q: %s", variable.name, err)
		}
		return converted, nil
	}
	if !variable.hasDefault {
		// Byte-identical to the pinned binary's diagnostic, so a script that
		// matches on it keeps working across the two.
		return cty.NilVal, fmt.Errorf("missing value for required variable %q", variable.name)
	}
	return variable.defValue, nil
}

// variableTypeKeywords are the scalar type constraints a schema-file variable
// block accepts, measured one keyword at a time against the pinned binary.
// `float`, `any`, `integer`, `object` and `tuple` are NOT among them: that
// binary reports "There is no variable named \"float\"" for each, and the
// negative control `nosuchkeyword` reports the same, so the list is the
// measured boundary rather than a guess.
var variableTypeKeywords = map[string]cty.Type{
	"bool":   cty.Bool,
	"int":    cty.Number,
	"number": cty.Number,
	"string": cty.String,
}

// variableTypeConstructors are the collection constraints: list(T), set(T) and
// map(T), each over one of the scalar keywords.
var variableTypeConstructors = map[string]func(cty.Type) cty.Type{
	"list": cty.List,
	"map":  cty.Map,
	"set":  cty.Set,
}

func variableType(name string, attr *hclsyntax.Attribute) (cty.Type, error) {
	if typ, ok := scalarVariableType(attr.Expr); ok {
		return typ, nil
	}
	if typ, ok := collectionVariableType(attr.Expr); ok {
		return typ, nil
	}
	return cty.NilType, fmt.Errorf(
		"parse HCL schema at %s: variable %q type is not supported: supported types are bool, int, number, string, and list, map or set of those",
		attr.NameRange.String(), name,
	)
}

func scalarVariableType(expr hclsyntax.Expression) (cty.Type, bool) {
	name, ok := rootTraversalName(expr)
	if !ok {
		return cty.NilType, false
	}
	typ, ok := variableTypeKeywords[name]
	return typ, ok
}

func collectionVariableType(expr hclsyntax.Expression) (cty.Type, bool) {
	call, ok := expr.(*hclsyntax.FunctionCallExpr)
	if !ok || len(call.Args) != 1 {
		return cty.NilType, false
	}
	constructor, ok := variableTypeConstructors[call.Name]
	if !ok {
		return cty.NilType, false
	}
	element, ok := scalarVariableType(call.Args[0])
	if !ok {
		return cty.NilType, false
	}
	return constructor(element), true
}

// rootTraversalName returns the single-step traversal name an expression is,
// such as `string` in `type = string`.
func rootTraversalName(expr hclsyntax.Expression) (string, bool) {
	scope, ok := expr.(*hclsyntax.ScopeTraversalExpr)
	if !ok || len(scope.Traversal) != 1 {
		return "", false
	}
	root, ok := scope.Traversal[0].(hcl.TraverseRoot)
	if !ok {
		return "", false
	}
	return root.Name, true
}

// bindLocals evaluates every `locals` attribute into the `local.` namespace.
//
// Locals may reference each other in any order, so evaluation runs to a fixed
// point: each pass binds whatever now resolves, and the first pass that binds
// nothing reports the attribute that is still stuck.
func bindLocals(ctx *hcl.EvalContext, body *hclsyntax.Body) (*hcl.EvalContext, error) {
	pending := hclsyntax.Attributes{}
	for _, block := range body.Blocks {
		if block.Type != localsBlockType {
			continue
		}
		if err := collectLocals(block, pending); err != nil {
			return nil, err
		}
	}
	values := make(map[string]cty.Value)
	for len(pending) > 0 {
		stuck := sortedAttrNames(pending)[0]
		if !bindResolvableLocals(ctx, values, pending) {
			attr := pending[stuck]
			_, diags := attr.Expr.Value(ctx)
			return nil, fmt.Errorf("parse HCL schema: local %q: %s", stuck, diags.Error())
		}
	}
	return ctx, nil
}

func collectLocals(block *hclsyntax.Block, pending hclsyntax.Attributes) error {
	if len(block.Labels) > 0 {
		return fmt.Errorf(
			"parse HCL schema at %s: locals block does not take a label",
			block.TypeRange.String(),
		)
	}
	if len(block.Body.Blocks) > 0 {
		nested := block.Body.Blocks[0]
		return fmt.Errorf(
			"parse HCL schema at %s: unsupported locals block %q",
			nested.TypeRange.String(), nested.Type,
		)
	}
	maps.Copy(pending, block.Body.Attributes)
	return nil
}

func bindResolvableLocals(ctx *hcl.EvalContext, values map[string]cty.Value, pending hclsyntax.Attributes) bool {
	progress := false
	for _, name := range sortedAttrNames(pending) {
		value, diags := pending[name].Expr.Value(ctx)
		if diags.HasErrors() {
			continue
		}
		values[name] = value
		ctx.Variables[localNamespace] = cty.ObjectVal(values)
		delete(pending, name)
		progress = true
	}
	return progress
}

func sortedAttrNames(attrs hclsyntax.Attributes) []string {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// schemaFunctions is the function set an HCL schema file is evaluated against.
//
// The names come from a measurement, not a guess: each candidate was called
// with no arguments in a column default and run through the pinned Atlas
// community binary v1.3.0. "There is no function named X" means absent;
// anything else -- including an arity complaint -- means present. The negative
// control `nosuchfnxyz` came back absent, so the probe discriminates.
//
// Four names measured PRESENT on that binary are deliberately not here:
// `yamldecode` and `yamlencode` would add a module dependency for a spelling no
// schema needs, and `uuid` and `print` are a nondeterministic generator and a
// debug tap, neither of which belongs in a schema that has to diff stably. A
// file using one of those four is refused here and planned there.
func schemaFunctions(printLine func(string)) map[string]function.Function {
	fns := map[string]function.Function{
		"abs":       stdlib.AbsoluteFunc,
		"can":       tryfunc.CanFunc,
		"ceil":      stdlib.CeilFunc,
		"chomp":     stdlib.ChompFunc,
		"chunklist": stdlib.ChunklistFunc,
		"compact":   stdlib.CompactFunc,
		"concat":    stdlib.ConcatFunc,
		"contains":  stdlib.ContainsFunc,
		"csvdecode": stdlib.CSVDecodeFunc,
		"distinct":  stdlib.DistinctFunc,
		"element":   stdlib.ElementFunc,
		"flatten":   stdlib.FlattenFunc,
		"floor":     stdlib.FloorFunc,
		"format":    stdlib.FormatFunc,
		"indent":    stdlib.IndentFunc,
		"index":     stdlib.IndexFunc,
		"join":      stdlib.JoinFunc,
		"keys":      stdlib.KeysFunc,
		"length":    stdlib.LengthFunc,
		"log":       stdlib.LogFunc,
		"lower":     stdlib.LowerFunc,
		"max":       stdlib.MaxFunc,
		"merge":     stdlib.MergeFunc,
		"min":       stdlib.MinFunc,
		"parseint":  stdlib.ParseIntFunc,
		"pow":       stdlib.PowFunc,
		"range":     stdlib.RangeFunc,
		"regex":     stdlib.RegexFunc,
		"regexall":  stdlib.RegexAllFunc,
		"replace":   stdlib.ReplaceFunc,
		"reverse":   stdlib.ReverseListFunc,
		"signum":    stdlib.SignumFunc,
		"slice":     stdlib.SliceFunc,
		"sort":      stdlib.SortFunc,
		"split":     stdlib.SplitFunc,
		"substr":    stdlib.SubstrFunc,
		"timeadd":   stdlib.TimeAddFunc,
		"title":     stdlib.TitleFunc,
		"trim":      stdlib.TrimFunc,
		"trimspace": stdlib.TrimSpaceFunc,
		"try":       tryfunc.TryFunc,
		"upper":     stdlib.UpperFunc,
		"values":    stdlib.ValuesFunc,
		"zipmap":    stdlib.ZipmapFunc,
	}
	maps.Copy(fns, schemaCollectionFunctions())
	maps.Copy(fns, schemaMeasuredExtraFunctions(printLine))
	return fns
}

// schemaMeasuredExtraFunctions holds the three names stokaro/ptah#1627 found
// present in the pinned community binary v1.3.0 and unimplemented here.
//
// The reasons recorded against them were "would add a module dependency" for
// the YAML pair and "a debug tap" for print. The first is answered by using the
// library that produces the same bytes rather than a hand-rolled encoder -- the
// quoting is the tell, since `yamlencode({a = 1})` renders `"a": 1` on that
// binary with the key quoted, which is go-cty-yaml's style and not what a
// marshal of a Go map produces. The second is not an argument against
// implementing a function that an operator debugging a schema file reaches for
// and finds missing.
//
// `uuid` is deliberately absent, and it is not a divergence. It was recorded as
// measured-present because calling `uuid()` on that binary does not answer with
// the absent-marker; measured again for #1627, the answer is `Type "uuid" does
// not accept attributes`, and `type = uuid` renders a `uuid` column at exit 0.
// It is a type keyword, which Ptah already accepts, and never was a function --
// the original probe read one non-absent answer as presence.
func schemaMeasuredExtraFunctions(printLine func(string)) map[string]function.Function {
	return map[string]function.Function{
		"yamldecode": ctyyaml.YAMLDecodeFunc,
		"yamlencode": ctyyaml.YAMLEncodeFunc,
		"print":      printFunc(printLine),
	}
}

// printFunc is the debug tap: it returns its argument unchanged and writes the
// value to standard output, which is where that binary writes it -- measured
// with the streams separated, `print("hello")` puts `hello` on stdout and
// nothing on stderr.
//
// Returning the argument is what makes it usable inside an expression being
// debugged rather than beside it, and it is what keeps a schema file that calls
// it deterministic: the rendered DDL is the same with the call and without.
func printFunc(printLine func(string)) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{{
			Name:             "value",
			Type:             cty.DynamicPseudoType,
			AllowNull:        true,
			AllowDynamicType: true,
		}},
		Type: func(args []cty.Value) (cty.Type, error) {
			return args[0].Type(), nil
		},
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			printLine(printValueText(args[0]))
			return args[0], nil
		},
	})
}

// printDestination is where a printed line ends up. It is a variable so a test
// can read what one call produced without capturing the process's own stdout.
var printDestination io.Writer = os.Stdout

// printValueText renders one value the way that binary prints it: a string
// unquoted, everything else through cty's own formatting.
func printValueText(value cty.Value) string {
	if value.IsKnown() && !value.IsNull() && value.Type() == cty.String {
		return value.AsString()
	}
	return value.GoString()
}

func schemaCollectionFunctions() map[string]function.Function {
	return map[string]function.Function{
		"alltrue":         allTrueFunc,
		"coalescelist":    stdlib.CoalesceListFunc,
		"endswith":        affixFunc(strings.HasSuffix),
		"formatdate":      stdlib.FormatDateFunc,
		"formatlist":      stdlib.FormatListFunc,
		"jsondecode":      stdlib.JSONDecodeFunc,
		"jsonencode":      stdlib.JSONEncodeFunc,
		"setintersection": stdlib.SetIntersectionFunc,
		"setproduct":      stdlib.SetProductFunc,
		"setsubtract":     stdlib.SetSubtractFunc,
		"setunion":        stdlib.SetUnionFunc,
		"startswith":      affixFunc(strings.HasPrefix),
		// reverse is the LIST reverse and strrev the STRING reverse, measured:
		// `reverse("abc")` is refused ("can only reverse list or tuple values")
		// while `strrev("abc")` renders "cba". go-cty spells them the other way
		// round -- ReverseFunc is the string one -- so a mechanical name match
		// here would have swapped both.
		"strrev":     stdlib.ReverseFunc,
		"tobool":     stdlib.MakeToFunc(cty.Bool),
		"tolist":     stdlib.MakeToFunc(cty.List(cty.DynamicPseudoType)),
		"tonumber":   stdlib.MakeToFunc(cty.Number),
		"toset":      stdlib.MakeToFunc(cty.Set(cty.DynamicPseudoType)),
		"tostring":   stdlib.MakeToFunc(cty.String),
		"trimprefix": stdlib.TrimPrefixFunc,
		"trimsuffix": stdlib.TrimSuffixFunc,
	}
}

// affixFunc builds startswith/endswith, which go-cty's stdlib does not export.
func affixFunc(match func(string, string) bool) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{Name: "str", Type: cty.String},
			{Name: "affix", Type: cty.String},
		},
		Type: function.StaticReturnType(cty.Bool),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			return cty.BoolVal(match(args[0].AsString(), args[1].AsString())), nil
		},
	})
}

// allTrueFunc reports whether every element of a list of bools is true, which
// go-cty's stdlib does not export either.
var allTrueFunc = function.New(&function.Spec{
	Params: []function.Parameter{{Name: "list", Type: cty.List(cty.Bool)}},
	Type:   function.StaticReturnType(cty.Bool),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		for _, item := range args[0].AsValueSlice() {
			if item.IsNull() || !item.True() {
				return cty.False, nil
			}
		}
		return cty.True, nil
	},
})
