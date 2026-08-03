package atlashcl

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

// IgnoredName is a schema-HCL name that was accepted and dropped under the
// unknown-name policy.
//
// It exists so a caller can say something about what it silently discarded.
// The community binary reports nothing at all, and that silence is a footgun:
// a typo'd attribute name does nothing and looks fine. Nothing about the
// parse result depends on whether a caller reports these, so drop-in fidelity
// is unaffected either way. This mirrors what the atlas.hcl parser records in
// [go.5x5.cz/ptah/config/projectconfig.Config.IgnoredConstructs], so both HCL
// layers make the same choice.
type IgnoredName struct {
	// Name is the block type or attribute name as written.
	Name string
	// Kind is "block" or "attribute".
	Kind string
	// Scope names the construct that contained it ("top-level", "table",
	// "column", ...). The position matters because the same name can be
	// modeled in one place and unmodeled in another.
	Scope string
	// Filename and Line locate it.
	Filename string
	Line     int
}

// typeKeyword is the Go type behind the opaque value a bare Atlas type keyword
// evaluates to inside a dropped body. It carries nothing: the only thing the
// keyword has to support is being written down.
type typeKeyword struct{}

// typeKeywordType makes that opacity a property of the cty type system rather
// than a convention.
//
// A capsule supports no conversion, no member access, no index, no iteration
// and no arithmetic, so every one of those fails here whatever the community
// binary does with the same expression. An object or a [cty.DynamicVal] would
// each quietly succeed at some of them, and succeeding where the community
// binary fails is the one direction this parser may never take.
var typeKeywordType = cty.Capsule("Atlas type keyword", reflect.TypeFor[typeKeyword]())

// typeKeywordValue is shared by every keyword so that comparing two of them is
// a comparison of equal values rather than an error.
var typeKeywordValue = cty.CapsuleVal(typeKeywordType, &typeKeyword{})

// droppedBodyScope is the ENTIRE variable scope a dropped body is evaluated
// in. It is a fixed table, not a view of the file, and that is the point.
//
// Two earlier attempts modeled the file's own blocks and variables as
// reference roots so that `table.t.column.id`, `attr.name` and `var.v` would
// resolve inside a dropped body the way they resolve on the community binary.
// Both had to represent something they had not measured -- an unlabeled block,
// a variable of unknown type -- and both reached for a wildcard to do it.
// Every wildcard turned into a position where Ptah accepted a file the
// community binary refuses: 12 of them, measured, including `var.v.nope` on a
// string variable, `1 + var.v`, `primary_key.nope` and `inner["typo"]`.
//
// So the scope is closed instead. `string`, `int` and `bool` are here because
// they are the only bare identifiers measured to resolve inside a dropped body
// on every dialect, and because the construct stokaro/ptah#1016 exists to
// accept -- `annotation "gql" { attr "name" { type = string } }` -- needs
// them. Everything else fails to resolve, which refuses some files the
// community binary accepts (`ref = table.t`, `ref = var.v`) and accepts none
// it refuses. That asymmetry is deliberate: refusing a file the binary accepts
// costs a user an error message, while accepting one it refuses ships a schema
// the real tool would never have loaded.
var droppedBodyScope = map[string]cty.Value{
	"string": typeKeywordValue,
	"int":    typeKeywordValue,
	"bool":   typeKeywordValue,
}

// droppedBodyFunctions is the function table used to evaluate the body of a
// dropped name.
//
// Every name here was confirmed present on the pinned community binary, and
// the argument-count and argument-type diagnostics it emitted for `join` and
// `split` are the verbatim go-cty stdlib texts, which is what pins the
// implementation these names resolve to. Any other call is refused, matching
// the binary's own `Call to unknown function` on a name it does not have.
//
// These stay safe under a closed scope for the same reason the scope is
// closed: the only values that can reach an argument are literals and opaque
// capsules, so an accepted call is always this go-cty stdlib applied to a
// literal -- which is exactly what the community binary does with it.
var droppedBodyFunctions = map[string]function.Function{
	"format":     stdlib.FormatFunc,
	"join":       stdlib.JoinFunc,
	"jsonencode": stdlib.JSONEncodeFunc,
	"lower":      stdlib.LowerFunc,
	"split":      stdlib.SplitFunc,
	"title":      stdlib.TitleFunc,
	"trimspace":  stdlib.TrimSpaceFunc,
	"upper":      stdlib.UpperFunc,
}

// droppedBodyContext is the evaluation context every dropped expression sees.
// It is built once and never mutated; HCL only reads it, and a for-expression
// binds its iterator in a child context rather than in this one.
var droppedBodyContext = &hcl.EvalContext{
	Variables: droppedBodyScope,
	Functions: droppedBodyFunctions,
}

// tolerateUnknownBlock accepts a block name this parser does not model.
//
// The block contributes nothing to the IR, but its body is still checked: the
// community binary evaluates the whole file before it decides which names to
// decode, so an unresolvable reference inside a construct it is about to drop
// is still fatal. Measured, same command, same dev database:
//
//	annotation { gql = "Thing" }               -> exit 0
//	annotation { gql = not_a_real_identifier } -> exit 1, "Unknown variable"
//
// Skipping the subtree instead would accept the second file, which makes this
// parser looser than the binary it is matching -- the dangerous direction, and
// the one that turns today's coincidental agreement into a real divergence.
func (p *parser) tolerateUnknownBlock(scope string, block *hclsyntax.Block) error {
	if err := p.checkDroppedBody(block.Body); err != nil {
		return err
	}
	p.noteIgnored(IgnoredName{
		Name:     block.Type,
		Kind:     "block",
		Scope:    scope,
		Filename: block.TypeRange.Filename,
		Line:     block.TypeRange.Start.Line,
	})
	return nil
}

// tolerateUnknownAttr accepts an attribute name this parser does not model,
// with the same name-level rule as tolerateUnknownBlock.
func (p *parser) tolerateUnknownAttr(scope string, attr *hclsyntax.Attribute) error {
	if err := p.checkDroppedExpr(attr.Expr); err != nil {
		return err
	}
	p.noteIgnored(IgnoredName{
		Name:     attr.Name,
		Kind:     "attribute",
		Scope:    scope,
		Filename: attr.NameRange.Filename,
		Line:     attr.NameRange.Start.Line,
	})
	return nil
}

// noteIgnored hands a dropped name to the caller's recorder, if it set one.
func (p *parser) noteIgnored(ignored IgnoredName) {
	if p.recordIgnored == nil {
		return
	}
	p.recordIgnored(ignored)
}

// checkDroppedBody checks every expression under a dropped name, attributes
// first and in name order so a body carrying several bad expressions always
// reports the same one.
func (p *parser) checkDroppedBody(body *hclsyntax.Body) error {
	if body == nil {
		return nil
	}
	for _, name := range slices.Sorted(maps.Keys(body.Attributes)) {
		if err := p.checkDroppedExpr(body.Attributes[name].Expr); err != nil {
			return err
		}
	}
	for _, block := range body.Blocks {
		if err := p.checkDroppedBody(block.Body); err != nil {
			return err
		}
	}
	return nil
}

// checkDroppedExpr evaluates one expression under a dropped name and reports
// the first thing that does not resolve.
//
// Evaluating rather than pattern-matching references is what keeps this parser
// from being looser than the community binary, which evaluates the whole file
// before deciding what to decode. All four of these exit 1 there and all four
// reach that verdict through a different diagnostic, so a check that only
// resolved reference roots would let three of them through:
//
//	annotation "gql" { ref = variable.v }                -> Unknown variable
//	annotation "gql" { ref = frobnicate_nonsense("a") }  -> Call to unknown function
//	annotation "gql" { ref = 1 + "abc" }                 -> Invalid operand
//	annotation "gql" { ref = 1 + string }                -> Invalid operand
//
// The unresolvable-root case is reported before evaluation only so the message
// can name the variable and underline the root rather than the whole
// traversal, which is what the community binary underlines.
func (p *parser) checkDroppedExpr(expr hclsyntax.Expression) error {
	if expr == nil {
		return nil
	}
	for _, traversal := range expr.Variables() {
		if len(traversal) == 0 {
			continue
		}
		root, ok := traversal[0].(hcl.TraverseRoot)
		if !ok {
			continue
		}
		if _, inScope := droppedBodyScope[root.Name]; inScope {
			continue
		}
		// The root's own range, not the whole traversal's: the community binary
		// underlines just the name it could not resolve, so `column.id` points
		// at `column`.
		return p.unknownVariableError(root.Name, root.SrcRange)
	}
	if _, diags := expr.Value(droppedBodyContext); diags.HasErrors() {
		return p.evaluationFailed(diags)
	}
	return nil
}

// unknownVariableError reports an unresolvable reference at the range of the
// reference, not the range of the construct that contains it, so the position
// lines up with the community binary's own diagnostic.
func (p *parser) unknownVariableError(name string, rng hcl.Range) error {
	return fmt.Errorf("parse HCL schema at %s: unknown variable %q", rng.String(), name)
}

// evaluationFailed reports the first evaluation diagnostic in the style of the
// rest of this package: lower-cased summary, then the detail the HCL library
// wrote, at the range the diagnostic points to.
func (p *parser) evaluationFailed(diags hcl.Diagnostics) error {
	for _, diag := range diags {
		if diag.Severity != hcl.DiagError {
			continue
		}
		where := "schema.hcl"
		if diag.Subject != nil {
			where = diag.Subject.String()
		}
		summary := strings.ToLower(diag.Summary)
		if diag.Detail == "" {
			return fmt.Errorf("parse HCL schema at %s: %s", where, summary)
		}
		return fmt.Errorf("parse HCL schema at %s: %s: %s", where, summary, diag.Detail)
	}
	return fmt.Errorf("parse HCL schema: %s", diags.Error())
}
