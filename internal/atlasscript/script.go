package atlasscript

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/convert"
)

// Kind is what a script does, and it is the first label on the block.
type Kind string

const (
	// KindQuery reads and reports. It writes nothing.
	KindQuery Kind = "query"
	// KindExec runs statements that change data.
	KindExec Kind = "exec"
	// KindLoop runs its body once per batch of an iterator.
	KindLoop Kind = "loop"
)

// Script is one `script "<kind>" "<name>"` block.
type Script struct {
	Kind Kind
	Name string
	// Steps are the body's steps in the order they were written, which is the
	// order they run. A script is a sequence, so the slice is the program.
	Steps []Step
	// Masks are the reusable `mask "<name>"` blocks declared beside it.
	Masks map[string]Mask
	// Iterator is the keyset walk a loop script runs its body over, and nil for
	// the other kinds.
	Iterator *Iterator
	// Range is where the block was written, for the report's `<file>:<line>`.
	Range hcl.Range
}

// Iterator is a `iterator "keyset" { … }` block: the walk a loop runs its body
// over, one batch at a time.
//
// Keyset rather than offset, and that is the documented shape rather than a
// choice made here -- an OFFSET walk over rows the body is deleting skips
// rows, because every delete shifts the offsets under the next page.
type Iterator struct {
	// Cursor is the columns carried from the last row of one page to the next
	// query, in declaration order.
	Cursor []Column
	// Batch is the columns of the page a do body reads as
	// iterator.keyset.batch. Empty means the cursor's columns.
	Batch []Column
	// InitSQL selects the first batch, and InitArgs are its constant
	// arguments.
	InitSQL  string
	InitArgs []hcl.Expression
	// NextSQL selects each batch after it, and NextArgs are its arguments,
	// which read the cursor as cursor.<col> and bind in the order written.
	NextSQL  string
	NextArgs []hcl.Expression
	Range    hcl.Range
}

// StepKind names what a step does.
type StepKind string

const (
	// StepQuery runs a SELECT and reports its rows.
	StepQuery StepKind = "query"
	// StepExec runs a statement and reports how many rows it changed.
	StepExec StepKind = "exec"
	// StepCondition runs a SELECT and stops the script when it is not true.
	StepCondition StepKind = "condition"
	// StepOutput prints a message.
	StepOutput StepKind = "output"
)

// Step is one thing a script does.
type Step struct {
	Kind StepKind
	Name string
	// SQL is the statement, for every kind but output.
	SQL string
	// Args are the placeholder arguments as written. Each is evaluated when
	// the step runs: a constant binds as its own type, and in a loop's do body
	// an element may read the page through iterator.keyset and self. What an
	// element may read is checked when the script is parsed.
	Args []hcl.Expression
	// ExpectRows is exec's assertion on the row count. Nil means no assertion,
	// which is different from zero -- a script that expects to change nothing
	// is a real thing to write, and it is not the same as not caring.
	ExpectRows *int
	// Message is output's text.
	Message string
	// Masks are the masks this step applies, in declaration order.
	Masks MaskSet
	// Range is where the step was written.
	Range hcl.Range
}

// ParseError is a refusal to read a script, carrying where it happened.
type ParseError struct {
	Range   hcl.Range
	Message string
}

func (e *ParseError) Error() string {
	if e.Range.Filename == "" {
		return e.Message
	}
	return fmt.Sprintf("%s:%d: %s", e.Range.Filename, e.Range.Start.Line, e.Message)
}

// Parse reads every script in one document.
//
// The grammar is reproduced from publicly documented behavior, and where that
// material is silent the parser REFUSES rather than guessing. A script runs
// statements against a database, and a block accepted with a meaning nobody
// stated is the shape that deletes the wrong rows quietly (stokaro/ptah#1017).
func Parse(data []byte, filename string) ([]Script, error) {
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, &ParseError{Message: diags.Error()}
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, &ParseError{Message: "the document is not HCL native syntax"}
	}

	masks, err := parseMaskBlocks(body)
	if err != nil {
		return nil, err
	}

	scripts := make([]Script, 0, len(body.Blocks))
	seen := make(map[string]hcl.Range, len(body.Blocks))
	for _, block := range body.Blocks {
		switch block.Type {
		case "mask":
			continue
		case "script":
			script, err := parseScriptBlock(block, masks)
			if err != nil {
				return nil, err
			}
			identity := string(script.Kind) + "\x00" + script.Name
			if first, taken := seen[identity]; taken {
				return nil, &ParseError{
					Range: block.DefRange(),
					Message: fmt.Sprintf(
						"script %q %q is declared twice, first at line %d; a name selects which script runs, so two cannot share one",
						script.Kind, script.Name, first.Start.Line),
				}
			}
			seen[identity] = block.DefRange()
			scripts = append(scripts, script)
		default:
			return nil, &ParseError{
				Range:   block.DefRange(),
				Message: fmt.Sprintf("unsupported block %q; a script document holds script and mask blocks", block.Type),
			}
		}
	}
	if len(scripts) == 0 {
		return nil, &ParseError{Message: "the document declares no script"}
	}
	return scripts, nil
}

func parseScriptBlock(block *hclsyntax.Block, masks map[string]Mask) (Script, error) {
	if len(block.Labels) != 2 {
		return Script{}, &ParseError{
			Range:   block.DefRange(),
			Message: `a script block takes two labels: script "<kind>" "<name>"`,
		}
	}
	kind := Kind(block.Labels[0])
	switch kind {
	case KindQuery, KindExec, KindLoop:
	default:
		return Script{}, &ParseError{
			Range: block.DefRange(),
			Message: fmt.Sprintf(
				"unsupported script kind %q; the kinds are query, exec and loop", block.Labels[0]),
		}
	}

	script := Script{Kind: kind, Name: block.Labels[1], Masks: masks, Range: block.DefRange()}
	// The iterator is read first: a loop's steps are checked against the
	// columns it declares.
	iterator, err := parseIterator(block)
	if err != nil {
		return Script{}, err
	}
	script.Iterator = iterator

	scope, shape := scopeConstant, pageShape{}
	if kind == KindLoop && iterator != nil {
		scope, shape = scopeBody, pageShape{cursor: iterator.Cursor, batch: iterator.Batch}
	}
	steps, err := parseSteps(block.Body, masks, scope, shape)
	if err != nil {
		return Script{}, err
	}
	script.Steps = steps

	// The pairing is checked here rather than at run time, because a script
	// with the wrong shape is wrong before it reaches a database. A loop with
	// no iterator would run its body once over everything -- which for a body
	// holding a DELETE is the batching silently not happening.
	if kind == KindLoop && script.Iterator == nil {
		return Script{}, &ParseError{
			Range: block.DefRange(),
			Message: fmt.Sprintf(
				"loop %q has no iterator; without one its body would run once over everything rather than in batches",
				script.Name),
		}
	}
	if kind != KindLoop && script.Iterator != nil {
		return Script{}, &ParseError{
			Range: block.DefRange(),
			Message: fmt.Sprintf(
				"%s %q declares an iterator, which only a loop runs", kind, script.Name),
		}
	}

	if len(script.Steps) == 0 {
		return Script{}, &ParseError{
			Range:   block.DefRange(),
			Message: fmt.Sprintf("script %q %q has no steps, so running it would do nothing", kind, script.Name),
		}
	}
	return script, nil
}

// parseSteps reads a body's steps, descending into `do` because a loop wraps
// its body in one.
func parseSteps(body *hclsyntax.Body, masks map[string]Mask, scope argScope, shape pageShape) ([]Step, error) {
	steps := make([]Step, 0, len(body.Blocks))
	for _, block := range body.Blocks {
		switch block.Type {
		case "do":
			nested, err := parseSteps(block.Body, masks, scope, shape)
			if err != nil {
				return nil, err
			}
			steps = append(steps, nested...)
		case "query", "exec", "condition", "output":
			step, err := parseStep(block, masks, scope, shape)
			if err != nil {
				return nil, err
			}
			steps = append(steps, step)
		case "iterator":
			// Read by parseScriptBlock, which owns it: an iterator is the
			// script's, not a step, and a body that returned it as one would
			// let a loop carry two.
			continue
		case "http":
			return nil, &ParseError{
				Range:   block.DefRange(),
				Message: "http blocks are not read yet",
			}
		default:
			return nil, &ParseError{
				Range:   block.DefRange(),
				Message: fmt.Sprintf("unsupported block %q inside a script", block.Type),
			}
		}
	}
	return steps, nil
}

func parseStep(block *hclsyntax.Block, masks map[string]Mask, scope argScope, shape pageShape) (Step, error) {
	step := Step{Kind: StepKind(block.Type), Range: block.DefRange()}
	if len(block.Labels) > 0 {
		step.Name = block.Labels[0]
	}

	if step.Kind == StepOutput {
		message, err := stringAttr(block, "message")
		if err != nil {
			return Step{}, err
		}
		if message == "" {
			return Step{}, &ParseError{Range: block.DefRange(), Message: "output has no message"}
		}
		step.Message = message
		return step, nil
	}

	sql, err := stringAttr(block, "sql")
	if err != nil {
		return Step{}, err
	}
	if strings.TrimSpace(sql) == "" {
		return Step{}, &ParseError{
			Range:   block.DefRange(),
			Message: fmt.Sprintf("%s has no sql", block.Type),
		}
	}
	step.SQL = sql

	if attr := block.Body.Attributes["args"]; attr != nil {
		args, err := parseArgs(attr, scope, shape)
		if err != nil {
			return Step{}, err
		}
		step.Args = args
	}

	if attr := block.Body.Attributes["expect_rows"]; attr != nil {
		count, err := intAttr(block, attr)
		if err != nil {
			return Step{}, err
		}
		step.ExpectRows = &count
	}

	stepMasks, err := parseStepMasks(block, masks)
	if err != nil {
		return Step{}, err
	}
	step.Masks = stepMasks
	if err := step.Masks.Compile(); err != nil {
		return Step{}, &ParseError{Range: block.DefRange(), Message: err.Error()}
	}
	return step, nil
}

func stringAttr(block *hclsyntax.Block, name string) (string, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return "", nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.IsNull() {
		return "", &ParseError{
			Range:   block.DefRange(),
			Message: fmt.Sprintf("%s must be a literal string", name),
		}
	}
	converted, err := convert.Convert(value, cty.String)
	if err != nil {
		return "", &ParseError{
			Range:   block.DefRange(),
			Message: fmt.Sprintf("%s must be a literal string", name),
		}
	}
	return converted.AsString(), nil
}

func intAttr(block *hclsyntax.Block, attr *hclsyntax.Attribute) (int, error) {
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.IsNull() {
		return 0, &ParseError{Range: block.DefRange(), Message: "expect_rows must be a number"}
	}
	converted, err := convert.Convert(value, cty.Number)
	if err != nil {
		return 0, &ParseError{Range: block.DefRange(), Message: "expect_rows must be a number"}
	}
	count, _ := converted.AsBigFloat().Int64()
	if count < 0 {
		return 0, &ParseError{Range: block.DefRange(), Message: "expect_rows is negative"}
	}
	return int(count), nil
}

// traversalText spells a reference the way it was written, such as cursor.id.
func traversalText(traversal hcl.Traversal) string {
	var text strings.Builder
	for _, step := range traversal {
		switch step := step.(type) {
		case hcl.TraverseRoot:
			text.WriteString(step.Name)
		case hcl.TraverseAttr:
			text.WriteString("." + step.Name)
		default:
			text.WriteString("[...]")
		}
	}
	return text.String()
}

// stringList reads a list attribute whose elements are literal strings.
//
// An element that is not one is refused rather than read as the empty string:
// a mask whose columns came back empty matches no column, and the query prints
// the values it was written to hide.
func stringList(attr *hclsyntax.Attribute, name string) ([]string, error) {
	list, ok := attr.Expr.(*hclsyntax.TupleConsExpr)
	if !ok {
		return nil, &ParseError{Range: attr.SrcRange, Message: name + " must be a list"}
	}
	values := make([]string, 0, len(list.Exprs))
	for _, expr := range list.Exprs {
		value, diags := expr.Value(nil)
		if diags.HasErrors() || value.IsNull() || value.Type() != cty.String {
			return nil, &ParseError{Range: expr.Range(), Message: fmt.Sprintf(
				"%s element %s must be a literal string", name, exprText(expr))}
		}
		values = append(values, value.AsString())
	}
	return values, nil
}
