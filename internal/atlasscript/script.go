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
	// Cursor names the columns carried between batches, in declaration order.
	Cursor []string
	// InitSQL selects the first batch.
	InitSQL string
	// NextSQL selects each batch after it, and NextArgs are the cursor
	// references it takes.
	NextSQL  string
	NextArgs []string
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
	// Args are the placeholder arguments, written as raw expressions because
	// the values a loop supplies are not known until it runs.
	Args []string
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
	steps, err := parseSteps(block.Body, masks)
	if err != nil {
		return Script{}, err
	}
	script.Steps = steps

	iterator, err := parseIterator(block)
	if err != nil {
		return Script{}, err
	}
	script.Iterator = iterator

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
func parseSteps(body *hclsyntax.Body, masks map[string]Mask) ([]Step, error) {
	steps := make([]Step, 0, len(body.Blocks))
	for _, block := range body.Blocks {
		switch block.Type {
		case "do":
			nested, err := parseSteps(block.Body, masks)
			if err != nil {
				return nil, err
			}
			steps = append(steps, nested...)
		case "query", "exec", "condition", "output":
			step, err := parseStep(block, masks)
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

func parseStep(block *hclsyntax.Block, masks map[string]Mask) (Step, error) {
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
		args, err := rawList(attr)
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

// rawList returns a list attribute's elements as their source text.
//
// Source text rather than values, because a loop's arguments reference the
// cursor and are not evaluable until it runs. Reading them as text keeps the
// grammar honest about what it has: the executor resolves them, and a step that
// reaches an unresolved reference refuses there rather than here.
func rawList(attr *hclsyntax.Attribute) ([]string, error) {
	list, ok := attr.Expr.(*hclsyntax.TupleConsExpr)
	if !ok {
		return nil, &ParseError{Range: attr.SrcRange, Message: "args must be a list"}
	}
	args := make([]string, 0, len(list.Exprs))
	for _, expr := range list.Exprs {
		value, diags := expr.Value(nil)
		if !diags.HasErrors() && !value.IsNull() {
			converted, err := convert.Convert(value, cty.String)
			if err == nil {
				args = append(args, converted.AsString())
				continue
			}
		}
		args = append(args, "")
	}
	return args, nil
}
