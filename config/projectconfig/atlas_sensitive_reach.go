package projectconfig

import (
	"slices"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/internal/secretmask"
)

// valueSources are the parts of an atlas.hcl file that pass a value on to the
// expressions that name them, beside the locals: the for_each of each env
// block, which gives each.key and each.value to the expressions of the block,
// and the body of each data block, which gives data.<type>.<name>. They are
// read before anything is evaluated, as the locals are, so an expression that
// failed can be followed to the sensitive variables it reads.
type valueSources struct {
	forEach []forEachSource
	data    map[string]*hclsyntax.Body
}

// forEachSource is the for_each expression of an env block, with the range of
// the block, which holds every expression `each` reaches.
type forEachSource struct {
	block hcl.Range
	expr  hclsyntax.Expression
}

// readValueSources returns the value sources of the top-level blocks of body.
func readValueSources(body *hclsyntax.Body) valueSources {
	sources := valueSources{data: make(map[string]*hclsyntax.Body)}
	for _, block := range body.Blocks {
		switch {
		case block.Type == atlasEnvBlockType:
			if attr, ok := block.Body.Attributes["for_each"]; ok {
				sources.forEach = append(sources.forEach, forEachSource{block: block.Range(), expr: attr.Expr})
			}
		case block.Type == "data" && len(block.Labels) == 2:
			sources.data[block.Labels[0]+"."+block.Labels[1]] = block.Body
		}
	}
	return sources
}

// readsSensitive reports whether expr reads a `sensitive = true` variable:
// by its name, through the expression of a local it names, through the
// for_each of the env block that holds it where it names each, or through the
// attributes of a data block it names. A nil expr reads none.
func (p atlasParser) readsSensitive(expr hcl.Expression) bool {
	if p.sensitiveNames == nil || len(*p.sensitiveNames) == 0 {
		return false
	}
	return p.reachesSensitive(expr, make(map[string]struct{}))
}

// reachesSensitive is [atlasParser.readsSensitive] with the sources already
// followed, each followed once.
func (p atlasParser) reachesSensitive(expr hcl.Expression, seen map[string]struct{}) bool {
	if expr == nil {
		return false
	}
	return slices.ContainsFunc(expr.Variables(), func(traversal hcl.Traversal) bool {
		return p.traversalReachesSensitive(traversal, expr.Range(), seen)
	})
}

// traversalReachesSensitive reports whether traversal, named by an expression
// at the range at, reads a sensitive variable.
func (p atlasParser) traversalReachesSensitive(traversal hcl.Traversal, at hcl.Range, seen map[string]struct{}) bool {
	switch traversal.RootName() {
	case "var":
		_, sensitive := (*p.sensitiveNames)[traversalStep(traversal, 1)]
		return sensitive
	case "local":
		name := traversalStep(traversal, 1)
		if !firstVisit(seen, "local."+name) {
			return false
		}
		expr, ok := p.locals[name]
		return ok && p.reachesSensitive(expr, seen)
	case "each":
		for _, source := range p.valueSources.forEach {
			if source.block.Filename == at.Filename && source.block.ContainsOffset(at.Start.Byte) {
				return firstVisit(seen, "each."+source.block.String()) && p.reachesSensitive(source.expr, seen)
			}
		}
	case "data":
		name := traversalStep(traversal, 1) + "." + traversalStep(traversal, 2)
		body, ok := p.valueSources.data[name]
		return ok && firstVisit(seen, "data."+name) && p.bodyReachesSensitive(body, seen)
	}
	return false
}

// bodyReachesSensitive reports whether an attribute of body, or of a block it
// holds, reads a sensitive variable.
func (p atlasParser) bodyReachesSensitive(body *hclsyntax.Body, seen map[string]struct{}) bool {
	for _, attr := range body.Attributes {
		if p.reachesSensitive(attr.Expr, seen) {
			return true
		}
	}
	return slices.ContainsFunc(body.Blocks, func(block *hclsyntax.Block) bool {
		return p.bodyReachesSensitive(block.Body, seen)
	})
}

// traversalStep returns the name step i of traversal takes, or "" when it
// takes none there; see [stepName].
func traversalStep(traversal hcl.Traversal, i int) string {
	if i >= len(traversal) {
		return ""
	}
	name, _ := stepName(traversal[i])
	return name
}

// stepName returns the name step takes: an attribute's, as in `var.tok`, or a
// string index the file writes out, as in `var["tok"]`. HCL reads the two
// spellings as the same lookup, so a rule about what a name reads must read
// both. It reports false for a step that takes no name, such as a number
// index.
func stepName(step hcl.Traverser) (string, bool) {
	switch step := step.(type) {
	case hcl.TraverseAttr:
		return step.Name, true
	case hcl.TraverseIndex:
		if step.Key.IsKnown() && !step.Key.IsNull() && step.Key.Type() == cty.String {
			return step.Key.AsString(), true
		}
	}
	return "", false
}

// evaluationRoots are the names the evaluation context of atlas.hcl gives an
// expression. A traversal from any other root names a variable a scope of the
// expression's own declares, as a `for` expression or a `%{ for }` directive
// declares its iteration variables.
var evaluationRoots = []string{"var", "local", "each", "data", "atlas"}

// readsAScopeOfItsOwn reports whether expr names a variable that no root of
// the evaluation context gives, so that what it reads is decided by an
// enclosing expression rather than by expr.
func readsAScopeOfItsOwn(expr hcl.Expression) bool {
	return slices.ContainsFunc(expr.Variables(), func(traversal hcl.Traversal) bool {
		return !slices.Contains(evaluationRoots, traversal.RootName())
	})
}

// firstVisit records key in seen, and reports whether it was not there.
func firstVisit(seen map[string]struct{}, key string) bool {
	if _, done := seen[key]; done {
		return false
	}
	seen[key] = struct{}{}
	return true
}

// shortSensitiveValues returns the values of `sensitive = true` variables too
// short for [atlasParser.scrubSensitive] to take out wherever they occur.
func (p atlasParser) shortSensitiveValues() []string {
	if p.sensitiveValues == nil {
		return nil
	}
	var short []string
	for _, value := range *p.sensitiveValues {
		if value != "" && !secretmask.MasksAnywhere(value) {
			short = append(short, value)
		}
	}
	return short
}

// holdsShortSensitiveValue reports whether text, as
// [atlasParser.scrubSensitive] left it, holds one of short as a word of its
// own. The text may quote the value, as `open x: file does not exist` quotes a
// path "x", or hold a word of its own spelled the same, and which of the two
// cannot be told. The letters of a value inside a longer word, as "x" stands in
// "exist", are the text's own.
func (p atlasParser) holdsShortSensitiveValue(text string, short []string) bool {
	scrubbed := p.scrubSensitive(text)
	return slices.ContainsFunc(short, func(value string) bool {
		return secretmask.ContainsWord(scrubbed, value)
	})
}

// diagnosticQuotesShortSensitiveValue reports whether a diagnostic of diags
// may quote the value of a `sensitive = true` variable that
// [atlasParser.scrubSensitive] cannot take out: its summary or its detail
// holds a short sensitive value as a word of its own, and the expression it
// blames reads a sensitive variable. Where it blames none, or blames one that
// names an iteration variable, as `file(s)` in `[for s in local.a : file(s)]`
// does, the expression read is attr, which holds what gives that variable its
// value. The rest of the message is Ptah's -- the file's name, a line and a
// column -- and an expression that reads no sensitive variable cannot quote
// one, so a word of the message spelled like the value does not withhold it.
func (p atlasParser) diagnosticQuotesShortSensitiveValue(attr hcl.Expression, diags hcl.Diagnostics) bool {
	short := p.shortSensitiveValues()
	if len(short) == 0 {
		return false
	}
	return slices.ContainsFunc(diags, func(diag *hcl.Diagnostic) bool {
		if diag.Severity != hcl.DiagError || !p.holdsShortSensitiveValue(diag.Summary+"\n"+diag.Detail, short) {
			return false
		}
		blamed := diag.Expression
		if blamed == nil || readsAScopeOfItsOwn(blamed) {
			blamed = attr
		}
		return p.readsSensitive(blamed)
	})
}

// reasonQuotesShortSensitiveValue reports whether reason, the error a Ptah rule
// refused the value of expr with, may quote the value of a `sensitive = true`
// variable that [atlasParser.scrubSensitive] cannot take out: it holds a short
// sensitive value as a word of its own, and expr reads a sensitive variable.
func (p atlasParser) reasonQuotesShortSensitiveValue(expr hcl.Expression, reason string) bool {
	short := p.shortSensitiveValues()
	return len(short) > 0 && p.holdsShortSensitiveValue(reason, short) && p.readsSensitive(expr)
}
