// Package lintexpr compiles the expression a declared lint rule is written as.
//
// A rule declared in a configuration file is an HCL expression over one
// statement, evaluated once per statement, whose result decides whether the
// rule fires:
//
//	contains(lower(statement.sql), "varchar(")
//
// # Why HCL, and why this function set
//
// The repository already evaluates HCL in `atlas.hcl` and in schema files, over
// one shared function set. Giving lint rules a second expression language would
// mean a second vocabulary for the same job, which is the defect
// stokaro/ptah#1810 was filed about in the other direction.
//
// The set is [atlashcl.ProjectFunctions], and what it LEAVES OUT is what makes
// it right here rather than merely convenient. It carries no `file`, `fileset`
// or `getenv`, and no `print`. A lint rule that could read a file or the
// environment would report findings that depend on the machine it ran on, so
// the same migration would lint clean on one developer's checkout and fail in
// CI with nothing in the migration to explain it. A rule that could write to
// stdout would interleave with the report on the same stream.
//
// So the evaluation is a pure function of the statement, which is the property
// that makes a lint finding reproducible.
package lintexpr

import (
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// Scope is the statement a rule expression is evaluated against.
//
// The field names are the ones the expression writes, so this type IS the
// documented surface of the rule language: adding a field adds vocabulary, and
// renaming one breaks every rule already written against it.
type Scope struct {
	// SQL is the statement text as written, comments included.
	SQL string
	// Canonical is the comment-stripped, whitespace-collapsed, uppercased form.
	// A rule matching structure rather than formatting wants this one.
	Canonical string
	// Words is the token-word sequence, with string literals and quoted
	// identifiers kept whole. A rule using `contains(statement.words, "DROP")`
	// cannot be fooled by a column named "drop" or a literal containing it,
	// which a substring match on SQL would be.
	Words []string
	// Line is the 1-based line of the statement's first token.
	Line int
	// Path is the migration file path as findings report it.
	Path string
	// IsUp and IsDown say which direction this statement belongs to, so a rule
	// can scope itself without a separate field in the declaration.
	IsUp   bool
	IsDown bool
	// Dialect is the target dialect being linted, lowercased.
	Dialect string
}

// Expression is a compiled rule expression, safe to evaluate concurrently.
type Expression struct {
	source string
	expr   hclsyntax.Expression
}

// Source returns the expression as it was written, for diagnostics.
func (e *Expression) Source() string { return e.source }

// Compile parses source and reports what is wrong with it in terms of the rule,
// not of the file it happened to be embedded in.
//
// Compiling at load time rather than at first evaluation is deliberate: a
// malformed rule is a configuration error, and reporting it when the config is
// read means `lint` fails before it reports findings, rather than after
// reporting some of them.
func Compile(ruleCode, source string) (*Expression, error) {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return nil, fmt.Errorf("lint rule %s: match expression is empty", ruleCode)
	}
	expr, diags := hclsyntax.ParseExpression([]byte(trimmed), ruleCode+".match", hcl.InitialPos)
	if diags.HasErrors() {
		return nil, fmt.Errorf("lint rule %s: parse match expression: %s", ruleCode, diags.Error())
	}
	compiled := &Expression{source: trimmed, expr: expr}
	if err := compiled.checkNames(ruleCode); err != nil {
		return nil, err
	}
	return compiled, nil
}

// checkNames refuses a variable the scope does not define, at COMPILE time.
//
// Left to evaluation, an expression naming `stmt.sql` instead of
// `statement.sql` would fail once per statement -- or, worse, be reported as a
// rule that simply never fires, which reads as "the code is clean".
func (e *Expression) checkNames(ruleCode string) error {
	for _, traversal := range e.expr.Variables() {
		root := traversal.RootName()
		if _, ok := scopeRoots[root]; !ok {
			return fmt.Errorf(
				"lint rule %s: unknown name %q in match expression; available: %s",
				ruleCode, root, strings.Join(scopeRootNames(), ", "))
		}
	}
	return nil
}

// Evaluate reports whether the rule fires for scope.
//
// A non-boolean result is an error rather than a coerced truth value. A rule
// whose expression returns a string would otherwise fire on every statement or
// on none, depending on the coercion, and both look like a working rule.
func (e *Expression) Evaluate(ruleCode string, scope Scope) (bool, error) {
	value, diags := e.expr.Value(evalContext(scope))
	if diags.HasErrors() {
		return false, fmt.Errorf(
			"lint rule %s: evaluate match expression: %s%s",
			ruleCode, diags.Error(), containsHint(diags))
	}
	if value.IsNull() {
		return false, fmt.Errorf("lint rule %s: match expression evaluated to null", ruleCode)
	}
	if value.Type() != cty.Bool {
		return false, fmt.Errorf(
			"lint rule %s: match expression must evaluate to a boolean, got %s",
			ruleCode, value.Type().FriendlyName())
	}
	return value.True(), nil
}

// containsHint names the fix for the one mistake this language invites.
//
// `contains` tests list membership, so `contains(statement.sql, "x")` -- the
// spelling everyone reaches for first -- fails with a type error about lists
// that says nothing about what to write instead.
func containsHint(diags hcl.Diagnostics) string {
	if !strings.Contains(diags.Error(), "argument must be list, tuple, or set") {
		return ""
	}
	return "; `contains` tests list membership (use it on statement.words) -- " +
		"for substring matching use `strcontains(haystack, needle)`"
}

// scopeRoots is the set of names an expression may start a traversal with.
var scopeRoots = map[string]struct{}{"statement": {}, "file": {}, "dialect": {}}

func scopeRootNames() []string { return []string{"dialect", "file", "statement"} }

func evalContext(scope Scope) *hcl.EvalContext {
	words := make([]cty.Value, 0, len(scope.Words))
	for _, word := range scope.Words {
		words = append(words, cty.StringVal(word))
	}
	// The empty case is built as a typed empty LIST so that statement.words has
	// one type whether or not the statement has words. `contains` would accept
	// an empty tuple too, so this is about the value being consistent rather
	// than about avoiding a failure.
	wordList := cty.ListValEmpty(cty.String)
	if len(words) > 0 {
		wordList = cty.ListVal(words)
	}
	return &hcl.EvalContext{
		Functions: ruleFunctions(),
		Variables: map[string]cty.Value{
			"statement": cty.ObjectVal(map[string]cty.Value{
				"sql":       cty.StringVal(scope.SQL),
				"canonical": cty.StringVal(scope.Canonical),
				"words":     wordList,
				"line":      cty.NumberIntVal(int64(scope.Line)),
			}),
			"file": cty.ObjectVal(map[string]cty.Value{
				"path":    cty.StringVal(scope.Path),
				"is_up":   cty.BoolVal(scope.IsUp),
				"is_down": cty.BoolVal(scope.IsDown),
			}),
			"dialect": cty.StringVal(scope.Dialect),
		},
	}
}
