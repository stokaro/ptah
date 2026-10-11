// Package hcldiag makes the HCL library's diagnostics deterministic before
// they become messages.
//
// The library suggests a function for a call to an unknown one by walking the
// evaluation context's function table, a map, and naming the first function
// close enough in that walk. Which function it names changed between runs of
// one binary: `now()` was answered with `Did you mean "pow"?` on one run and
// `Did you mean "log"?` on the next. [Stable] names the nearest function
// instead, and the first in name order among equally near ones, in the
// library's own wording. The Atlas schema reader and the atlas.hcl reader
// both pass their diagnostics through it.
package hcldiag

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// unknownFunction is the summary the library gives a call to a function the
// evaluation context does not hold.
const unknownFunction = "Call to unknown function"

// suggestionLimit is the library's threshold: a function is suggested only
// when fewer than this many edits turn the name written into it.
const suggestionLimit = 3

// Stable returns diags with the suggestion of every call to an unknown
// function recomputed in a fixed order. Every other diagnostic, and the
// wording of the rewritten ones, is the library's. diags is not modified.
func Stable(diags hcl.Diagnostics) hcl.Diagnostics {
	if len(diags) == 0 {
		return diags
	}
	stable := make(hcl.Diagnostics, len(diags))
	for i, diag := range diags {
		stable[i] = diag
		detail, ok := unknownFunctionDetail(diag)
		if !ok {
			continue
		}
		rewritten := *diag
		rewritten.Detail = detail
		stable[i] = &rewritten
	}
	return stable
}

// unknownFunctionDetail is the library's detail for a call to an unknown
// function, with the suggestion chosen by [nearest].
func unknownFunctionDetail(diag *hcl.Diagnostic) (string, bool) {
	if diag == nil || diag.Summary != unknownFunction || diag.EvalContext == nil {
		return "", false
	}
	call, ok := diag.Expression.(*hclsyntax.FunctionCallExpr)
	if !ok {
		return "", false
	}
	available := slices.Sorted(maps.Keys(diag.EvalContext.Functions))
	if separator := strings.LastIndex(call.Name, "::"); separator != -1 {
		namespace, name := call.Name[:separator+2], call.Name[separator+2:]
		available = slices.DeleteFunc(available, func(candidate string) bool { return !strings.HasPrefix(candidate, namespace) })
		if len(available) == 0 {
			return "", false
		}
		// The library compares the name without its namespace against the
		// namespaced candidates, and prints the namespace before the one it
		// picks. Both are kept, so only the choice changes.
		suggestion := nearest(name, available)
		if suggestion != "" {
			suggestion = fmt.Sprintf(" Did you mean %s%s?", namespace, suggestion)
		}
		return fmt.Sprintf("There is no function named %q in namespace %s.%s", name, namespace, suggestion), true
	}
	suggestion := nearest(call.Name, available)
	if suggestion != "" {
		suggestion = fmt.Sprintf(" Did you mean %q?", suggestion)
	}
	return fmt.Sprintf("There is no function named %q.%s", call.Name, suggestion), true
}

// nearest returns the candidate the fewest edits away from given, within the
// library's threshold, and the first in candidates' order among equally near
// ones. It returns the empty string when none is close enough.
func nearest(given string, candidates []string) string {
	best, bestDistance := "", suggestionLimit
	for _, candidate := range candidates {
		if distance := editDistance(given, candidate); distance < bestDistance {
			best, bestDistance = candidate, distance
		}
	}
	return best
}

// editDistance is the Levenshtein distance between a and b in runes, every
// insertion, deletion and substitution costing one, as the library counts.
func editDistance(a, b string) int {
	source, target := []rune(a), []rune(b)
	previous := make([]int, len(target)+1)
	current := make([]int, len(target)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(source); i++ {
		current[0] = i
		for j := 1; j <= len(target); j++ {
			cost := 1
			if source[i-1] == target[j-1] {
				cost = 0
			}
			current[j] = min(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(target)]
}
