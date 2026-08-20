package atlashcl

import (
	"maps"

	"github.com/zclconf/go-cty/cty/function"
)

// ProjectFunctions returns the function set an Atlas PROJECT file is evaluated
// against, before the caller overlays the names it binds itself.
//
// It is the schema evaluator's set, exported rather than duplicated. The two
// evaluators reading different sets is what stokaro/ptah#1810 is about: an
// expression like `join(",", var.schemas)` evaluated in a schema file and
// refused in `atlas.hcl` is one language with two vocabularies, and a project
// file is the place a list of schemas is most likely to be assembled.
//
// Three names are the caller's to supply, and this set deliberately contains
// none of them: `file` and `fileset` read a filesystem, and `getenv` reads the
// environment. Which filesystem is the caller's question -- a project file
// resolves paths against the project directory, a schema file against its own
// -- so binding them here would answer it wrongly for somebody. See
// [ProjectBoundFunctionNames].
//
// `print` is deliberately NOT in the set, and it is the one name whose absence
// is a decision rather than an omission. It returns its argument and writes the
// value to stdout, and a project file is the one place a `sensitive = true`
// variable exists -- so `print(var.token)` would put a credential in the
// command's output and in CI logs, before any diagnostic redaction could run.
// Measured on an earlier revision of this change, it printed the token
// verbatim.
//
// Binding it sensitively was the alternative. A function sees a VALUE, not the
// expression that produced it, so enforcing sensitivity there would mean
// threading the parser's variable registry into a function binding -- the exact
// coupling this split exists to avoid. Omitting it costs a debug tap in a file
// that is evaluated once at startup, which is also before the command has
// decided whether its output is machine-readable.
func ProjectFunctions() map[string]function.Function {
	fns := schemaFunctions(projectPrintLine)
	delete(fns, "print")
	return fns
}

// ProjectBoundFunctionNames are the functions [ProjectFunctions] leaves to the
// caller because their meaning depends on where the file being evaluated lives.
//
// It exists so the overlay can be asserted rather than assumed: a test can
// check that each of these names in an assembled set is the caller's binding
// and not a shared one, which is the failure that would send `file("x.sql")`
// looking in the wrong directory.
var ProjectBoundFunctionNames = []string{"file", "fileset", "getenv"}

// WithProjectBoundFunctions returns shared overlaid with bound, so a name in
// both resolves to the caller's binding.
//
// The direction is the whole point of the helper. Copying the other way would
// leave `file` reading whatever directory the shared set happened to bind, and
// nothing about the result would look wrong -- the function would exist, take
// the right arguments, and return the contents of a different file.
func WithProjectBoundFunctions(
	shared, bound map[string]function.Function,
) map[string]function.Function {
	combined := make(map[string]function.Function, len(shared)+len(bound))
	maps.Copy(combined, shared)
	maps.Copy(combined, bound)
	return combined
}

// projectPrintLine exists only to satisfy schemaFunctions' signature; the
// `print` it binds is removed from the set before it is returned, so nothing
// calls this. It panics rather than printing, so a future change that puts
// `print` back cannot do it silently.
func projectPrintLine(string) {
	panic("print is not available in a project file; see ProjectFunctions")
}
