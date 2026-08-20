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
// `print` is included, and it is the one entry with a side effect. It writes
// through the same destination the schema evaluator uses; a project file that
// calls it prints while the project is being read, which is when an operator
// debugging that file wants to see it.
func ProjectFunctions() map[string]function.Function {
	return schemaFunctions(projectPrintLine)
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

// projectPrintLine writes a `print` call's line while a project file is being
// evaluated.
//
// Unlike the schema evaluator's, it has no emitting guard to consult: a project
// file is evaluated once, so there is no second walk for a line to be repeated
// on.
func projectPrintLine(line string) {
	writePrintLine(line)
}
