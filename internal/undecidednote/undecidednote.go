// Package undecidednote reports the declared objects a comparison withheld as
// undecided, on every surface that compares a desired schema with a database.
//
// A comparison withholds a declared object when the current side says it did
// not look at that kind -- a read the server refused, a selection that left the
// kind out, a target that cannot report it -- and the statement Ptah would
// render for the object cannot safely run against an unknown state. No
// statement is planned for such an object, so a surface that says nothing about
// it reports a plan that quietly does less than the author asked for. Every
// surface that compares prints the same warning through [Report] -- native
// `schema compare` and `migrations plan`, `schema apply` and `schema plan` on
// both surfaces, and the compatibility `schema diff` and `migrate diff` -- so a
// withheld object is explained in one set of words wherever it is met.
package undecidednote

import (
	"fmt"
	"io"

	"ptah.run/core/coverage"
)

// Report names every object the comparison declined to plan a creation for
// because the CURRENT side's coverage record says it does not describe that
// kind and the creation Ptah would emit cannot safely converge from an unknown
// current state. That includes an unguarded creation, and a guarded PostgreSQL
// extension whose existing installation may be in another schema.
//
// currentDescription and desiredDescription name the two command-specific
// inputs in prose, so `schema diff` can say `--from`, `migrate diff` can name
// the replayed migration directory, and a verb whose current side is always a
// database can say so without producing a misleading diagnostic. diagnostics
// may be nil, which drops the report.
func Report(
	diagnostics io.Writer,
	undecided []coverage.Object,
	currentDescription,
	desiredDescription string,
) {
	if diagnostics == nil {
		return
	}
	for _, object := range undecided {
		_, _ = fmt.Fprintf(diagnostics,
			"Warning: %s %q is declared by %s but no change was planned for it:"+
				" %s, so this comparison cannot tell it apart from one that already exists,"+
				" and the creation Ptah renders for it cannot safely converge from an unknown current state.\n",
			object.Kind, object.Name, desiredDescription,
			cause(object, currentDescription))
	}
}

// cause says why the current side could not decide the object, in the most
// specific words the coverage record supports.
//
// A record that carries a reason gets the sentence that reason is for: a
// refused catalog, a selection, a target that has no such objects and a
// compatibility policy are four different problems with four different answers,
// and a user who is told only that something was held back cannot tell which of
// them they are looking at (stokaro/ptah#1346).
//
// A record that carries no reason -- what a hand-authored `ptah:not-described`
// line is -- gets the directive quoted back instead, because that line is all
// the description said and it is what the user will search the document for.
func cause(object coverage.Object, currentDescription string) string {
	if clause := object.Explain(); clause != "" {
		return fmt.Sprintf("%s does not describe %s objects because %s",
			currentDescription, object.Kind, clause)
	}
	return fmt.Sprintf("%s records `%s %s`",
		currentDescription, coverage.DirectiveMarker, object.Kind)
}
