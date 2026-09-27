// Package undecidednote reports the declared objects a comparison withheld as
// undecided, on every surface that compares a desired schema with a database.
//
// A comparison withholds a declared object when the current side says it did
// not look at that kind -- a read the server refused, a selection that left the
// kind out, a target that cannot report it -- and the statement Ptah would
// render for the object cannot safely run against an unknown state. No
// statement is planned for such an object, so a surface that says nothing about
// it reports a plan that quietly does less than the author asked for. Every
// surface that compares prints the same warning through [Report], so a
// withheld object is explained in one set of words wherever it is met. A
// surface that summarizes says [Summary], and a surface that grades drift
// counts the objects through [Findings].
package undecidednote

import (
	"fmt"
	"io"

	"ptah.run/core/coverage"
	"ptah.run/migration/safety"
)

// FindingCategory is the category [Findings] reports undecided objects under.
const FindingCategory = "undecided"

// Findings grades the undecided objects the way drift grades a difference: one
// finding, counting them, at [safety.Warning]. It is empty when nothing was
// withheld.
//
// The severity is a warning because the check could not look, which a reader
// has to act on, and because nothing it could not see is a destructive change:
// a withheld object is one Ptah would create. So a drift threshold of "all"
// fails on one, and a threshold of "destructive" does not.
func Findings(undecided []coverage.Object) []safety.Finding {
	if len(undecided) == 0 {
		return nil
	}
	return []safety.Finding{{Category: FindingCategory, Count: len(undecided), Severity: safety.Warning}}
}

// Summary says how many declared objects could not be decided, as a clause a
// sentence can carry: "1 declared object could not be decided".
func Summary(count int) string {
	noun := "objects"
	if count == 1 {
		noun = "object"
	}
	return fmt.Sprintf("%d declared %s could not be decided", count, noun)
}

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
			Cause(object, currentDescription))
	}
}

// Cause says why the current side could not decide the object, in the most
// specific words the coverage record supports. currentDescription names the
// current side, as it does for [Report].
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
func Cause(object coverage.Object, currentDescription string) string {
	if clause := object.Explain(); clause != "" {
		return fmt.Sprintf("%s does not describe %s objects because %s",
			currentDescription, object.Kind, clause)
	}
	return fmt.Sprintf("%s records `%s %s`",
		currentDescription, coverage.DirectiveMarker, object.Kind)
}
