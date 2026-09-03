package inference_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/inference"
)

// TestEveryVerbTakingBothASpecificationAndARunIsClassified is stokaro/ptah#2637
// made into a decision that has to be made rather than one made by omission.
//
// Nothing compared the specification a verb was handed with the generation the
// run was prepared for. The documented second-generation workflow leaves the
// run id alone — the guide derives it from a date, the quick start exports
// PTAH_RUN_ID — so a `backfill` under the new specification resumed the old,
// finished run and reported "3 scanned, 3 embedded" at exit 0 with no provider
// request made and no vector written.
//
// The verbs are walked rather than listed, and each one is in exactly one of
// two sets below. A verb added later is in neither, and this fails until
// somebody decides which — because "nobody thought about it" is how the gap got
// here.
func TestEveryVerbTakingBothASpecificationAndARunIsClassified(t *testing.T) {
	c := qt.New(t)

	verbs := verbsTakingASpecificationAndARun(inference.NewCommand())
	c.Assert(len(verbs) > 4, qt.IsTrue,
		qt.Commentf("the walk found %d verbs; a walk that finds none passes vacuously", len(verbs)))

	for _, verb := range verbs {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)

			_, guarded := generationMustMatch[verb]
			_, exempt := generationNeedNotMatch[verb]

			c.Assert(guarded, qt.Not(qt.Equals), exempt,
				qt.Commentf("%q is in neither set or in both; every verb taking a "+
					"specification and a run id decides whether they have to agree", verb))
		})
	}
}

// TestTheExemptionsSayWhy keeps the exempt set from becoming a place to put a
// verb nobody wanted to think about.
func TestTheExemptionsSayWhy(t *testing.T) {
	for verb, reason := range generationNeedNotMatch {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(len(reason) > 40, qt.IsTrue,
				qt.Commentf("%q is exempt with the reason %q", verb, reason))
		})
	}
}

// TestNeitherSetNamesAVerbThatIsNotThere is the other direction, and it is what
// keeps the two maps from becoming lists that were true when they were written.
//
// A verb renamed or removed leaves an entry behind, and an entry naming nothing
// makes the classification above look more complete than it is.
func TestNeitherSetNamesAVerbThatIsNotThere(t *testing.T) {
	c := qt.New(t)
	walked := make(map[string]bool)
	for _, verb := range verbsTakingASpecificationAndARun(inference.NewCommand()) {
		walked[verb] = true
	}

	for verb := range generationMustMatch {
		c.Assert(walked[verb], qt.IsTrue,
			qt.Commentf("%q is classified as needing agreement and takes no --run-id", verb))
	}
	for verb := range generationNeedNotMatch {
		c.Assert(walked[verb], qt.IsTrue,
			qt.Commentf("%q is exempt and takes no --run-id", verb))
	}
}

// generationMustMatch are the verbs that refuse a run prepared for another
// generation. Each is driven against a live database elsewhere; the value is
// the property being asserted, for a reader of this list.
var generationMustMatch = map[string]string{
	"prepare":  "registers a generation and adds columns to the user's table",
	"backfill": "resumes the run's cursor and writes the specification's identity",
	"catchup":  "the same, from the run's watermark",
	"index":    "builds the specification's index and marks the run's phase",
	"verify":   "measures the specification's rows against the run's watermark",
	"status": "renders watermarks using the selected consistency mode and measures " +
		"readiness against the selected generation",
	"cutover": "moves a pointer on the strength of a verification, and reaches " +
		"the check through the same verify the report comes from",
}

// generationNeedNotMatch are the verbs where the specification is how the
// database is reached and not what the verb is about.
var generationNeedNotMatch = map[string]string{
	"pause": "stops whatever the run is doing; a run driven with the wrong " +
		"specification is exactly the run an operator needs to be able to stop",
	"resume": "returns a paused run to running, for the same reason pause does",
}

// verbsTakingASpecificationAndARun is every leaf command registering both.
func verbsTakingASpecificationAndARun(root *cobra.Command) []string {
	var found []string
	for _, command := range root.Commands() {
		found = append(found, verbsTakingASpecificationAndARun(command)...)
	}
	return appendIfTakesBoth(found, root)
}

// appendIfTakesBoth adds root when it is a leaf registering both flags.
func appendIfTakesBoth(found []string, root *cobra.Command) []string {
	takesBoth := !root.HasSubCommands() &&
		root.Flags().Lookup("spec") != nil &&
		root.Flags().Lookup("run-id") != nil
	for range onlyWhen(takesBoth) {
		found = append(found, root.Name())
	}
	return found
}
