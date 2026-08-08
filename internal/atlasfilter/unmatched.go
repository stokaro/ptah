package atlasfilter

import (
	"os"
	"slices"
	"strconv"
)

// AllowUnmatchedExcludeEnvVar restores the permissive treatment of an
// --exclude selector that named no object: a warning instead of a refusal, on
// every verb.
//
// It is an environment variable and not a flag on purpose. The conformance
// cli-surface tier asserts that `ptah-compat` registers exactly the flags the
// pinned community binary registers, so a flag that binary does not have would
// break the very promise this surface exists to keep. Precedent and spelling:
// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar].
//
// The variable exists because refusing an unmatched selector is the safe
// default but not a free one: a shared exclude list reused across environments
// can legitimately name an object one of them does not have. That workflow
// keeps working on this same surface rather than being told to rewrite.
const AllowUnmatchedExcludeEnvVar = "PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE"

// AllowUnmatchedExclude reports whether the opt-in variable is set to a true
// boolean value. Unset, empty, false and unparsable values all keep the
// default.
func AllowUnmatchedExclude() bool {
	allow, err := strconv.ParseBool(os.Getenv(AllowUnmatchedExcludeEnvVar))
	return err == nil && allow
}

// UnmatchedAcrossStates intersects per-state [ExcludeReport]s into the
// selectors that matched nothing ANYWHERE.
//
// The intersection is the whole point. A comparison filters two states with
// one selector list, and a selector naming an object that exists on only one
// side matches on that side alone -- which is exactly what a CREATE or a DROP
// looks like. Reporting per state would fire on every such selector. Only a
// selector that named nothing in any state the command looked at is a scope
// that failed.
//
// A call with no reports returns nothing: there was no state to fail against.
func UnmatchedAcrossStates(reports ...ExcludeReport) []string {
	if len(reports) == 0 {
		return nil
	}
	unmatched := slices.Clone(reports[0].Unmatched)
	for _, report := range reports[1:] {
		unmatched = slices.DeleteFunc(unmatched, func(selector string) bool {
			return !slices.Contains(report.Unmatched, selector)
		})
	}
	if len(unmatched) == 0 {
		return nil
	}
	return unmatched
}
