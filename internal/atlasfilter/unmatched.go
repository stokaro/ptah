package atlasfilter

import (
	"slices"

	"go.5x5.cz/ptah/internal/envbool"
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

// allowUnmatchedExclude is the declaration of the variable, made once, in the
// package that owns it. See [go.5x5.cz/ptah/internal/envbool].
var allowUnmatchedExclude = envbool.New(AllowUnmatchedExcludeEnvVar, false)

// AllowUnmatchedExclude reports whether the opt-in restores the permissive
// treatment of an --exclude selector that named no object.
//
// Unset keeps the refusal and a valid false spelling keeps it too; an empty or
// unparsable value is a configuration error. A shared exclude list is exactly
// the setting where the variable is exported once and read on every run, so a
// typo in it must not read as "refusal still on" while the operator believes
// they turned it off (stokaro/ptah#1334).
func AllowUnmatchedExclude() (bool, error) {
	return allowUnmatchedExclude.Resolve()
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
