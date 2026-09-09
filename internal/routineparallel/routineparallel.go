// Package routineparallel holds the single description of how a routine's
// PARALLEL level is folded before two sides of a comparison are matched.
//
// It is its own package for the reason [ptah.run/internal/rlspolicy] is: the
// declaration and the catalog spell the absent case differently, and every
// place that compares the two has to reach one answer rather than carrying its
// own. A declaration that states no level and a catalog reporting the server's
// default describe the same routine, and a comparison that called them
// different would plan a replacement on every run, forever.
package routineparallel

import "strings"

// Unsafe is the level PostgreSQL applies to a routine that declares none.
const Unsafe = "UNSAFE"

// Level folds a declared or observed PARALLEL level onto one spelling.
//
// An empty value becomes [Unsafe], which is what the server does with a
// routine that states no level: pg_proc.proparallel reports 'u' for it, and the
// declaration model's zero value is the empty string, so the two sides of one
// comparison meet here.
//
// The fold is for comparison only. It is deliberately not written back into
// either side: the renderer emits what its node carries, so a declaration that
// omitted the clause keeps rendering without one rather than having Ptah's
// normalization written into the author's DDL.
func Level(parallel string) string {
	folded := strings.ToUpper(strings.TrimSpace(parallel))
	if folded == "" {
		return Unsafe
	}
	return folded
}
