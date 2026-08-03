// Package teststyleguard hosts the self-test for the test-style gate
// (scripts/check-test-style.sh).
//
// The gate used to hand the scanner a directory to walk. A filesystem walk
// prunes by directory *name*, and the root of a linked git worktree is an
// ordinary directory whose `.git` is a regular *file*, so the walk descended
// into every checkout parked under the repository and judged its tests against
// this repository's baseline. The gate went red for code that is not in the
// working tree, and the documented baseline regeneration captured those foreign
// paths into the tracked baseline.
//
// The fix sources the scanned paths from git, which refuses to descend past a
// nested `.git` marker. The obvious wrong fix -- pruning the walk by name until
// the worktrees stop being reported -- looks identical on a clean tree, because
// a gate that selects nothing is also green. So the test asserts both
// directions: a linked worktree's test must not be selected, and a tracked file,
// a never-staged new file, and the real repository's several hundred test files
// must all still be selected.
//
// The test drives the shipped script through its `--list-scan-paths` mode
// against a git repository it builds in a temporary directory at run time, so it
// exercises the gate's real path source rather than a Go reimplementation of it.
// The package carries no runtime code of its own.
package teststyleguard
