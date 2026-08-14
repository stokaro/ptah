#!/usr/bin/env sh
# select_test_files prints every test file a repository gate is allowed to judge,
# one per line, relative to the repository root. Source this file; it defines the
# function and nothing else.
#
# No gate is ever handed a directory to walk. A filesystem walk prunes by
# directory *name* only (.git, vendor, node_modules, testdata), and the root of a
# linked git worktree is an ordinary directory whose `.git` is a regular *file*
# holding a `gitdir:` pointer -- nothing prunes it. The walk therefore descended
# into every checkout parked under the repo and reported its tests as violations
# of this repo's rules, so a gate went red for code that is not in the working
# tree at all. `git worktree list` in this repository routinely shows more than a
# hundred linked worktrees under .claude/worktrees and .codex/worktrees, so this
# is the normal case here, not an edge case.
#
# git is the authority on what belongs to this checkout: it refuses to descend
# past a nested `.git` marker, so no worktree path can appear here regardless of
# ignore rules.
#
#   --cached                    tracked files
#   --others --exclude-standard brand-new local test files, so a gate still fires
#                               before `git add`. Dropping these would make a gate
#                               green on the very file the author is about to
#                               commit.
#   core.quotePath=false        emit non-ASCII paths raw instead of C-quoted, so
#                               they survive line-based reading
#
# A future submodule would appear here as a gitlink rather than as its files; it
# would need a scan of its own.
select_test_files() {
	git -c core.quotePath=false ls-files --cached --others --exclude-standard -- '*_test.go'
}
