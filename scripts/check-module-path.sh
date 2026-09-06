#!/usr/bin/env bash
# The module path is `ptah.run`, and the retired one is gone.
#
# This is the second rename, and the lesson the first one left is not about
# imports: a consumer pinned to a path nothing publishes to any more cannot be
# BUMPED, only rewritten, and `stokaro/ptah-atlas-conformance` spent weeks
# measuring a frozen build because of it.
#
# Inside this tree the failure is quieter. An import of the old path still
# compiles -- the vanity host still answers -- so the module proxy fetches a
# published copy of Ptah and the build succeeds against a DIFFERENT tree than
# the one being edited. A test can pass against last month's code with nothing
# red anywhere, which is why a one-time sweep is not the guarantee here and a
# gate is.
#
# Both spellings are refused: the module path, and the bare vanity host without
# it. The sweep for the rename found 9485 of the first and missed the single
# occurrence of the second, in a sentence about what a workflow resolves. This
# file names neither, for the reason stated above -- including in its prose,
# which is where the first draft of it failed its own rule.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Assembled rather than written out, for the reason check-docs-origin.sh states
# about itself: a scanner that spells what it forbids matches its own source,
# and the alternative is an exemption list that silently excuses whatever gets
# added to it. Assembling it lets the scan cover every file including this one.
# What is refused is the OLD ORGANISATION's token, in any spelling, rather than
# one host. The retired domain and the current one belong to different entities,
# and the token turned up in three unrelated shapes: the Go module host, an OCI
# annotation namespace in reverse DNS, and an MCP `_meta` key written host-first
# with a path. A rule aimed at the module host alone would have moved the
# imports and left two wire identifiers naming the other entity.
#
# It is a regular expression rather than a fixed string because the host is also
# written with each dot backslash-escaped for a regexp. A fixed-string search
# finds only the plain spelling, and seven of the escaped one survived the
# rename's own sweep -- in two CI workflows, three test assertions, and one GATE
# whose grep would then have matched nothing and reported success over an empty
# set.
retired_token="5$(printf '\170')5"
current_module="ptah$(printf '\056')run"

# corpus is the working tree, asked of git rather than walked: a walk descends
# into the linked worktrees parked under this repository and reports another
# checkout's files. `--others --exclude-standard` is what makes it the working
# tree rather than the index, so a file added by the change under review is
# seen -- a gate reading `git ls-files` alone cannot fail on the one file most
# likely to be wrong (stokaro/ptah#2884).
corpus() {
	git ls-files --cached --others --exclude-standard "$@"
}

status=0
ours=0
foreign=0

hits="$(corpus -z | xargs -0 grep -nF -- "$retired_token" 2>/dev/null || true)"
if [ -n "$hits" ]; then
	echo "check-module-path: the retired organisation is still named here:" >&2
	echo "$hits" | sed 's/^/  /' >&2
	echo "  the module, the OCI annotations and the MCP _meta key all belong to" >&2
	echo "  ${current_module} now; see stokaro/ptah#2943" >&2
	status=1
fi

# Every module in the tree declares the current path. Discovered rather than
# listed, so a module added later is covered by existing.
while IFS= read -r modfile; do
	declared="$(sed -n 's/^module //p' "$modfile" | head -1)"
	case "$declared" in
	"${current_module}" | "${current_module}"/*)
		ours=$((ours + 1))
		;;
	example.com/*)
		# A fixture module that deliberately stands for someone else's code.
		foreign=$((foreign + 1))
		;;
	*)
		echo "check-module-path: ${modfile} declares ${declared}" >&2
		echo "  every module here belongs to ${current_module}" >&2
		status=1
		;;
	esac
done < <(corpus '*go.mod')

# A scan for an absent string reports the same success whether it read the
# repository or read nothing, so the corpus is asserted rather than assumed.
files="$(corpus | wc -l | tr -d ' ')"
modules="$(corpus '*go.mod' | wc -l | tr -d ' ')"
if [ "$files" -lt 100 ] || [ "$modules" -lt 2 ]; then
	echo "check-module-path: read ${files} files and ${modules} modules;" >&2
	echo "  that is too few to be this repository, so the scan is not reporting on it" >&2
	exit 1
fi

[ "$status" -eq 0 ] || exit 1
# The counts are separate because they are different claims. A run that folded
# the fixture module into the total would report more agreement than it measured.
echo "check-module-path: OK (${files} files name no retired organisation;" \
	"${ours} of ${modules} modules declare ${current_module}, ${foreign} stand for other code)"
