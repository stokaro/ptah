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

# Every module of ours declares the path its directory has under the vanity
# host: the root declares the bare host and a nested module appends its
# directory. That is how the go command finds a nested module of this
# repository. It asks the host, which names the repository, takes the rest of
# the import path as the directory, and reads that directory's go.mod at a tag
# named after it. A module that declares any other path cannot be fetched, and
# two modules cannot claim one identity. The provider SDK and the SQL syntax
# module are nested modules like any other: no module here has an identity of
# its own.
while IFS= read -r modfile; do
	declared="$(sed -n 's/^module //p' "$modfile" | head -1)"
	directory="$(dirname "$modfile")"
	want="${current_module}"
	if [ "$directory" != . ]; then
		want="${current_module}/${directory}"
	fi
	case "$declared" in
	example.com/*)
		# A fixture module that deliberately stands for someone else's code.
		foreign=$((foreign + 1))
		;;
	"$want")
		ours=$((ours + 1))
		;;
	"${current_module}" | "${current_module}"/*)
		echo "check-module-path: ${modfile} declares ${declared}" >&2
		echo "  a module in ${directory} must declare ${want}" >&2
		status=1
		;;
	*)
		echo "check-module-path: ${modfile} declares ${declared}" >&2
		echo "  every module here belongs to ${current_module}" >&2
		status=1
		;;
	esac
done < <(corpus '*go.mod')

# The provider SDK and the SQL syntax module are ptah.run/provider-sdk and
# ptah.run/sqlsyntax. Their releases up to provider-sdk/v0.39.0 and
# sqlsyntax/v0.1.0 declare paths under the repository's GitHub address, and the
# go command still resolves those paths to this repository. So an import or a
# requirement of one builds against a published copy of the module rather than
# the tree being edited -- the failure the first check above exists for.
#
# An old path may still name one of those releases, because they exist only
# there: the SDK's mixed-version tests build against two of them, and a reader
# pinned to one needs the path it declares. So an occurrence passes only as a
# release query, the module path or a package below it followed by @vX.Y.Z at
# or below the last release under that path, with any package path after the
# version (`<path>@v0.28.0/provider`). Every other spelling is refused: an
# import, a requirement, a module declaration, the path escaped for a regexp,
# a sentence that names it as current.
#
# The owner is assembled for the reason the retired token is: this file names no
# old path in full, so the scan covers it too.
old_owner="stokaro$(printf '\057')ptah"
old_hits="$(corpus -z | xargs -0 grep -naE -- "${old_owner}/(provider-sdk|sqlsyntax)" 2>/dev/null |
	awk -v owner="$old_owner" -v last='provider-sdk=0.39.0 sqlsyntax=0.1.0' '
		BEGIN {
			split(last, pairs, " ")
			for (i in pairs) {
				split(pairs[i], pair, "=")
				limit[pair[1]] = pair[2]
			}
		}
		# released reports whether token is a release query of an old path
		# at or below the last release under it.
		function released(token,   module, rest, version, want, have) {
			if (index(token, "@") == 0) {
				return 0
			}
			module = substr(token, length(owner) + 2)
			sub(/[\/@].*/, "", module)
			rest = token
			sub(/^[^@]*@/, "", rest)
			version = rest
			sub(/\/.*/, "", version)
			if (version !~ /^v0\.[0-9]+\.[0-9]+$/) {
				return 0
			}
			split(substr(version, 2), have, ".")
			split(limit[module], want, ".")
			return have[2] + 0 < want[2] + 0 || (have[2] + 0 == want[2] + 0 && have[3] + 0 <= want[3] + 0)
		}
		{
			rest = $0
			refused = 0
			while (match(rest, owner "/(provider-sdk|sqlsyntax)[A-Za-z0-9._/@+-]*")) {
				if (!released(substr(rest, RSTART, RLENGTH))) {
					refused = 1
				}
				rest = substr(rest, RSTART + RLENGTH)
			}
			if (refused) {
				print
			}
		}
	' || true)"
if [ -n "$old_hits" ]; then
	echo "check-module-path: an old path of the provider SDK or the SQL syntax module is named here:" >&2
	echo "$old_hits" | sed 's/^/  /' >&2
	echo "  import ${current_module}/provider-sdk and ${current_module}/sqlsyntax; an old path" >&2
	echo "  names only a release that declares it, such as github$(printf '\056')com/${old_owner}/provider-sdk@v0.39.0" >&2
	status=1
fi

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
	"${ours} of ${modules} modules declare their path under ${current_module}, ${foreign} stand for other code)"
