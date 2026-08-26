#!/usr/bin/env bash
# Proves that each gate below can still fail.
#
# A gate stops gating quietly. It does not error -- it discovers zero inputs and
# reports the same success it reports on a clean tree, which is why several of
# them say so in their own headers: "a gate that compares nothing to nothing
# reports success at exactly the moment it stopped working."
#
# Eleven gates could already prove otherwise -- three shell ones carry a
# `-selftest.sh` companion and eight `check-*.mjs` take `--selftest`. The rest
# carried nothing, and an audit that broke each rule by hand is a photograph:
# nothing keeps it true (stokaro/ptah#1923).
#
# This is the single-harness shape that issue proposes, with its violations as
# the fixtures. Each one breaks the rule the gate states, runs the gate, and
# requires a non-zero exit. A gate that passes its own broken fixture is a gate
# that has stopped reading something.
#
# The mutations run in a throwaway git worktree, never in the caller's tree.
# That is not tidiness: several gates read the whole repository through
# `git rev-parse --show-toplevel`, so they have to see a real checkout, and a
# harness that edited the caller's files would leave them edited when a gate
# exits non-zero -- which is the expected outcome here, not the exception.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

worktree=""
cleanup() {
	if [ -n "$worktree" ] && [ -d "$worktree" ]; then
		git worktree remove --force "$worktree" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

worktree="$(mktemp -d "${TMPDIR:-/tmp}/ptah-gate-selftest.XXXXXX")"
rmdir "$worktree"
git worktree add --detach "$worktree" HEAD >/dev/null 2>&1

failures=0
checked=0
# fixtured accumulates the gates run_case was given, so the coverage guard at
# the end reads what actually ran rather than a second list beside it.
fixtured=()

# run_case breaks one rule and requires the gate to notice.
#
# The mutation is a shell snippet run inside the worktree. Restoring is
# `git checkout` plus `git clean`, because some mutations add files and
# `checkout` alone leaves those behind -- a leftover from one case would then
# decide the next one.
run_case() {
	local gate="$1" description="$2" mutation="$3"
	checked=$((checked + 1))
	fixtured+=("$gate")

	if ! (cd "$worktree" && bash "scripts/${gate}" >/dev/null 2>&1); then
		echo "check-gate-selftests: ${gate} fails on an UNMODIFIED tree; the fixture below proves nothing" >&2
		failures=$((failures + 1))
		return
	fi

	(cd "$worktree" && eval "$mutation")
	local status=0
	(cd "$worktree" && bash "scripts/${gate}" >/dev/null 2>&1) || status=$?
	(cd "$worktree" && git checkout -- . >/dev/null 2>&1 && git clean -fdq >/dev/null 2>&1)

	if [ "$status" -eq 0 ]; then
		echo "check-gate-selftests: ${gate} PASSED with ${description}" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %-40s %s\n' "$gate" "$description"
}

echo "check-gate-selftests: breaking each gate's own rule and requiring it to notice"

# The path is assembled rather than written out, because check-repository-local-
# paths.sh reads this file too and would find its own fixture. Its exclusion
# list names only the gate itself, which is the right list: a harness that had
# to be added to it would be a harness nobody could tell from a real leak.
home_root="/Users"
run_case check-repository-local-paths.sh \
	"an absolute developer path in README" \
	"printf '\n    %s/somebody/Work/ptah/schema.sql\n' \"${home_root}\" >>README.md"

run_case check-adr-index.sh \
	"a second record taking a number another one already has" \
	"printf '# ADR 0001: A second record\n' >docs/adr/0001-a-second-record.md"

run_case check-go-toolchain-single-source.sh \
	"a Go version literal pinned in a workflow" \
	"perl -0pi -e 's/go-version-file: go.mod/go-version: \"1.26.0\"/' .github/workflows/go-unit-tests.yml"

run_case check-go-module-lint-coverage.sh \
	"working-directory for the nested module removed from one job" \
	"perl -0pi -e 's|working-directory: examples/orm-loaders/gorm||' .github/workflows/go-lint.yml"

run_case check-version-matrix.sh \
	"a declared release line deleted from the documented block" \
	"perl -0pi -e 's/^\|\s\`postgres\`\s\|\s18[^\n]*\n//m' docs/site/src/content/docs/databases/support-matrix.md"

run_case check-lint-rules.sh \
	"a heading removed from the generated block" \
	"perl -0pi -e 's/^## Identifier families\n//m' docs/site/src/content/docs/reference/lint-rules.md"

run_case check-test-style.sh \
	"a conditional added to a test function" \
	"printf '\nfunc TestGateSelftestConditional(t *testing.T) {\n\tif t.Name() == \"\" {\n\t\tt.Fatal(\"unreachable\")\n\t}\n}\n' >>internal/dbtarget/dbtarget_test.go"

run_case check-public-api-snapshot.sh \
	"an exported field added to a documented struct" \
	"perl -0pi -e 's/type DomainExpression struct \{/type DomainExpression struct {\n\t\/\/ GateSelftestField exists only inside this fixture.\n\tGateSelftestField string\n/' config/config.go"

run_case check-capability-tables.sh \
	"a key removed from the generated capability table" \
	"perl -0pi -e 's/^\| \`advisory_locks\`[^\n]*\n//m' docs/capabilities.md"

run_case check-agent-surface.sh \
	"a verb row removed from the generated agent-surface table" \
	"perl -0pi -e 's/^.*introspects the database and prints what it found.*\\n//m' docs/agent-surface.md"

run_case check-public-api.sh \
	"an exported package with no doc comment" \
	"mkdir -p gateselftest && printf 'package gateselftest\n\nfunc Exported() {}\n' >gateselftest/gateselftest.go"

# The ledger scrape reads list items only. This fixture documents the new
# package in a prose paragraph rather than a bullet: a scrape that let prose
# mentions join the allowlist would accept it and pass (stokaro/ptah#2246).
run_case check-public-api.sh \
	"a package whose only ledger mention is a prose paragraph" \
	'mkdir -p gateselftest && printf "package gateselftest\n\nfunc Exported() {}\n" >gateselftest/gateselftest.go && printf "\nA prose paragraph that mentions \`go.5x5.cz/ptah/gateselftest\` is not a listing.\n" >>docs/public_api.md'

run_case check-exported-docs.sh \
	"a doc comment is deleted from an exported function on the stable surface" \
	"perl -0pi -e 's{// NormalizeDialect folds.*?\nfunc NormalizeDialect}{func NormalizeDialect}s' core/platform/constants.go"

run_case check-public-api-docs-sync.sh \
	"a ledger package's row removed from the site's stable-packages table" \
	"perl -0pi -e 's/^\| \`core\/coverage\`[^\n]*\n//m' docs/site/src/content/docs/extend/public-api.md"

run_case check-public-api-docs-sync.sh \
	"a package the site table lists deleted from the ledger" \
	"perl -0pi -e 's/^- \`go\.5x5\.cz\/ptah\/migration\/seeder\`\n//m' docs/public_api.md"

# What this harness does NOT cover, and why. A data table rather than a
# paragraph, because the guard at the end of this file reads it: a coverage list
# nobody checks is the same failure mode the harness exists to prevent --
# silence that reads as completeness (stokaro/ptah#1923).
uncovered=(
	"check-coverage.sh	runs the whole test suite; minutes per fixture"
	"check-public-api-released.sh	resolves the published module over the network"
	"check-documented-install.sh	downloads the published binaries"
	"check-api-export-acceptance.sh	needs a built binary the throwaway worktree has none of"
	"check-hcl-export-acceptance.sh	the same"
	"check-protobuf-export-acceptance.sh	the same"
)

# companion_gates lists the gates that prove themselves, derived from the files
# on disk rather than written down: a `-selftest.sh` beside a gate IS the
# statement that the gate can fail, and a hand-written copy of that list is what
# falls behind when the next companion lands. It already had: this list named
# two companions while the tree carried three.
companion_gates() {
	local companion base
	for companion in scripts/check-*-selftest.sh; do
		[ -e "$companion" ] || continue
		base="$(basename "$companion")"
		echo "${base%-selftest.sh}.sh"
	done
}

echo
echo "  not covered here, with the reason:"
for entry in "${uncovered[@]}"; do
	printf '    %-40s %s\n' "${entry%%	*}" "${entry##*	}"
done
for gate in $(companion_gates); do
	printf '    %-40s %s\n' "$gate" "carries its own -selftest.sh companion"
done
printf '    %-40s %s\n' "check-*.mjs" "take --selftest and run it in the docs job"

# The coverage guard. Every shell gate has to be in one of the three lists
# above, or this harness reports a number that means less than it looks like:
# "9 gates each failed on their own broken rule" says nothing about the tenth
# that nobody wired in.
unlisted=""
guarded=0
for path in scripts/check-*.sh; do
	gate="$(basename "$path")"
	case "$gate" in
	*-selftest.sh | check-gate-selftests.sh) continue ;;
	esac
	guarded=$((guarded + 1))
	listed=""
	for covered in "${fixtured[@]}" $(companion_gates); do
		[ "$covered" = "$gate" ] && listed="yes"
	done
	for entry in "${uncovered[@]}"; do
		[ "${entry%%	*}" = "$gate" ] && listed="yes"
	done
	[ -n "$listed" ] || unlisted="${unlisted} ${gate}"
done

echo
if [ -n "$unlisted" ]; then
	echo "check-gate-selftests: no fixture, no companion and no reason for:${unlisted}" >&2
	echo "  add a run_case for it, give it a -selftest.sh, or say in uncovered why not" >&2
	exit 1
fi
if [ "$failures" -gt 0 ]; then
	echo "check-gate-selftests: ${failures} of ${checked} gates did not notice their own broken rule" >&2
	exit 1
fi
echo "check-gate-selftests: OK (${checked} gates each failed on their own broken rule," \
	"${guarded} shell gates all accounted for)"
