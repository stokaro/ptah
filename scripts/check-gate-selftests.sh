#!/usr/bin/env bash
# Proves that each gate below can still fail.
#
# A gate stops gating quietly. It does not error -- it discovers zero inputs and
# reports the same success it reports on a clean tree, which is why several of
# them say so in their own headers: "a gate that compares nothing to nothing
# reports success at exactly the moment it stopped working."
#
# Some gates could already prove otherwise -- three shell ones carry a
# `-selftest.sh` companion and twelve of the thirteen `check-*.mjs` take
# `--selftest`. The rest carried nothing, and an audit that broke each rule by
# hand is a photograph: nothing keeps it true (stokaro/ptah#1923).
#
# A `--selftest` is not by itself a proof either, and the gap is worth naming
# because the docs job cannot see it: a self-test reduced to a bare
# `OK (0 assertions)` still exits 0, so `npm run check:*:selftest` reports the
# same success whether the gate asserts everything or nothing. What can tell
# them apart is breaking the rule the self-test covers and requiring the
# SELF-TEST to go red -- a self-test that asserts nothing stays green under that
# mutation. run_node_selftest_case below does exactly that.
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
# mjs_fixtured does the same for the `.mjs` gates, which have their own guard.
fixtured=()
mjs_fixtured=()

# run_case breaks one rule and requires the gate to notice.
#
# The mutation is a shell snippet run inside the worktree. Restoring is
# `git reset --hard` plus `git clean`, because a mutation can reach the index as
# well as the working tree and can add files: `git checkout -- .` restores the
# tree FROM the index, so a staged deletion survives it, and `clean` is what
# removes an added file that would otherwise decide the next case. A route gate
# enumerates pages with `git ls-files --cached`, so staging is how a fixture
# retires a page at all, which is what makes the difference load-bearing here.
restore_worktree() {
	(cd "$worktree" && git reset -q --hard HEAD >/dev/null 2>&1 && git clean -fdq >/dev/null 2>&1)
}

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
	restore_worktree

	if [ "$status" -eq 0 ]; then
		echo "check-gate-selftests: ${gate} PASSED with ${description}" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %-40s %s\n' "$gate" "$description"
}

# run_node_gate_case is run_case for a `.mjs` gate under docs/site/scripts. The
# gate is invoked directly rather than through npm, because the throwaway
# worktree has no node_modules -- and these gates deliberately have no npm
# dependency, which is the property that lets them be exercised here at all.
run_node_gate_case() {
	local script="$1" description="$2" mutation="$3"
	local gate
	gate="$(basename "$script")"
	checked=$((checked + 1))
	mjs_fixtured+=("$gate")

	if ! (cd "$worktree" && node "docs/site/scripts/${script}" >/dev/null 2>&1); then
		echo "check-gate-selftests: ${gate} fails on an UNMODIFIED tree; the fixture below proves nothing" >&2
		failures=$((failures + 1))
		return
	fi

	(cd "$worktree" && eval "$mutation")
	local status=0
	(cd "$worktree" && node "docs/site/scripts/${script}" >/dev/null 2>&1) || status=$?
	restore_worktree

	if [ "$status" -eq 0 ]; then
		echo "check-gate-selftests: ${gate} PASSED with ${description}" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %-40s %s\n' "$gate" "$description"
}

# run_node_selftest_case breaks a rule and requires the gate's own `--selftest`
# to notice, which is the only thing that distinguishes a self-test still
# reading from one reduced to a bare `OK`. The docs job runs these self-tests
# and reads their exit code; nothing there reads whether they assert.
run_node_selftest_case() {
	local script="$1" description="$2" mutation="$3"
	local gate
	gate="$(basename "$script")"
	checked=$((checked + 1))
	mjs_fixtured+=("$gate")

	if ! (cd "$worktree" && node "docs/site/scripts/${script}" --selftest >/dev/null 2>&1); then
		echo "check-gate-selftests: ${gate} --selftest fails on an UNMODIFIED tree; the fixture below proves nothing" >&2
		failures=$((failures + 1))
		return
	fi

	(cd "$worktree" && eval "$mutation")
	local status=0
	(cd "$worktree" && node "docs/site/scripts/${script}" --selftest >/dev/null 2>&1) || status=$?
	restore_worktree

	if [ "$status" -eq 0 ]; then
		echo "check-gate-selftests: ${gate} --selftest PASSED with ${description}" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %-40s %s\n' "${gate} --selftest" "$description"
}

# run_shell_selftest_case is run_node_selftest_case for a shell gate: it breaks
# the rule in the code the gate's `--selftest` covers and requires that
# self-test to notice. Nothing else can tell a self-test that still asserts from
# one reduced to a bare `OK (0 assertions)`, and the workflow reads only the
# exit code.
run_shell_selftest_case() {
	local gate="$1" description="$2" mutation="$3"
	checked=$((checked + 1))
	fixtured+=("$gate")

	if ! (cd "$worktree" && bash "scripts/${gate}" --selftest >/dev/null 2>&1); then
		echo "check-gate-selftests: ${gate} --selftest fails on an UNMODIFIED tree; the fixture below proves nothing" >&2
		failures=$((failures + 1))
		return
	fi

	(cd "$worktree" && eval "$mutation")
	local status=0
	(cd "$worktree" && bash "scripts/${gate}" --selftest >/dev/null 2>&1) || status=$?
	restore_worktree

	if [ "$status" -eq 0 ]; then
		echo "check-gate-selftests: ${gate} --selftest PASSED with ${description}" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %-40s %s\n' "${gate} --selftest" "$description"
}

echo "check-gate-selftests: breaking each gate's own rule and requiring it to notice"

# The path is assembled rather than written out, because check-repository-local-
# paths.sh reads this file too and would find its own fixture. Its exclusion
# list names only the gate itself, which is the right list: a harness that had

run_case check-go-module-lint-coverage.sh \
	"working-directory for the nested module removed from one job" \
	"perl -0pi -e 's|working-directory: examples/orm-loaders/gorm||' .github/workflows/go-lint.yml"



run_case check-goreleaser-artifact-names.sh \
	"the snapshot version taken from whatever tag git describe reaches" \
	"perl -0pi -e 's|version_template: \"0\\.0\\.0-SNAPSHOT-\\{\\{ \\.ShortCommit \\}\\}\"|version_template: \"{{ .Version }}-SNAPSHOT-{{ .ShortCommit }}\"|' .goreleaser.yaml"

run_case check-brand-assets.sh \
	"the favicon drifting away from the header logo" \
	"perl -0pi -e 's/fill=\"#f59e0b\"/fill=\"#38bdf8\"/' docs/site/public/favicon.svg"

# The same edit applied to both files, so the drift rule stays satisfied and only
# the legibility one can fire. A gate whose two rules are only ever broken
# together cannot say which of them it still reads.
run_case check-brand-assets.sh \
	"two courses moved closer than one favicon pixel, in both files" \
	"perl -0pi -e 's/y=\"27\"/y=\"25\"/' docs/site/src/assets/logo.svg docs/site/public/favicon.svg"

run_case check-test-style.sh \
	"a conditional added to a test function" \
	"printf '\nfunc TestGateSelftestConditional(t *testing.T) {\n\tif t.Name() == \"\" {\n\t\tt.Fatal(\"unreachable\")\n\t}\n}\n' >>internal/dbtarget/dbtarget_test.go"

run_case check-public-api-snapshot.sh \
	"an exported field added to a documented struct" \
	"perl -0pi -e 's/type DomainExpression struct \{/type DomainExpression struct {\n\t\/\/ GateSelftestField exists only inside this fixture.\n\tGateSelftestField string\n/' config/config.go"



# Two fixtures, one per comparison mode. The command reference is three marker
# blocks and one whole page, and a gate whose two rules are only ever broken
# together cannot say which of them it still reads.
#
# Six fixtures on this one gate rather than one, for the same reason: docsync
# checks eleven blocks from four generators, and a single fixture would leave
# ten of them proven by nothing.
#
# Neither mutation writes a backtick or a dollar sign. The snippet is expanded
# once by the caller and evaluated again by run_case, and a code span in a
# generated table row would be command substitution on the second pass. `.`
# matches the backtick in the pattern and perl's `\x60` writes one in the
# replacement.


# The derived feature inventory. Three fixtures, because the gate's rules fail
# in three different places and only the first leaves a diff anyone would see.
#
# A row deleted from the committed artifact is the shape a merge produces, and
# the byte comparison is the whole of the gate's coverage of every derived
# column: kind, surface and identifier are checked by that one comparison and by
# nothing else.

# The one hand-written datum in the system. A page claiming an identifier the
# derivation does not produce is the mistake the inverted direction exists to
# catch: the closed attempt searched pages for a feature and credited any page
# containing any token of its name.

# The coverage floor. Removing an `owns:` line rewrites the artifact AND drops
# the claimed count below the floor, so this fixture is the one that says the
# floor is read rather than merely written down.

# And the floor itself, edited in the artifact it governs. The floor used to be
# read out of this file and written back by `--write`, which made it the one
# field the byte comparison could not police: lowering the line lowered the
# floor and the gate reported success, so a coverage regression could be
# laundered through a regeneration. It is a source constant now, so the same
# edit is a stale artifact. A tree whose floor is already 0 makes this mutation
# a no-op and the harness reports the fixture as proving nothing.

# And the self-test itself, against a rule short-circuited in its own source.
run_shell_selftest_case check-feature-inventory.sh \
	"the unknown-claim refusal short-circuited inside applyClaims()" \
	"perl -0pi -e 's/if !known \\{/if !known \\&\\& false \\{/' internal/featureinventory/pages.go"

# A page listed under runnable_examples has to run something. The marking is
# deliberate -- a page writes `quickstart: true` -- but a deliberate marking is
# still a claim, and a page of prose carrying it was published as a runnable
# example with the gate reporting success until this refusal existed.
run_shell_selftest_case check-feature-inventory.sh \
	"the runnable-example refusal short-circuited inside exampleProblems()" \
	"perl -0pi -e 's/if len\\(example\\.Shells\\) == 0 \\{/if false \\&\\& len(example.Shells) == 0 {/' internal/featureinventory/inventory.go"

run_case check-public-api.sh \
	"an exported package with no doc comment" \
	"mkdir -p gateselftest && printf 'package gateselftest\n\nfunc Exported() {}\n' >gateselftest/gateselftest.go"

# The ledger scrape reads list items only. This fixture documents the new
# package in a prose paragraph rather than a bullet: a scrape that let prose
# mentions join the allowlist would accept it and pass (stokaro/ptah#2246).
run_case check-public-api.sh \
	"a package whose only ledger mention is a prose paragraph" \
	'mkdir -p gateselftest && printf "package gateselftest\n\nfunc Exported() {}\n" >gateselftest/gateselftest.go && printf "\nA prose paragraph that mentions \`go.5x5.cz/ptah/gateselftest\` is not a listing.\n" >>docs/public_api.md'


run_case check-public-api-docs-sync.sh \
	"a ledger package's row removed from the site's stable-packages table" \
	"perl -0pi -e 's/^\| \`core\/coverage\`[^\n]*\n//m' docs/site/src/content/docs/extend/public-api.md"

run_case check-public-api-docs-sync.sh \
	"a package the site table lists deleted from the ledger" \
	"perl -0pi -e 's/^- \`go\.5x5\.cz\/ptah\/migration\/seeder\`\n//m' docs/public_api.md"


# The `.mjs` route gates under docs/site/scripts. They take no npm dependency,
# so they run here exactly as they run in the docs job.
#
# Two fixtures each, and the pair is the point. The first breaks the site and
# requires the GATE to notice, which is what run_case does everywhere above. The
# second breaks the rule in the gate's own source and requires its `--selftest`
# to notice, which is the only thing that can tell a self-test that still reads
# from one that has been reduced to a bare `OK`.

# `git mv`, not `mv`, because a route gate enumerates pages with `git ls-files
# --cached`: a page deleted and left unstaged still reads as live, and the gate
# says so in its own header.
run_node_gate_case check-route-retirement.mjs \
	"a published route retired with no redirect left behind" \
	"git mv docs/site/src/content/docs/schema/dbml.md docs/site/src/content/docs/schema/dbml-export.md"

run_node_selftest_case check-route-retirement.mjs \
	"the retirement rule short-circuited inside analyze()" \
	"perl -0pi -e 's/if \\(liveSet\\.has\\(route\\) \\|\\| sources\\.has\\(route\\)\\) continue;/if (true) continue;/' docs/site/scripts/check-route-retirement.mjs"

# A promise with no owner, on a reader-facing page. The gate exists because
# section 6.2's other fifteen spellings have a present-tense reading and these
# three do not, so the fixture is a sentence somebody would plausibly write.
run_node_gate_case check-implementation-chronology.mjs \
	"a reader page promising a capability in a later phase" \
	"printf '\nThey return when a later phase can supply one.\n' >>docs/site/src/content/docs/operate/ai-agents.md"

run_node_selftest_case check-implementation-chronology.mjs \
	"the phrase list emptied inside findingsIn()" \
	"perl -0pi -e 's/for \\(const \\{ pattern, why \\} of phrases\\) \\{/for (const { pattern, why } of []) {/' docs/site/scripts/check-implementation-chronology.mjs"

# Two files publishing one route. The build resolves it by serving one body in
# place of the other and exits 0, so the refusal has to be on this gate's path
# and not only on the retirement gate's.
run_node_gate_case check-page-health.mjs \
	"a second page shadowing an existing route" \
	"mkdir -p docs/site/src/content/docs/schema/dbml && printf -- '---\ntitle: DBML\ndescription: A shadow.\n---\n' >docs/site/src/content/docs/schema/dbml/index.md"

run_node_selftest_case check-page-health.mjs \
	"the sidebar-to-page rule short-circuited inside analyze()" \
	"perl -0pi -e 's/if \\(!liveRoutes\\.has\\(entry\\.route\\)\\) \\{/if (false) {/' docs/site/scripts/check-page-health.mjs"

# The Pages root. Two fixtures, because the gate's two directions fail
# differently and only one of them leaves a trace anybody would notice.
#
# The first is the whole reason the gate exists: the tree still carries
# docs/site/public/install.sh, the assembly this gate runs for itself still
# writes it, and the deploy stops doing so. `_site` is built from scratch and
# uploaded whole on every deploying run, so the file survives until the next
# deploy and then is gone, while the documentation goes on publishing
# `curl ... | sh`.
run_node_gate_case check-pages-root.mjs \
	"the step that writes the install scripts into the Pages root removed" \
	'perl -0pi -e "s{run: node docs/site/scripts/publish-root-assets.mjs [^\n]*}{run: echo nothing}" .github/workflows/docs.yml'

# The other direction. Retiring an asset from the declaration is the same 404
# reached from the other end, and nothing else in the tree objects: the file is
# still in docs/site/public, the workflow still runs the publisher, and the
# documentation still tells a reader to pipe an address into a shell.
run_node_gate_case check-pages-root.mjs \
	"a documented root URL dropped from the declaration the assembly reads" \
	"perl -0pi -e 's/  \\{\\n    name: .install\\.ps1.,\\n.*?\\n  \\},\\n//s' docs/site/scripts/publish-root-assets.mjs"

run_node_selftest_case check-pages-root.mjs \
	"the produced-file rule short-circuited inside analyze()" \
	"perl -0pi -e 's/if \\(!assembled\\.has\\(name\\)\\) \\{/if (false) {/' docs/site/scripts/check-pages-root.mjs"

# The --live rule. It is the only one that asks the published address, and it is
# the only one that can see a root a deploy from an older tag replaced -- a tag
# runs the workflow file AS IT EXISTS AT THAT TAG, which for anything cut before
# the publish step has no publish step. Nothing in this tree can be edited to
# produce that, so the rule is measured through its own self-test.
run_node_selftest_case check-pages-root.mjs \
	"the published-address status rule short-circuited inside analyzeLive()" \
	"perl -0pi -e 's/if \\(answer\\.status !== 200\\) \\{/if (false) {/' docs/site/scripts/check-pages-root.mjs"

# The terminology gate has four things to prove and they fail independently:
# the ban still fires on prose, it still reaches the site sources that are not
# Markdown, section 7 is still compared against the registry it is generated
# from, and the gate's own self-test still asserts.
#
# The prose fixture writes the banned spelling as a WORKFLOW LABEL, which is the
# only sense section 7 retires. The word itself is legitimate 123 times in this
# tree, so a gate reddening on the fixture and staying green on the tree is the
# claim being tested, not the mutation alone.
run_node_gate_case check-terminology.mjs \
	"a retired workflow label written into a governed page" \
	"printf '\nThe declarative schema changes workflow runs the difference now.\n' >>docs/site/src/content/docs/direct/overview.md"

# The sidebar is the site's primary navigation and it is not Markdown, so it was
# outside the gate's corpus until the corpus stopped being a hand-written list
# of globs. A reader meets these 26 labels before any page.
run_node_gate_case check-terminology.mjs \
	"a retired workflow label in the sidebar, which is not a Markdown file" \
	"perl -0pi -e \"s/label: 'Direct schema changes'/label: 'Declarative schema changes'/\" docs/site/src/sidebar.mjs"

# Section 7 is generated. Hand-editing it is the failure build-feature-matrix
# already exists to catch on another page, and it has to be caught here too --
# otherwise the registry and the table a human reads drift apart in the one
# direction nobody notices, with both looking authoritative.
run_node_gate_case check-terminology.mjs \
	"section 7's generated table edited by hand" \
	"perl -0pi -e 's/\| revision table \| The database table recording applied migrations\. \| review \|/| revision table | The table. | review |/' docs/STYLE_GUIDE.md"

run_node_selftest_case check-terminology.mjs \
	"the head classification short-circuited inside analyze()" \
	"perl -0pi -e \"s/if \\(verdict === 'allowed'\\) continue;/if (true) continue;/\" docs/site/scripts/check-terminology.mjs"

# The mention rule is the gate's only silencer, and it is the lever that decides
# between a gate nobody can use and a gate that reports nothing. Widening it to
# "a quote silences everything" is the shape that would pass a self-test written
# only from fixtures that must fire.
run_node_selftest_case check-terminology.mjs \
	"the mention rule widened to silence every quoted stem" \
	"perl -0pi -e 's/export function isMention\\(prose, stemStart, stemEnd\\) \\{/export function isMention(prose, stemStart, stemEnd) { return true;/' docs/site/scripts/check-terminology.mjs"

# The library both route gates read the tree through. Its self-test is the only
# thing asserting that pages come from git rather than from a walk, and that a
# route Astro would spell differently is refused rather than guessed at.
run_node_selftest_case lib/docroutes.mjs \
	"the unmodeled-segment refusal removed from routeFor" \
	"perl -0pi -e 's/^  assertSlugStable\\(withoutExtension.*\\n//m' docs/site/scripts/lib/docroutes.mjs"

# What this harness does NOT cover, and why. A data table rather than a
# paragraph, because the guard at the end of this file reads it: a coverage list
# nobody checks is the same failure mode the harness exists to prevent --
# silence that reads as completeness (stokaro/ptah#1923).
# adjacent lists the gates whose known-bad fixtures already live BESIDE the
# checker, which is where stokaro/ptah#2509 wants all of them. A mutation here
# would run a second, weaker version of a claim that is already made where it
# can be read next to the code it is about -- and the harness's own summary
# would count it twice.
#
# Each entry names where the fixtures are, so the claim is checkable rather than
# asserted. This list is the harness shrinking toward its own deletion: it is
# the direction, and a gate joins it by growing an adjacent test.
adjacent=(
	"check-docsync.sh	internal/docsync: each fail-closed refusal, and Replace asserted idempotent"
	"check-feature-inventory.sh	--selftest breaks every derivation rule against in-memory fixtures, and go-lint.yml runs it"
	"check-go-toolchain-single-source.sh	internal/gotoolchain: every YAML spelling and both forwarding shapes, with controls"
	"check-renovate-regex.sh	internal/renovateregex: the backreference that stopped Renovate, and the group spelling that is the control for it"
	"check-exported-docs.sh	internal/cmd/exporteddocs: each rule over an AST fixture, and the method exemption that 148 of 158 findings turned on"
)

uncovered=(
	"check-coverage.sh	runs the whole test suite; minutes per fixture"
	"check-public-api-released.sh	resolves the published module over the network"
	"check-documented-install.sh	installs the published module with go install, over the network"
	"check-api-export-acceptance.sh	needs a built binary the throwaway worktree has none of"
	"check-hcl-export-acceptance.sh	the same"
	"check-protobuf-export-acceptance.sh	the same"
	"check-quickstart.sh	the same; internal/quickstart is where it is proven able to fail"
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
for entry in "${adjacent[@]}"; do
	printf '    %-40s %s\n' "${entry%%	*}" "proven beside the checker -- ${entry##*	}"
done
for gate in $(companion_gates); do
	printf '    %-40s %s\n' "$gate" "carries its own -selftest.sh companion"
done

# The `.mjs` gates under docs/site/scripts, and what is claimed about each.
#
# Five fixtures above watched three of them go red on a broken rule. Every other
# one is covered by the text scan below and by nothing else, which asks only
# whether the file takes `--selftest` at all. That is weaker, deliberately, and
# the weakness is worth writing down rather than leaving for the next reader: a
# `--selftest` reduced to a bare `OK (0 assertions)` still exits 0, so neither
# this scan nor `npm run check:*:selftest` in the docs job can tell a gate that
# asserts from one that stopped. Only a mutation fixture can, and giving the
# remaining gates one is work rather than a decision (stokaro/ptah#1923).
#
# The glob is `check-*.mjs` on purpose. `build-feature-matrix.mjs` is a
# generator with a `--check` mode rather than a gate, and `gen-versions.mjs`
# carries its own `versions:selftest`; neither is a `check-*` gate and neither
# is claimed here. `lib/docroutes.mjs` is a library, not a gate, and it is
# covered by a fixture above instead.
uncovered_mjs=(
	"check-core-doc-links.mjs	a substring scan over pinned core paths; it takes no --selftest yet"
)

echo
echo "  docs/site/scripts .mjs gates, and what is claimed about each:"
mjs_guarded=0
mjs_unlisted=""
for path in docs/site/scripts/check-*.mjs; do
	gate="$(basename "$path")"
	mjs_guarded=$((mjs_guarded + 1))
	listed=""
	for covered in ${mjs_fixtured[@]+"${mjs_fixtured[@]}"}; do
		[ "$covered" = "$gate" ] && listed="fixture"
	done
	# The `${a[@]+...}` guard is not decoration: with `set -u`, expanding an
	# EMPTY array is an error in bash 3.2, and this list is empty the moment
	# the last gate without a --selftest gets one.
	for entry in ${uncovered_mjs[@]+"${uncovered_mjs[@]}"}; do
		[ "${entry%%	*}" = "$gate" ] && listed="reason: ${entry##*	}"
	done
	if [ "$listed" = "fixture" ]; then
		printf '    %-40s %s\n' "$gate" "a fixture above required it to fail"
		continue
	fi
	if [ -n "$listed" ]; then
		printf '    %-40s %s\n' "$gate" "$listed"
		continue
	fi
	if grep -q -- '--selftest' "$path"; then
		printf '    %-40s %s\n' "$gate" "takes --selftest, run in the docs job; no mutation fixture here"
		continue
	fi
	mjs_unlisted="${mjs_unlisted} ${gate}"
done

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
	for entry in "${uncovered[@]}" "${adjacent[@]}"; do
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
if [ -n "$mjs_unlisted" ]; then
	echo "check-gate-selftests: no fixture, no --selftest and no reason for:${mjs_unlisted}" >&2
	echo "  give it a --selftest and run it in the docs job, add a run_node_selftest_case," >&2
	echo "  or say in uncovered_mjs why not" >&2
	exit 1
fi
if [ "$failures" -gt 0 ]; then
	echo "check-gate-selftests: ${failures} of ${checked} gates did not notice their own broken rule" >&2
	exit 1
fi
# Both numbers, because a run that accounts for every shell gate while saying
# nothing about the .mjs ones is the same silence-that-reads-as-completeness
# this harness exists to prevent.
echo "check-gate-selftests: OK (${checked} gates each failed on their own broken rule," \
	"${guarded} shell gates and ${mjs_guarded} docs/site .mjs gates all accounted for)"
