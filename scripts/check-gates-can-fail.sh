#!/usr/bin/env bash
# Checks that every gate still fails when the rule it states is broken.
#
# A gate stops gating quietly. It does not error: it discovers zero inputs, or
# its pattern stops matching, and it reports the same success it reports on a
# clean tree. check-capability-tables.sh says it in its own header -- "a gate
# that compares nothing to nothing reports success at exactly the moment it
# stopped working" -- and check-public-api.sh, check-public-api-snapshot.sh,
# check-documented-install.sh and check-repository-local-paths.sh each refuse a
# vacuous pass for the same reason.
#
# Eleven gates could already demonstrate they fail: three shell gates carry a
# -selftest.sh companion and eight docs gates take --selftest. The rest could
# not, and an audit that broke each of their rules by hand found all of them
# still working -- which is a photograph, not a guarantee (stokaro/ptah#1923).
#
# Each row below is one gate and the smallest edit that breaks the rule that
# gate states. The edit is applied in a throwaway worktree, so this never
# touches the tree it is run from and a fixture that fails to restore cannot
# poison the next row.
#
# Usage:
#   scripts/check-gates-can-fail.sh [gate-name ...]
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# fixtures maps a gate to a shell snippet that breaks its stated rule. The
# snippet runs inside the throwaway worktree with that worktree as $PWD.
#
# Each is the edit an audit measured turning that gate red, quoted from what it
# did rather than invented here.
fixtures() {
	case "$1" in
	check-repository-local-paths)
		# A developer-specific path in a tracked file, which is the whole rule.
		printf 'Developer checkout lives at /Users/example/Work/ptah\n' >>README.md
		;;
	check-go-toolchain-single-source)
		# One setup-go step pinning a Go version literal instead of deriving it.
		perl -0pi -e 's/go-version-file: go\.mod/go-version: "1.26.5"/' .github/workflows/go-lint.yml
		;;
	check-go-module-lint-coverage)
		# A tracked module linted by one job instead of both.
		perl -0pi -e 's/\n *- name: Golint \(testkit\).*?\n(?= *- name: )//s' .github/workflows/go-lint.yml
		;;
	check-lint-rules)
		# A rule the code registers that the generated block stops documenting.
		perl -ni -e 'print unless $. > 1 && /^\| `[A-Z]{2}\d{3}` \|/ && $seen++ == 0' \
			docs/site/src/content/docs/reference/lint-rules.md
		;;
	check-version-matrix)
		# A declared release line the documented matrix stops carrying.
		perl -0pi -e 's/GENERATED VERSION MATRIX/GENERATED VERSION TABLE/g' docs/capabilities.md
		;;
	check-capability-tables)
		# One cell of the generated preset matrix flipped, which is the drift
		# shape the script's own header describes.
		perl -0pi -e 's/✅/❌/' docs/capabilities.md
		;;
	check-documented-install)
		# A documented install command naming a binary the module lacks.
		perl -0pi -e 's{cmd/ptah-ls\@latest}{cmd/ptah-lsp\@latest}' \
			docs/site/src/content/docs/start/install.md
		;;
	check-public-api)
		# A public package with no row in docs/public_api.md.
		mkdir -p pubprobe
		printf 'package pubprobe\n\n// Answer is exported and undocumented.\nfunc Answer() int { return 42 }\n' \
			>pubprobe/pubprobe.go
		;;
	check-public-api-snapshot)
		# An exported field added to a documented struct.
		perl -0pi -e 's/(type Finding struct \{)/$1\n\tNote string `json:"note,omitempty"`/' \
			migration/safety/safety.go
		;;
	check-test-style)
		# A conditional in a test function with no baseline entry.
		perl -0pi -e 's/(func TestStampTargetsNameThisPackage\(t \*testing\.T\) \{)/$1\n\tif testing.Verbose() {\n\t\tt.Log("verbose")\n\t}/' \
			internal/buildinfo/stamp_test.go
		;;
	*)
		printf 'no fixture for gate %s\n' "$1" >&2
		return 1
		;;
	esac
}

# uncovered names the gates with no fixture yet, each with the reason. It is not
# a courtesy list: the guard below refuses to run unless every gate without a
# -selftest.sh companion is either in `gates` or here, so this harness cannot
# quietly come to cover less than it did.
uncovered() {
	case "$1" in
	check-api-export-acceptance | check-hcl-export-acceptance | check-protobuf-export-acceptance)
		printf 'needs bin/ptah built from the worktree, and the fixture is a renderer edit rather than a file edit'
		;;
	check-coverage)
		printf 'the fixture is 6000 uncovered statements and the gate runs the whole test suite'
		;;
	check-public-api-released)
		printf 'the fixture renames an exported function and updates its five call sites, which perl cannot do safely'
		;;
	*)
		return 1
		;;
	esac
}

gates=(
	check-repository-local-paths
	check-go-toolchain-single-source
	check-go-module-lint-coverage
	check-lint-rules
	check-version-matrix
	check-capability-tables
	check-documented-install
	check-public-api
	check-public-api-snapshot
	check-test-style
)

if [[ $# -gt 0 ]]; then
	gates=("$@")
fi

# The guard. A gate that carries neither a fixture nor a companion selftest nor
# a recorded reason is one this harness would silently not cover.
ungoverned=0
for script in scripts/check-*.sh; do
	name="$(basename "$script" .sh)"
	case "$name" in
	*-selftest | check-gates-can-fail) continue ;;
	esac
	[[ -f "scripts/${name}-selftest.sh" ]] && continue
	if printf '%s\n' "${gates[@]}" | grep -qx "$name"; then
		continue
	fi
	if uncovered "$name" >/dev/null; then
		continue
	fi
	printf 'check-gates-can-fail: %s has no fixture, no companion selftest and no recorded reason\n' \
		"$name" >&2
	ungoverned=1
done

if ((ungoverned != 0)); then
	exit 1
fi


worktree="$(mktemp -d)/gate-fixture"
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
	rm -rf "$(dirname "$worktree")"
}
trap cleanup EXIT

failures=0
checked=0
for gate in "${gates[@]}"; do
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
	git worktree add --detach --quiet "$worktree" HEAD

	clean_status=0
	(cd "$worktree" && bash "scripts/$gate.sh" >/dev/null 2>&1) || clean_status=$?
	if ((clean_status != 0)); then
		printf '  %-36s CLEAN TREE ALREADY FAILS (exit %d)\n' "$gate" "$clean_status"
		failures=$((failures + 1))
		continue
	fi

	if ! (cd "$worktree" && fixtures "$gate"); then
		failures=$((failures + 1))
		continue
	fi

	broken_status=0
	(cd "$worktree" && bash "scripts/$gate.sh" >/dev/null 2>&1) || broken_status=$?
	checked=$((checked + 1))
	if ((broken_status == 0)); then
		printf '  %-36s BLIND: its rule was broken and it exited 0\n' "$gate"
		failures=$((failures + 1))
		continue
	fi
	printf '  %-36s catches (exit %d)\n' "$gate" "$broken_status"
done

if ((failures > 0)); then
	printf 'check-gates-can-fail: %d of %d gates could not demonstrate a failure\n' \
		"$failures" "${#gates[@]}" >&2
	exit 1
fi
printf 'check-gates-can-fail: OK (%d gates broken and caught)\n' "$checked"
