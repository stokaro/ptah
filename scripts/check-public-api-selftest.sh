#!/usr/bin/env bash
# Proves check-public-api.sh reports a library package that is importable by an
# embedder and classified by neither ledger category, accepts the ones that are,
# and stays quiet about the packages that publish no importable surface at all.
#
# The fixture is a throwaway module, and the ledger it is judged against is a
# stub binary: PTAH_FEATURE_INVENTORY is the seam the gate opens for exactly
# this, so a fixture does not have to carry a copy of internal/cmd
# (stokaro/ptah#2509).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-public-api.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-public-api.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# ledger writes a stub answering both listing questions the way
# internal/cmd/featureinventory does: --list-ledger prints the stable category,
# --list-boundary prints stable plus documentation-only. Paths after
# `--documentation-only` go in the second category.
#
# The two answers differ on purpose. A stub that printed one list for both would
# accept a gate that asked --list-ledger, and the documentation-only category
# would then be enforced by nothing -- so the fixture below that lists a package
# in that category alone is what pins which question the gate asks.
ledger() {
	local stable="" documentation_only="" target=stable
	for argument in "$@"; do
		case "$argument" in
			--documentation-only)
				target=documentation_only
				continue
				;;
		esac
		case "$target" in
			stable) stable="$stable$argument"$'\n' ;;
			*) documentation_only="$documentation_only$argument"$'\n' ;;
		esac
	done
	# The two lines below carry the stub's own expansions. They have to reach it
	# unexpanded, so that the stub evaluates them when it runs.
	# shellcheck disable=SC2016
	{
		printf '#!/usr/bin/env bash\n'
		printf 'stable=%s\n' "$(printf '%q' "$stable")"
		printf 'documentation_only=%s\n' "$(printf '%q' "$documentation_only")"
		printf 'printf %%s "$stable"\n'
		printf 'case "${1:-}" in --list-boundary) printf %%s "$documentation_only" ;; esac\n'
	} >"$work_dir/ledger"
	chmod +x "$work_dir/ledger"
}

# new_module starts an empty throwaway module carrying a copy of the gate.
new_module() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-public-api.sh"
	printf 'module example.com/thing\n\ngo 1.26\n' >"$work_dir/repo/go.mod"
}

# add_library adds a package with production source at each path given.
add_library() {
	for path in "$@"; do
		mkdir -p "$work_dir/repo/$path"
		printf 'package %s\n' "$(basename "$path" | tr -d '-')" >"$work_dir/repo/$path/doc.go"
	done
}

# add_main adds a program: a package the gate must skip because it publishes no
# importable surface, wherever in the tree it sits.
add_main() {
	mkdir -p "$work_dir/repo/$1"
	printf 'package main\n\nfunc main() {}\n' >"$work_dir/repo/$1/main.go"
}

# add_test_only adds a directory whose only Go file is a test. `go list` reports
# it as a package, and it publishes nothing an embedder can import -- the case
# the path-pattern gate this replaces could not tell from a library.
add_test_only() {
	mkdir -p "$work_dir/repo/$1"
	printf 'package %s_test\n' "$(basename "$1" | tr -d '-')" >"$work_dir/repo/$1/probe_test.go"
}

# write_module is the common shape: an empty module holding one library package
# per path given.
write_module() {
	new_module
	add_library "$@"
}

run_gate() {
	(cd "$work_dir/repo" && PTAH_FEATURE_INVENTORY="$work_dir/ledger" \
		scripts/check-public-api.sh) >"$work_dir/out" 2>"$work_dir/err"
}

assert_rejected() {
	local name=$1 expected=$2
	if run_gate; then
		printf 'public API self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'public API self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	local name=$1
	if ! run_gate; then
		printf 'public API self-test: %s was rejected:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

# A package an embedder can import and the ledger classifies under neither
# category. This is the whole rule: the surface is what the ledger says, not
# what happens to be importable.
write_module core/ast
ledger 'example.com/thing/other'
assert_rejected 'a package outside the ledger' 'unclassified public package: example.com/thing/core/ast'

# The control. Without it every row here is satisfied by a gate that reports
# every package, and the repository could not add one.
write_module core/ast
ledger 'example.com/thing/core/ast'
assert_accepted 'a package the ledger lists as stable'

# The documentation-only category, and the discriminator for which question the
# gate asks. This package is absent from the stable listing the stub prints for
# --list-ledger, so a gate reading that flag reports it as unclassified.
write_module core/ast examples/models
ledger 'example.com/thing/core/ast' --documentation-only 'example.com/thing/examples/models'
assert_accepted 'a package the ledger lists as documentation-only'

# The trees still outside the ledger, each a named exemption the gate carries
# until stokaro/ptah#2974 internalizes the subtree. A fixture per exemption is
# what keeps one from being dropped by accident: the ledger below lists nothing
# that matches, so a tree that is not exempt fails.
for exempt in cmd/thing integration/suite stubs; do
	write_module core/ast "$exempt"
	ledger 'example.com/thing/core/ast'
	assert_accepted "an exempt tree: $exempt"
done

# The exemptions that are gone. `examples/**` is classified package by package
# now, and the testutil and mocks patterns matched nothing in the tree at all,
# so a package under either name is an ordinary unclassified library.
for reported in examples/one catalog/testutil catalog/mocks; do
	write_module core/ast "$reported"
	ledger 'example.com/thing/core/ast'
	assert_rejected "a formerly exempt path: $reported" \
		"unclassified public package: example.com/thing/$reported"
done

# An internal boundary is a whole path segment. The nested pattern this replaces
# matched `core/internal/detail` and missed the package sitting directly in
# `core/internal`, so both are fixtures.
for boundary in internal/thing internal core/internal/detail core/internal; do
	write_module core/ast "$boundary"
	ledger 'example.com/thing/core/ast'
	assert_accepted "an internal boundary: $boundary"
done

# The inverse. A segment that merely starts with the word is not a boundary, and
# a prefix comparison would have made this package unreachable to the gate.
write_module core/ast internalized
ledger 'example.com/thing/core/ast'
assert_rejected 'a package named like a boundary' \
	'unclassified public package: example.com/thing/internalized'

# A main package is a program rather than an embedder surface, wherever it sits.
# The listed package beside it keeps the corpus floor satisfied, so an
# acceptance here means the program was skipped rather than that nothing ran.
new_module
add_library core/ast
add_main tools/generator
ledger 'example.com/thing/core/ast'
assert_accepted 'a main package outside cmd/'

# A directory holding only test files publishes no import path. This is why the
# gate reads package metadata instead of matching path prefixes: `go list`
# reports the directory either way, and only .GoFiles says which it is.
new_module
add_library core/ast
add_test_only probe/suite
ledger 'example.com/thing/core/ast'
assert_accepted 'a directory holding only test files'

# The corpus floor. Every rule above narrows the set, and a narrowing that went
# too far reports zero findings, which reads exactly like a clean tree.
new_module
add_main tools/generator
ledger 'example.com/thing/core/ast'
assert_rejected 'a module with no library package at all' 'classified no library packages at all'

printf 'public API self-test: an unclassified package is reported, stable and documentation-only listings are not, three exempt trees stay quiet, three former exemptions and a boundary-lookalike are reported, four internal boundaries, a program and a test-only directory stay quiet, and an empty corpus is refused\n'
