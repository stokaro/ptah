#!/usr/bin/env bash
# Proves check-public-api.sh reports a package that is importable by an
# embedder and absent from the ledger, and stays quiet about the trees that are
# deliberately outside it.
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

# ledger writes a stub that prints the package list the gate compares against.
ledger() {
	{
		printf '#!/usr/bin/env bash\n'
		printf 'cat <<LEDGER\n'
		printf '%s\n' "$@"
		printf 'LEDGER\n'
	} >"$work_dir/ledger"
	chmod +x "$work_dir/ledger"
}

# write_module builds a module holding one package per path given.
write_module() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-public-api.sh"
	printf 'module example.com/thing\n\ngo 1.26\n' >"$work_dir/repo/go.mod"
	for path in "$@"; do
		mkdir -p "$work_dir/repo/$path"
		printf 'package %s\n' "$(basename "$path" | tr -d '-')" >"$work_dir/repo/$path/doc.go"
	done
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

# A package an embedder can import and the ledger does not list. This is the
# whole rule: the supported surface is what the ledger says, not what happens to
# be importable.
write_module core/ast
ledger 'example.com/thing/other'
assert_rejected 'a package outside the ledger' 'undocumented public package: example.com/thing/core/ast'

# The control. Without it every row here is satisfied by a gate that reports
# every package, and the repository could not add one.
write_module core/ast
ledger 'example.com/thing/core/ast'
assert_accepted 'a package the ledger lists'

# The trees deliberately outside the ledger. Each is a real exemption the gate
# names, and a fixture per exemption is what keeps one from being dropped: the
# ledger below lists nothing at all, so a tree that is not exempt fails.
for exempt in cmd/thing examples/one integration/suite stubs internal/thing core/internal/detail catalog/testutil catalog/mocks; do
	write_module "$exempt"
	ledger 'example.com/thing/nothing'
	assert_accepted "an exempt tree: $exempt"
done

# A main package is a program rather than an embedder surface, wherever it sits.
write_module tools/generator
printf 'package main\n\nfunc main() {}\n' >"$work_dir/repo/tools/generator/doc.go"
ledger 'example.com/thing/nothing'
assert_accepted 'a main package outside cmd/'

printf 'public API self-test: an unlisted package is reported, a listed one is not, and eight exempt trees and a main package stay quiet\n'
