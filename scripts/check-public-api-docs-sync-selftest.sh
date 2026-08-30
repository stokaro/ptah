#!/usr/bin/env bash
# Proves check-public-api-docs-sync.sh reports the ledger and the site table
# naming different package sets, in both directions, and refuses a vacuous pass
# when either side reads as empty.
#
# The site page carries a hand-written copy of a list the ledger owns, and a
# copy has no way to notice the original moving: the table fell six packages
# behind before anyone read both in one sitting (stokaro/ptah#2246;
# stokaro/ptah#2509 moves the fixtures here).
#
# The fixture supplies the ledger side by putting a stub next to a copy of the
# gate. `script_dir` is derived from the gate's own path, so a copy in the
# fixture's `scripts/` calls the fixture's `list-public-api-packages.sh`. The
# production path still runs the real parser through that command.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-public-api-docs-sync.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-public-api-docs-sync.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

module_path="example.test/ledger"
page_dir="docs/site/src/content/docs/extend"

# write_repo takes the ledger's answer as arguments and the page body on stdin.
# The ledger prints full import paths and the table prints them without the
# module prefix, which is the one transformation between the two sides.
write_repo() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/$page_dir"
	printf 'module %s\n\ngo 1.26.5\n' "$module_path" >"$work_dir/repo/go.mod"
	cp "$check" "$work_dir/repo/scripts/check-public-api-docs-sync.sh"

	{
		printf '#!/usr/bin/env bash\n'
		for package in "$@"; do
			printf 'printf %%s\\\\n %s/%s\n' "$module_path" "$package"
		done
	} >"$work_dir/repo/scripts/list-public-api-packages.sh"
	chmod +x "$work_dir/repo/scripts/list-public-api-packages.sh"

	cat >"$work_dir/repo/$page_dir/public-api.md"
}

assert_rejected() {
	local name=$1 expected=$2
	if (cd "$work_dir/repo" && scripts/check-public-api-docs-sync.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'public API docs-sync self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'public API docs-sync self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	local name=$1
	if ! (cd "$work_dir/repo" && scripts/check-public-api-docs-sync.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'public API docs-sync self-test: %s was rejected:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

# The control. Both sides name the same two packages, and the page carries the
# shapes that must NOT count as listings: a second table, and prose naming a
# package in a code span. Without this case every rejection below could be the
# gate rejecting everything.
write_repo core/ast dbschema <<'PAGE'
# Public API

## Stable packages

| Package | Purpose |
| --- | --- |
| `core/ast` | Typed schema DDL AST nodes. |
| `dbschema` | Live database connection and catalog reading. |

## Guardrails

The gates below run over `core/renderer` as well, though it is not a stable
package.

| Script | Purpose |
| --- | --- |
| `scripts/check-public-api.sh` | Ledger membership. |
PAGE
assert_accepted "matching sets, with a second table and a prose mention that must not count"

# The direction #2246 reported: the ledger grew and the hand-written table did
# not.
write_repo core/ast dbschema migration/planner <<'PAGE'
## Stable packages

| Package | Purpose |
| --- | --- |
| `core/ast` | Typed schema DDL AST nodes. |
| `dbschema` | Live database connection and catalog reading. |
PAGE
assert_rejected "a package added to the ledger and not to the table" "  migration/planner"

# The other direction. A row for a package the ledger does not carry promises a
# stability the gates do not enforce, which is the worse half of the two.
write_repo core/ast <<'PAGE'
## Stable packages

| Package | Purpose |
| --- | --- |
| `core/ast` | Typed schema DDL AST nodes. |
| `migration/planner` | Migration planning. |
PAGE
assert_rejected "a table row for a package outside the ledger" "  migration/planner"

# Both refusals below are the reason the gate cannot be written as a plain diff.
# An empty side compares equal to nothing and reports a clean pass, so each side
# asserts a floor before comparing.
write_repo <<'PAGE'
## Stable packages

| Package | Purpose |
| --- | --- |
| `core/ast` | Typed schema DDL AST nodes. |
PAGE
assert_rejected "a ledger that answered nothing" "refusing to report a vacuous pass"

# The realistic way the table side empties: the heading is renamed and the awk
# scope stops matching, leaving the rows in the page and out of the comparison.
write_repo core/ast <<'PAGE'
## Stable packages for embedders

| Package | Purpose |
| --- | --- |
| `core/ast` | Typed schema DDL AST nodes. |
PAGE
assert_rejected "a renamed heading that scoped the table out" "refusing to report a vacuous pass"

printf 'public API docs-sync self-test: both drift directions reported, a second table and a prose mention are not listings, and an empty side on either half is refused\n'
