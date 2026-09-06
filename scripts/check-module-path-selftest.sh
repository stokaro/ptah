#!/usr/bin/env bash
# Proves check-module-path.sh rejects each shape it exists to reject.
#
# Four rejections and a control. The control is what keeps a gate that refused
# everything from satisfying the four, and the unstaged case is the one that
# matters most: a file added by the change under review is untracked until it is
# staged, and a gate reading the index alone cannot fail on the file most likely
# to be wrong (stokaro/ptah#2943, stokaro/ptah#2884).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-module-path.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-module-path.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# Assembled, like the gate's own needle: a self-test that spells the retired
# organisation's token is a file the gate then reports. The token is built first
# because the gate refuses it in ANY shape -- the module host, the OCI
# annotation namespace, the MCP _meta key -- and the fixtures below need to
# construct each of those without naming it.
retired_token="5$(printf '\170')5"
retired_host="go$(printf '\056')${retired_token}$(printf '\056')cz"
current_module="ptah$(printf '\056')run"

write_repo() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/filler" "$work_dir/repo/nested"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-module-path.sh"
	printf 'module %s\n\ngo 1.26.5\n' "$current_module" >"$work_dir/repo/go.mod"
	printf 'module %s/nested\n\ngo 1.26.5\n' "$current_module" >"$work_dir/repo/nested/go.mod"
	local i
	for i in $(seq 1 110); do printf 'filler %s\n' "$i" >"$work_dir/repo/filler/f$i.txt"; done
	git -C "$work_dir/repo" add -A >/dev/null 2>&1
}

assert_rejected() {
	local name=$1 expected=$2
	if (cd "$work_dir/repo" && bash scripts/check-module-path.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'module path self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'module path self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
	printf '  %-46s rejected\n' "$name"
}

echo "check-module-path-selftest: breaking each rule and requiring the gate to notice"

write_repo
if ! (cd "$work_dir/repo" && bash scripts/check-module-path.sh) >"$work_dir/out" 2>"$work_dir/err"; then
	echo 'module path self-test: the control was rejected:' >&2
	sed 's/^/  /' "$work_dir/err" >&2
	exit 1
fi
printf '  %-46s accepted\n' "the control"

# 1. The retired host in an import, which is the shape the rename removed.
write_repo
printf 'package a\n\nimport _ "%s/ptah/core/ast"\n' "$retired_host" >"$work_dir/repo/a.go"
git -C "$work_dir/repo" add -A >/dev/null 2>&1
assert_rejected "an import of the retired path" "the retired organisation"

# 2. The bare host with no module path after it. The rename's own sweep missed
#    exactly this, in a comment.
write_repo
printf '# a workflow that resolves %s\n' "$retired_host" >"$work_dir/repo/note.md"
git -C "$work_dir/repo" add -A >/dev/null 2>&1
assert_rejected "the bare host, with no module path" "the retired organisation"

# 3. The host with each dot escaped for a regexp. A fixed-string search finds
#    only the plain spelling, and seven of these survived the rename's sweep --
#    including one inside a gate, whose grep would then have matched nothing and
#    reported success over an empty set.
write_repo
printf 'grep -E %s/ptah unit-tests.log\n' "$(printf '%s' "$retired_host" | sed 's/[.]/\\./g')" \
	>"$work_dir/repo/scan.sh"
git -C "$work_dir/repo" add -A >/dev/null 2>&1
assert_rejected "the host escaped for a regexp" "the retired organisation"

# 4. The OCI annotation namespace, in reverse DNS. It names the same retired
#    organisation without containing the module host at all, so a rule aimed at
#    the host would move every import and leave this wire identifier behind.
write_repo
printf 'package a\n\nconst k = "cz.%s.ptah.inference.passed"\n' "$retired_token" \
	>"$work_dir/repo/oci.go"
git -C "$work_dir/repo" add -A >/dev/null 2>&1
assert_rejected "the OCI namespace in reverse DNS" "the retired organisation"

# 5. A module that declares something else. A rename that reaches every import
#    and leaves a go.mod behind builds until someone consumes that module.
write_repo
printf 'module example.invalid/other\n\ngo 1.26.5\n' >"$work_dir/repo/nested/go.mod"
git -C "$work_dir/repo" add -A >/dev/null 2>&1
assert_rejected "a module declaring another path" "every module here belongs to"

# 6. Not staged. Nothing here stages the fixture, which is the whole point.
write_repo
git -C "$work_dir/repo" add -A >/dev/null 2>&1
printf 'package b\n\nimport _ "%s/ptah/dbschema"\n' "$retired_host" >"$work_dir/repo/brand-new.go"
assert_rejected "the retired path, not staged yet" "the retired organisation"

# 7. A corpus too small to be this repository must refuse rather than report the
#    success an absent string always earns.
rm -rf "$work_dir/small"
mkdir -p "$work_dir/small/scripts"
git -C "$work_dir/small" init --quiet
cp "$check" "$work_dir/small/scripts/check-module-path.sh"
printf 'module %s\n' "$current_module" >"$work_dir/small/go.mod"
git -C "$work_dir/small" add -A >/dev/null 2>&1
if (cd "$work_dir/small" && bash scripts/check-module-path.sh) >"$work_dir/out" 2>"$work_dir/err"; then
	echo 'module path self-test: a two-file repository passed the scan' >&2
	exit 1
fi
grep -qF "is not reporting on it" "$work_dir/err" || {
	echo 'module path self-test: the tiny repository failed for the wrong reason:' >&2
	sed 's/^/  /' "$work_dir/err" >&2
	exit 1
}
printf '  %-46s rejected\n' "a corpus too small to be this repository"

echo "check-module-path-selftest: OK (7 broken rules each noticed, control accepted)"
