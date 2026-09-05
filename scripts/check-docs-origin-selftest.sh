#!/usr/bin/env bash
# Proves check-docs-origin.sh rejects each shape it exists to reject.
#
# Four branches, four known-bad fixtures and a control. Every branch, because a
# gate with two rules that is only ever mutated on one of them is half a gate,
# and the half nobody broke is the half that has stopped reading. The floor case
# is the one most worth having: a scan for a string that is absent reports the
# same success whether it read the repository or read nothing, so the branch
# that refuses an implausibly small corpus is the only thing standing between
# "no occurrences" and "no files" (stokaro/ptah#2884).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-docs-origin.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-docs-origin.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# Assembled rather than written, for the reason the gate states about itself:
# a self-test that spells the retired host is a file the gate then reports.
retired_host="stokaro.github$(printf '\056')io"
live_host="docs.ptah$(printf '\056')run"

# write_repo builds a throwaway repository large enough to clear the gate's own
# floor, so that a fixture fails on the rule under test rather than on the size
# of the tree. The gate reads `git ls-files`, so every file has to be added.
write_repo() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/docs/site/scripts" \
		"$work_dir/repo/docs/site/src/lib" "$work_dir/repo/filler"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-docs-origin.sh"
	printf "export const Origin = 'https://%s';\n" "$live_host" \
		>"$work_dir/repo/docs/site/src/lib/docs-origin.mjs"
	# Ten scripts and a hundred files: the two floors the gate holds.
	local i
	for i in $(seq 1 12); do
		printf "import { Origin } from '../src/lib/docs-origin.mjs';\nexport const a%s = Origin;\n" \
			"$i" >"$work_dir/repo/docs/site/scripts/gate$i.mjs"
	done
	for i in $(seq 1 110); do
		printf 'filler %s\n' "$i" >"$work_dir/repo/filler/file$i.txt"
	done
	git -C "$work_dir/repo" add -A >/dev/null 2>&1
}

assert_rejected() {
	local name=$1 expected=$2
	git -C "$work_dir/repo" add -A >/dev/null 2>&1
	if (cd "$work_dir/repo" && bash scripts/check-docs-origin.sh) \
		>"$work_dir/out" 2>"$work_dir/err"; then
		printf 'docs origin self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'docs origin self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
	printf '  %-44s rejected\n' "$name"
}

assert_accepted() {
	git -C "$work_dir/repo" add -A >/dev/null 2>&1
	if ! (cd "$work_dir/repo" && bash scripts/check-docs-origin.sh) \
		>"$work_dir/out" 2>"$work_dir/err"; then
		printf 'docs origin self-test: the control was rejected:\n' >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
	printf '  %-44s accepted\n' "the control"
}

echo "check-docs-origin-selftest: breaking each rule and requiring the gate to notice"

# The control first. A gate that refused everything would satisfy every case
# below, so this is what makes the four rejections mean something.
write_repo
assert_accepted

# 1. The retired host comes back, in ordinary prose rather than in code. This is
#    the shape the sweep for #2884 had to remove from README.md and the
#    installers, none of which is JavaScript.
write_repo
printf 'Read the docs at https://%s/ptah/edge/.\n' "$retired_host" \
	>"$work_dir/repo/README.md"
assert_rejected "the retired host in prose" "the retired documentation host"

# 2. The retired host comes back inside a docs/site script. Rule 1 covers the
#    whole tree, so this must be caught by rule 1 rather than by rule 2 -- the
#    two rules overlap here on purpose, and the case pins which one answers.
write_repo
printf "export const base = 'https://%s/ptah/';\n" "$retired_host" \
	>"$work_dir/repo/docs/site/scripts/gate1.mjs"
assert_rejected "the retired host in a docs/site script" "the retired documentation host"

# 3. The live origin spelled again instead of imported. This is the defect that
#    caused the outage: not a wrong address, but a second copy of a right one,
#    which is how half of them came to be updated and half did not.
write_repo
printf "export const url = 'https://%s/install.sh';\n" "$live_host" \
	>"$work_dir/repo/docs/site/scripts/gate1.mjs"
assert_rejected "the live origin spelled again" "spelled again instead of imported"

# 3b. The project site's origin spelled again. The installers are advertised
#     there, and the declaration holds that address too; a literal copy in a
#     docs/site script is the same second-spelling defect as case 3.
write_repo
printf "export const install = 'https://ptah%srun/install.sh';\n" "$(printf '\056')" \
	>"$work_dir/repo/docs/site/scripts/gate1.mjs"
assert_rejected "the site origin spelled again" "spelled again instead of imported"

# 4. The declaration is gone. Renaming or deleting it must fail here rather than
#    leave rule 2 comparing every file against a file that is not there.
write_repo
rm "$work_dir/repo/docs/site/src/lib/docs-origin.mjs"
assert_rejected "the declaration missing" "nothing declares the site's address"

# 5. The retired host in a file that is not staged yet. This is the case the
#    gate was blind to when it was written: `git ls-files` answers the index, so
#    a file added by the very change under review is invisible, and the gate
#    reports success over a tree it has not read. It ran green over its own
#    declaration this way, and CI -- which reads a committed tree -- was what
#    disagreed. Nothing here stages the fixture, which is the whole point.
write_repo
git -C "$work_dir/repo" add -A >/dev/null 2>&1
printf "export const legacy = 'https://%s/ptah/';\n" "$retired_host" \
	>"$work_dir/repo/docs/site/scripts/brand-new.mjs"
if (cd "$work_dir/repo" && bash scripts/check-docs-origin.sh) \
	>"$work_dir/out" 2>"$work_dir/err"; then
	echo 'docs origin self-test: an unstaged file carrying the retired host passed' >&2
	exit 1
fi
if ! grep -qF "the retired documentation host" "$work_dir/err"; then
	echo 'docs origin self-test: the unstaged file failed for the wrong reason:' >&2
	sed 's/^/  /' "$work_dir/err" >&2
	exit 1
fi
printf '  %-44s rejected\n' "the retired host, not staged yet"

# 6. The corpus itself. A repository the gate can see almost nothing of must
#    refuse rather than report the success that an absent string always earns.
rm -rf "$work_dir/small"
mkdir -p "$work_dir/small/docs/site/src/lib" "$work_dir/small/scripts"
git -C "$work_dir/small" init --quiet
cp "$check" "$work_dir/small/scripts/check-docs-origin.sh"
printf "export const Origin = 'https://%s';\n" "$live_host" \
	>"$work_dir/small/docs/site/src/lib/docs-origin.mjs"
git -C "$work_dir/small" add -A >/dev/null 2>&1
if (cd "$work_dir/small" && bash scripts/check-docs-origin.sh) \
	>"$work_dir/out" 2>"$work_dir/err"; then
	echo 'docs origin self-test: a two-file repository passed the scan' >&2
	exit 1
fi
if ! grep -qF "is not reporting on it" "$work_dir/err"; then
	echo 'docs origin self-test: the tiny repository failed for the wrong reason:' >&2
	sed 's/^/  /' "$work_dir/err" >&2
	exit 1
fi
printf '  %-44s rejected\n' "a corpus too small to be this repository"

echo "check-docs-origin-selftest: OK (7 broken rules each noticed, control accepted)"
