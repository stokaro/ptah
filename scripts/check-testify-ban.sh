#!/usr/bin/env sh
# Control for the testify prohibition.
#
# The ban itself is a depguard entry in .golangci.yml, on the import graph. This
# script is the thing that proves the ban is not inert, because a ban nobody
# measures is indistinguishable from no ban at all.
#
# History, so the shape of this file makes sense (stokaro/ptah#1139):
#
#   The prohibition used to be a text scan:
#
#     git grep -nE 'github\.com/stretchr/testify|\b(assert|require)\.' -- '*.go'
#
#   It had two defects, and the second hid the first.
#
#   1. It matched English. `\b(assert|require)\.` is "a word, then a full stop",
#      which is how sentences end. A comment reading "... never reaches the
#      assert." was refused by CI with a message about testify, and the fix
#      applied twice in this repo was to reword the comment (#709, #1137).
#
#   2. It meant different things on the two platforms the project is developed
#      on. `git grep -E` compiles the pattern with the platform regex library:
#      GNU/glibc on the Linux runners reads `\b` as a word boundary, the
#      BSD-derived library macOS ships reads it as the literal letter `b`.
#      Measured with `git grep -nE '\b(assert|require)\.'` over one tree holding
#      a real `assert.Equal(t, 1, 1)` call, a comment ending "... the assert.",
#      and the string `bassert.Foo`:
#
#        macOS 26.5, git 2.50.1  -> matches only `bassert.Foo`
#        GNU grep 3.11 (Linux)   -> matches the call and the comment
#
#      Exactly inverted. The half of the pattern meant to catch calls was inert
#      locally, so a genuine testify call added on a Mac passed the local gate,
#      and the half that fired on CI fired on a sentence.
#
# depguard replaces it because it matches the import declaration rather than
# source text: it cannot read a sentence as a call, and it has no regex whose
# flavor could vary. What it cannot do is notice a ban that has been deleted
# from the config, or files golangci-lint never loads -- which is what the three
# checks below are for.
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

lint_bin="${GOLANGCI_LINT:-golangci-lint}"
if ! command -v "$lint_bin" >/dev/null 2>&1; then
	echo "testify-ban: $lint_bin not found on PATH" >&2
	echo "testify-ban: this gate runs golangci-lint; refusing to report success without it" >&2
	exit 1
fi

# Printed, not asserted: the version is not pinned here, and a surprising result
# should be attributable to the binary that produced it without a second run.
"$lint_bin" --version

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-testify-ban.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM
# Physical path: on macOS $TMPDIR is reached through /var -> /private/var, and
# golangci-lint reports positions against the resolved path, which then does not
# match the directory the fixture was written to.
work_dir="$(cd "$work_dir" && pwd -P)"

mkdir -p "$work_dir/banned" "$work_dir/allowed" "$work_dir/.lintcache"

# The fixture is checked against the shipped configuration, not a copy of the
# rule written out here: copying .golangci.yml is what makes deleting the deny
# entry turn this gate red.
cp "$repo_root/.golangci.yml" "$work_dir/.golangci.yml"

# Derived rather than hard-coded so the fixture module cannot drift away from
# the language version the repository declares.
go_version="$(awk '$1 == "go" { print $2; exit }' "$repo_root/go.mod")"
if [ -z "$go_version" ]; then
	echo "testify-ban: could not read the go directive from go.mod" >&2
	exit 1
fi

cat >"$work_dir/go.mod" <<EOF
module ptah.local/testifybanfixture

go $go_version
EOF

# Positive control. A real import and a real call, in an ordinary file.
cat >"$work_dir/banned/banned.go" <<'EOF'
// Package banned genuinely imports and calls testify.
package banned

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Check is a testify call, not a mention of one.
func Check(t *testing.T) {
	require.Equal(t, 1, 1)
}
EOF

# Second positive control, in a _test.go file: .golangci.yml carries several
# exclusion rules keyed on '_test\.go$', and this asserts that none of them
# takes depguard with it.
cat >"$work_dir/banned/banned_test.go" <<'EOF'
package banned

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBannedFromATestFile(t *testing.T) {
	assert.Equal(t, 1, 1)
}
EOF

# Negative control. The two comment shapes the old scan refused, verbatim, plus
# a real quicktest call. If a replacement ever goes back to reading source text,
# this file is what catches it.
cat >"$work_dir/allowed/allowed.go" <<'EOF'
// Package allowed contains prose that reads like testify, and no testify.
package allowed

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// Check runs the checks this would require.
//
// Reverted, the first call runs the up body and this never reaches the assert.
func Check(t *testing.T) {
	c := qt.New(t)
	c.Assert(1, qt.Equals, 1)
}
EOF

# A heredoc that produced an empty or truncated file would make check 2 pass
# without having tested anything, so the markers are asserted before use.
# grep -F, because a fixed string has no regex flavor to differ across platforms.
for marker in 'never reaches the assert.' 'the checks this would require.'; do
	if ! grep -qF "$marker" "$work_dir/allowed/allowed.go"; then
		echo "testify-ban: negative-control fixture lost its prose marker: $marker" >&2
		exit 1
	fi
done

# golangci-lint refuses to run while another copy of itself holds its lock, and
# says so on stderr with a non-zero status. Left unhandled that reads as "the
# fixture was accepted" or "the clean file was refused" depending on which check
# hit it, so it is named and retried instead of interpreted.
lint_lock_message='parallel golangci-lint is running'

lint_is_locked_out() {
	case "$1" in
	*"$lint_lock_message"*) return 0 ;;
	*) return 1 ;;
	esac
}

# lint <runner> [args...] — run <runner>, retrying while golangci-lint is locked
# out, and leave the result in `out` and `rc`. Never returns a verdict produced
# by the lock, and never gives up quietly: exhausting the retries exits.
lint() {
	runner="$1"
	shift
	attempt=1
	while :; do
		set +e
		out="$("$runner" "$@")"
		rc=$?
		set -e
		if ! lint_is_locked_out "$out"; then
			return 0
		fi
		if [ "$attempt" -ge 6 ]; then
			echo "testify-ban: $lint_lock_message, after $attempt attempts" >&2
			echo "testify-ban: this gate never ran, so it is not reporting a verdict" >&2
			exit 1
		fi
		echo "testify-ban: $lint_lock_message; retrying ($attempt)" >&2
		attempt=$((attempt + 1))
		sleep 10
	done
}

fixture_depguard() {
	# --enable-only keeps the shipped depguard settings and the shipped
	# exclusions while silencing every other linter, so an unrelated finding in
	# the fixture cannot be mistaken for the ban firing.
	#
	# A private lint cache, because every run builds the fixture at the same
	# module path in a fresh directory: with the shared cache golangci-lint
	# answered the second run from the first run's entry and reported positions
	# in a temporary directory that no longer existed.
	(cd "$work_dir" && GOWORK=off GOFLAGS= GOLANGCI_LINT_CACHE="$work_dir/.lintcache" "$lint_bin" run \
		--config "$work_dir/.golangci.yml" \
		--enable-only=depguard \
		"$@" 2>&1)
}

# module_depguard <module dir, relative to the repo root> <GOOS>
module_depguard() {
	(cd "$repo_root/$1" && GOWORK=off GOOS="$2" "$lint_bin" run \
		--config "$repo_root/.golangci.yml" \
		--enable-only=depguard \
		--build-tags="$tags" \
		./... 2>&1)
}

echo "testify-ban: 1/3 positive control -- a fixture that genuinely calls testify"
lint fixture_depguard ./banned/...
echo "$out"
if [ "$rc" -eq 0 ]; then
	echo "testify-ban: golangci-lint accepted a file that imports and calls testify" >&2
	echo "testify-ban: the prohibition is inert -- check the depguard deny entry in .golangci.yml" >&2
	exit 1
fi
case "$out" in
*"github.com/stretchr/testify"*"(depguard)"*) ;;
*)
	echo "testify-ban: the fixture was refused, but not by depguard" >&2
	echo "testify-ban: something else failed the run, so the ban itself is untested" >&2
	exit 1
	;;
esac
# Both banned files must be named. One finding would mean the _test.go file was
# excluded and the ban does not reach tests, which is where testify would land.
for f in banned.go banned_test.go; do
	case "$out" in
	*"$f"*) ;;
	*)
		echo "testify-ban: depguard did not report banned/$f" >&2
		exit 1
		;;
	esac
done

echo "testify-ban: 2/3 negative control -- prose that ends a sentence with assert. and require."
lint fixture_depguard ./allowed/...
if [ "$rc" -ne 0 ]; then
	echo "$out"
	echo "testify-ban: the prohibition refused a file with no testify in it" >&2
	echo "testify-ban: this is stokaro/ptah#1139 again -- it is reading English, not imports" >&2
	exit 1
fi

# The deleted text scan read every tracked *.go file. depguard reads only what
# golangci-lint loads, which is narrower in three ways, so the sweep below opens
# each one back up rather than quietly shipping a smaller ban.
#
#   Build constraints. 47 test files sit behind `//go:build integration`.
#   Measured: with a testify import planted in one of them, `golangci-lint run
#   --enable-only=depguard ./...` reports 0 issues and exits 0; the same run with
#   --build-tags=integration reports the depguard finding and exits 1. The tag is
#   not set in .golangci.yml itself because the full linter set has 15 unrelated
#   findings under it -- restricting this sweep to depguard closes the blind spot
#   without dragging those in.
#
#   Target platform. `//go:build windows` and `//go:build !windows` files cannot
#   both be loaded in one run, so each GOOS is swept separately.
#
#   Nested modules. `./...` stops at a nested go.mod, and this repository has
#   two. They are read from git rather than listed, so a module added later is
#   swept the day it is committed.
tags=integration,testkitcontainers,observability

# The root module is spelled `.` rather than the empty string: `for m in $list`
# drops empty fields, which would skip the whole repository and still print ok.
modules="$(git -c core.quotePath=false ls-files '*go.mod' | sed -e 's#^go\.mod$#.#' -e 's#/go\.mod$##')"
case "
$modules
" in
*"
.
"*) ;;
*)
	echo "testify-ban: the module list does not contain the repository root:" >&2
	echo "$modules" >&2
	echo "testify-ban: refusing to report a clean sweep of a list that lost the root" >&2
	exit 1
	;;
esac

echo "testify-ban: 3/3 sweep -- every module, every target platform, every build tag"
sweep_failed=0
for module in $modules; do
	module_failed=0
	for goos in linux darwin windows; do
		lint module_depguard "$module" "$goos"
		if [ "$rc" -ne 0 ]; then
			sweep_failed=1
			module_failed=1
			echo "testify-ban: FAILED $module GOOS=$goos" >&2
			echo "$out" >&2
		fi
	done
	if [ "$module_failed" -eq 0 ]; then
		echo "  ok: $module (linux, darwin, windows)"
	fi
done

if [ "$sweep_failed" -ne 0 ]; then
	echo "testify-ban: prohibited import found" >&2
	exit 1
fi

echo "testify-ban: ok"
