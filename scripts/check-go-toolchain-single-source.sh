#!/usr/bin/env bash

set -euo pipefail

# The Go toolchain must be declared exactly once, as the `toolchain` directive
# in the root go.mod, and every other place that needs it must derive it.
#
# The failure this exists to stop: a dependency bump titled "update dependency
# golang to v1.26.6" merged having changed one file, .golangci.yml, because that
# was the only file its updater could reach. The literal 1.26.5 still stood in
# eighteen setup-go steps and an action input default, so CI kept building with
# the older toolchain and govulncheck reported seven standard-library
# vulnerabilities against it. Nothing failed while the declarations disagreed,
# which is what let them disagree for as long as they did.
#
# The check that was supposed to catch that already existed and could not: it
# globbed .github/workflows/go-*.yml (missing eleven of nineteen literals), read
# only the first go-version per file with `head -1`, and wrapped its
# .golangci.yml comparison in `if [ -f go/.golangci.yml ]` for a directory that
# has never existed. This replaces it.
#
# The detectors enumerate POSITIVELY: they find every setup-go step and require
# each one to derive its version, rather than hunting for a known-bad literal.
# A workflow copied from an older one therefore fails on arrival, and none of
# the detectors mention the current version, so the check cannot quietly stop
# working the day the toolchain moves.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

status=0

fail() {
	# GitHub annotation form, so the message lands on the offending line in the
	# pull request diff rather than only in the job log.
	printf '::error file=%s,line=%s::%s\n' "$1" "$2" "$3" >&2
	status=1
}

# D0: the source exists.
#
# Without this, deleting the directive is a silent regression: actions/setup-go
# falls back to the `go` directive, CI quietly builds with the compatibility
# floor again, and every other detector still passes because every setup-go step
# is still deriving -- from a source that no longer says what it used to.
toolchain_count="$(awk '/^toolchain /{n++} END{print n+0}' go.mod)"
if ((toolchain_count != 1)); then
	printf 'go toolchain check: go.mod declares %d `toolchain` directives; it must declare exactly 1\n' \
		"$toolchain_count" >&2
	printf 'go toolchain check: add e.g. `toolchain go1.26.6` to go.mod; it is the single source every setup-go step reads\n' >&2
	exit 1
fi
toolchain="$(awk '/^toolchain /{print $2; exit}' go.mod)"

# D1: every setup-go step derives its version, and derives it from THE source.
#
# The version key is read from the lines following `uses: actions/setup-go@`
# until the step ends, so a third or fourth setup-go step in one file is seen --
# the old check read one per file and was blind to the rest.
#
# Naming the file is not enough on its own: `go-version-file: testkit/go.mod`
# derives honestly and still selects the wrong toolchain, because testkit is a
# separately released module that carries a compatibility floor and no
# `toolchain` directive, so setup-go would fall back to its `go` line. The job
# would quietly build a patch release behind, which is the exact drift this gate
# exists to stop. Only the root go.mod carries the toolchain, so only the root
# go.mod is accepted.
setup_go_steps=0
derived_steps=0

while IFS=: read -r file line _; do
	setup_go_steps=$((setup_go_steps + 1))

	version_key=""
	version_line=""
	version_value=""
	while IFS=: read -r hit_line hit_text; do
		case "$hit_text" in
		*go-version-file:*)
			version_key="file"
			version_line="$hit_line"
			version_value="${hit_text#*go-version-file:}"
			# go-version-file is the authoritative key for this gate, so it
			# always wins: a step may legitimately carry both, and reading the
			# first key alone would let the other go unexamined.
			break
			;;
		*go-version:*)
			# `go-version:` forwarding an expression is how the composite
			# action passes its caller's choice through; that is derivation,
			# not a pin. Do NOT stop scanning on it -- the same step still
			# carries the go-version-file that decides what happens when the
			# input is empty, and stopping here would leave it unread.
			case "$hit_text" in
			*'${{'*)
				version_key="expression"
				version_line="$hit_line"
				;;
			*)
				version_key="literal"
				version_line="$hit_line"
				break
				;;
			esac
			;;
		esac
	done < <(awk -v start="$line" 'NR > start && NR <= start + 8 {print NR ":" $0}' "$file")

	# Strip surrounding whitespace and quotes so 'go.mod', "go.mod" and go.mod
	# are judged the same way.
	version_value="$(printf '%s' "$version_value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^["'"'"']//' -e 's/["'"'"']$//')"

	case "$version_key" in
	expression)
		derived_steps=$((derived_steps + 1))
		;;
	file)
		case "$version_value" in
		go.mod | ./go.mod)
			derived_steps=$((derived_steps + 1))
			;;
		*'${{'*)
			# A composite action forwarding its caller's input.
			derived_steps=$((derived_steps + 1))
			;;
		*)
			fail "$file" "$version_line" \
				"setup-go step reads '$version_value', which is not the module that declares the toolchain. Use 'go-version-file: go.mod'; only the root go.mod carries 'toolchain $toolchain'."
			;;
		esac
		;;
	literal)
		fail "$file" "$version_line" \
			"setup-go step pins a Go version literal. Use 'go-version-file: go.mod'; the toolchain is declared once, in go.mod (toolchain $toolchain)."
		;;
	"")
		fail "$file" "$line" \
			"setup-go step declares no Go version. Use 'go-version-file: go.mod'; the toolchain is declared once, in go.mod (toolchain $toolchain)."
		;;
	esac
done < <(git grep -n 'uses: actions/setup-go@' -- .github)

# D2: no version-shaped default in a composite action.
#
# A composite action runs in the CALLER's workspace, so it cannot point
# go-version-file at this repository's go.mod. It must instead carry no version
# of its own and let the caller's module decide.
action_files=0
while IFS= read -r action_file; do
	action_files=$((action_files + 1))
	while IFS=: read -r line input text; do
		fail "$action_file" "$line" \
			"composite action input '$input' defaults to a Go version literal ($text). Leave it empty and read the caller's module through a go-version-file input."
	done < <(awk '
		/^inputs:/ { in_inputs = 1; next }
		/^[^[:space:]#]/ { in_inputs = 0 }
		in_inputs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
			input = $1
			sub(/:$/, "", input)
			next
		}
		in_inputs && input ~ /[Gg][Oo]|[Tt]oolchain/ && /^[[:space:]]+default:[[:space:]]*"?[0-9]+\.[0-9]+/ {
			value = $0
			sub(/^[[:space:]]*default:[[:space:]]*/, "", value)
			print NR ":" input ":" value
		}
	' "$action_file")

	# D2b: a go-version-file input must default to the module that declares the
	# toolchain.
	#
	# The step in the manifest reads this input through an expression, and an
	# expression is opaque to D1 -- it can only see that the value is derived,
	# never from what. So the default is the last place the module can still be
	# named, and pointing it at a module without a `toolchain` directive would
	# put the action back on the compatibility floor with every detector green.
	while IFS=: read -r line value; do
		clean="$(printf '%s' "$value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^["'"'"']//' -e 's/["'"'"']$//')"
		case "$clean" in
		go.mod | ./go.mod) ;;
		*)
			fail "$action_file" "$line" \
				"the go-version-file input defaults to '$clean', which is not the module that declares the toolchain. Default it to 'go.mod' so the caller's own root module is read."
			;;
		esac
	done < <(awk '
		/^inputs:/ { in_inputs = 1; next }
		/^[^[:space:]#]/ { in_inputs = 0 }
		in_inputs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
			input = $1
			sub(/:$/, "", input)
			next
		}
		in_inputs && input == "go-version-file" && /^[[:space:]]+default:/ {
			value = $0
			sub(/^[[:space:]]*default:[[:space:]]*/, "", value)
			print NR ":" value
		}
	' "$action_file")
done < <(git ls-files '.github/actions/*/action.yml' '.github/actions/*/action.yaml')

if ((action_files == 0)); then
	printf 'go toolchain check: no composite action manifests found under .github/actions; detector D2 would pass vacuously\n' >&2
	exit 1
fi

# D3: golangci-lint restates no Go version.
#
# `run.go` is documented to default to the go directive in go.mod. Setting it
# duplicates a fact, and this is the copy that drifted.
if [[ -f .golangci.yml ]]; then
	while IFS=: read -r file line _; do
		fail "$file" "$line" \
			"golangci-lint declares run.go. Delete it; its documented default is already the go directive from go.mod."
	done < <(awk '/^run:/{in_run=1; next} /^[^[:space:]#]/{in_run=0} in_run && /^[[:space:]]+go:[[:space:]]*"?[0-9]/{print FILENAME ":" NR ":" $0}' .golangci.yml)
fi

# D4: the secondary module's floor does not exceed the root's.
#
# testkit is a separately released import path that depends on the root module,
# and Go requires a main module's `go` directive to be at least its
# dependencies'. Its CI jobs also select their toolchain from the ROOT go.mod, so
# nothing else in the tree states this relationship.
root_go="$(awk '/^go /{print $2; exit}' go.mod)"
if [[ -f testkit/go.mod ]]; then
	testkit_go="$(awk '/^go /{print $2; exit}' testkit/go.mod)"
	highest="$(printf '%s\n%s\n' "$root_go" "$testkit_go" | sort -V | tail -1)"
	if [[ $testkit_go != "$root_go" && $highest == "$testkit_go" ]]; then
		fail "testkit/go.mod" "3" \
			"testkit declares go $testkit_go but the root module it depends on declares go $root_go. A main module's go directive may not be below its dependencies'."
	fi
fi

# D5: the only user-facing Go minimum tracks the root `go` directive.
#
# It must track the FLOOR, not the toolchain: the toolchain is what CI builds
# with, and quoting it to a reader overstates what they need installed. This
# line was seven minor versions stale, which is what an unchecked copy does.
if [[ -f TESTING.md ]]; then
	if ! grep -qF "**Go ${root_go}+**" TESTING.md; then
		stated="$(grep -oE '\*\*Go [0-9][^*]*\*\*' TESTING.md | head -1)"
		fail "TESTING.md" "$(grep -nE '\*\*Go [0-9]' TESTING.md | head -1 | cut -d: -f1)" \
			"the stated Go prerequisite (${stated:-none}) does not match the go directive in go.mod. Write **Go ${root_go}+**; the floor comes from the go directive, never from the toolchain directive."
	fi
fi

# Anti-vacuity. Every count above is derived from a search, and a search that
# matches nothing reports zero violations -- which is indistinguishable from a
# clean tree right up until it is not. The repository has more than twenty
# setup-go steps; if the enumeration collapses, that is a broken check, not a
# passing one.
if ((setup_go_steps < 20)) || ((derived_steps < 1)); then
	printf 'go toolchain check: found %d setup-go steps (%d deriving). The scan matched almost nothing and would have passed vacuously\n' \
		"$setup_go_steps" "$derived_steps" >&2
	exit 1
fi

if ((status != 0)); then
	exit "$status"
fi

printf 'go toolchain check: go.mod declares toolchain %s; scanned %d setup-go steps, all deriving (%d via go-version-file or an input expression)\n' \
	"$toolchain" "$setup_go_steps" "$derived_steps"
