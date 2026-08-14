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

# The composite action manifests. D1 and D2 share the list so the one place an
# opaque expression is tolerated and the place its value is pinned are talking
# about the same set of files.
action_manifests="$(git ls-files '.github/actions/*/action.yml' '.github/actions/*/action.yaml')"

is_action_manifest() {
	printf '%s\n' "$action_manifests" | grep -qxF -- "$1"
}

# The exact forwarding shape: a whole-value expression naming one of the action's
# OWN inputs. `${{ env.GO_VERSION }}` and `${{ vars.GO_VERSION }}` are not it --
# they name a literal declared somewhere else in the same repository, which is
# this gate's own failure mode wearing a different hat.
is_forwarded_input() {
	case "$1" in
	'${{'*'}}')
		case "$1" in
		*inputs.*) return 0 ;;
		esac
		;;
	esac
	return 1
}

# Strip surrounding whitespace and quotes so 'go.mod', "go.mod" and go.mod are
# judged the same way.
strip_value() {
	printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^["'"'"']//' -e 's/["'"'"']$//'
}

# D1: every setup-go step derives its version, and derives it from THE source.
#
# The step is read from `uses: actions/setup-go@` down to the first later line
# that starts further left, so a third or fourth setup-go step in one file is
# seen -- the old check read one per file and was blind to the rest.
#
# BOTH version keys are judged, never whichever appears first. setup-go accepts
# `go-version` and `go-version-file` on the same step, so stopping at one leaves
# the other unread, and the unread one is the one that decides what happens when
# the first resolves to empty. Judging each key on its own also makes this
# detector independent of which key setup-go prefers: the step passes only when
# NEITHER key can select a module other than the root go.mod.
#
# Naming a file is not enough on its own: `go-version-file: testkit/go.mod`
# derives honestly and still selects the wrong toolchain, because testkit is a
# separately released module that carries a compatibility floor and no
# `toolchain` directive, so setup-go would fall back to its `go` line. The job
# would quietly build a patch release behind, which is the exact drift this gate
# exists to stop. Only the root go.mod carries the toolchain, so only the root
# go.mod is accepted.
#
# A `${{ }}` expression is opaque: it can show that a value is derived, never
# from what. It is accepted in exactly one place -- a composite action manifest
# forwarding one of its own inputs -- because a composite action runs in the
# CALLER's workspace and must not be pinned to this repository's go.mod. Every
# other expression, in any other file, is refused. What the one accepted
# expression resolves to is pinned separately, by D2b.
setup_go_steps=0
derived_steps=0
forwarding_steps=0

while IFS=: read -r file line _; do
	setup_go_steps=$((setup_go_steps + 1))

	gv_seen=0
	gv_line=""
	gv_value=""
	gvf_seen=0
	gvf_line=""
	gvf_value=""
	while IFS=: read -r hit_key hit_line hit_text; do
		case "$hit_key" in
		version)
			gv_seen=1
			gv_line="$hit_line"
			gv_value="$(strip_value "$hit_text")"
			;;
		file)
			gvf_seen=1
			gvf_line="$hit_line"
			gvf_value="$(strip_value "$hit_text")"
			;;
		esac
	done < <(awk -v start="$line" '
		NR == start {
			# The step runs from `uses:` down to the first later non-blank line
			# that starts further left: the next list item, or the end of the
			# block. A fixed line budget would either stop inside a long step or
			# run on into the following one.
			key_indent = index($0, "uses:") - 1
			next
		}
		NR < start { next }
		/^[[:space:]]*$/ { next }
		{
			indent = match($0, /[^[:space:]]/) - 1
			if (indent < key_indent) { exit }
		}
		/^[[:space:]]*go-version-file:/ {
			value = $0
			sub(/^[[:space:]]*go-version-file:/, "", value)
			print "file:" NR ":" value
			next
		}
		/^[[:space:]]*go-version:/ {
			value = $0
			sub(/^[[:space:]]*go-version:/, "", value)
			print "version:" NR ":" value
			next
		}
	' "$file")

	step_ok=1
	step_forwards=0

	if ((gv_seen == 1)) && [[ -n $gv_value ]]; then
		case "$gv_value" in
		'${{'*'}}')
			if is_forwarded_input "$gv_value" && is_action_manifest "$file"; then
				step_forwards=1
			else
				step_ok=0
				fail "$file" "$gv_line" \
					"setup-go step takes its Go version from the expression '$gv_value'. An expression shows only that a value is derived, never from what, so it is accepted in one place: a composite action manifest forwarding one of its own inputs. Write 'go-version-file: go.mod'; the toolchain is declared once, in go.mod (toolchain $toolchain)."
			fi
			;;
		*)
			step_ok=0
			fail "$file" "$gv_line" \
				"setup-go step pins a Go version literal ('$gv_value'). Use 'go-version-file: go.mod'; the toolchain is declared once, in go.mod (toolchain $toolchain)."
			;;
		esac
	fi

	if ((gvf_seen == 1)); then
		case "$gvf_value" in
		go.mod | ./go.mod) ;;
		'${{'*'}}')
			if is_forwarded_input "$gvf_value" && is_action_manifest "$file"; then
				step_forwards=1
			else
				step_ok=0
				fail "$file" "$gvf_line" \
					"setup-go step takes its module file from the expression '$gvf_value'. An expression shows only that a value is derived, never from what, so it is accepted in one place: a composite action manifest forwarding one of its own inputs. Elsewhere, name the module: 'go-version-file: go.mod'."
			fi
			;;
		*)
			step_ok=0
			fail "$file" "$gvf_line" \
				"setup-go step reads '$gvf_value', which is not the module that declares the toolchain. Use 'go-version-file: go.mod'; only the root go.mod carries 'toolchain $toolchain'."
			;;
		esac
	fi

	if ((gvf_seen == 0)) && [[ -z $gv_value ]]; then
		step_ok=0
		fail "$file" "$line" \
			"setup-go step declares no Go version. Use 'go-version-file: go.mod'; the toolchain is declared once, in go.mod (toolchain $toolchain)."
	fi

	# A forwarded input may resolve to empty, and then nothing has named a
	# module. The go-version-file beside it is the declaration that answers that
	# case, so the exemption is only granted when it is there to be judged.
	if ((step_forwards == 1)) && ((gvf_seen == 0)); then
		step_ok=0
		fail "$file" "$gv_line" \
			"setup-go step forwards '$gv_value' with no 'go-version-file' beside it. A forwarded input may resolve to empty; add 'go-version-file' so the fallback is still a declaration."
	fi

	if ((step_ok == 1)); then
		derived_steps=$((derived_steps + 1))
	fi
	if ((step_forwards == 1)); then
		forwarding_steps=$((forwarding_steps + 1))
	fi
done < <(git grep -n 'uses: actions/setup-go@' -- .github)

# D2: no version-shaped default in a composite action.
#
# A composite action runs in the CALLER's workspace, so it cannot point
# go-version-file at this repository's go.mod. It must instead carry no version
# of its own and let the caller's module decide.
action_files=0
while IFS= read -r action_file; do
	# printf on an empty list still emits one line; an empty name must not be
	# counted, or the vacuity guard below would read it as a manifest.
	[[ -n $action_file ]] || continue
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
		# The quoting is stripped BEFORE the value is judged, never matched as
		# part of the pattern. YAML spells the same scalar `1.26.5`, "1.26.5"
		# and '"'"'1.26.5'"'"', so a pattern that admits only one of them lets a
		# version through under either of the others -- and a pinned, non-empty
		# go-version makes setup-go ignore go-version-file entirely, which puts
		# the action back on a literal with this gate still green.
		in_inputs && input ~ /[Gg][Oo]|[Tt]oolchain/ && /^[[:space:]]+default:/ {
			value = $0
			sub(/^[[:space:]]*default:[[:space:]]*/, "", value)
			gsub(/^["'"'"']|["'"'"']$/, "", value)
			if (value ~ /^[0-9]+\.[0-9]+/) {
				print NR ":" input ":" value
			}
		}
	' "$action_file")

	# D2b: an action with a setup-go step must DECLARE a go-version-file input
	# that defaults to the module carrying the toolchain.
	#
	# The step reads this input through an expression, and an expression is
	# opaque to D1 -- it can only see that the value is derived, never from what.
	# So the default is the last place the module can still be named.
	#
	# This asserts the declaration exists rather than only judging one that
	# happens to be there. Validating what is present is precisely how a check
	# passes on a deletion: with `go-version` also defaulting to empty, removing
	# the default -- or the whole input -- leaves the step selecting nothing,
	# and a detector that only inspects rows it found reports success because it
	# found none.
	if grep -q 'uses: actions/setup-go@' "$action_file"; then
		# Collected with a read loop rather than `mapfile`, which is a bash 4
		# builtin: macOS still ships bash 3.2, and a developer running this
		# locally would otherwise get exit 127 instead of a verdict.
		gvf_defaults=()
		while IFS= read -r gvf_row; do
			gvf_defaults+=("$gvf_row")
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

		if ((${#gvf_defaults[@]} != 1)); then
			fail "$action_file" "1" \
				"an action with a setup-go step must declare exactly one go-version-file input with a default; found ${#gvf_defaults[@]}. Without it the step selects no module and the toolchain silently stops being read from go.mod."
		else
			line="${gvf_defaults[0]%%:*}"
			value="${gvf_defaults[0]#*:}"
			clean="$(printf '%s' "$value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^["'"'"']//' -e 's/["'"'"']$//')"
			case "$clean" in
			go.mod | ./go.mod) ;;
			*)
				fail "$action_file" "$line" \
					"the go-version-file input defaults to '$clean', which is not the module that declares the toolchain. Default it to 'go.mod' so the caller's own root module is read."
				;;
			esac
		fi
	fi
done < <(printf '%s\n' "$action_manifests")

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

# D4: the root's floor never rises above the secondary module's.
#
# testkit is a separately released import path that REQUIRES the root module, so
# testkit is the main module of its own build and the root is its dependency. Go
# requires a main module's `go` directive to be at least its dependencies', which
# makes raising the ROOT's floor the breaking direction. Measured, with the local
# replace in place:
#
#   root go 1.26.6, testkit go 1.26.5 -> go build ./... in testkit/ exits 1
#     go: module .. requires go >= 1.26.6 (running go 1.26.5)
#   root go 1.26.5, testkit go 1.26.6 -> go build ./... in testkit/ exits 0
#
# So testkit sitting HIGHER is legal and needs no gate; testkit sitting LOWER
# than the root is what breaks, and it breaks at the next release rather than
# today, because the published-pin job resolves the tagged root whose directive
# was frozen at release time. Nothing else in the tree states this relationship.
root_go="$(awk '/^go /{print $2; exit}' go.mod)"
if [[ -f testkit/go.mod ]]; then
	testkit_go="$(awk '/^go /{print $2; exit}' testkit/go.mod)"
	testkit_go_line="$(awk '/^go /{print NR; exit}' testkit/go.mod)"
	highest="$(printf '%s\n%s\n' "$root_go" "$testkit_go" | sort -V | tail -1)"
	if [[ $testkit_go != "$root_go" && $highest == "$root_go" ]]; then
		fail "testkit/go.mod" "$testkit_go_line" \
			"testkit declares go $testkit_go but the root module it requires declares go $root_go. testkit is the main module of its own build, and a main module's go directive may not be below its dependencies' -- raise this floor with the root's."
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

printf 'go toolchain check: go.mod declares toolchain %s; scanned %d setup-go steps, all deriving (%d, of which %d forward a composite action input)\n' \
	"$toolchain" "$setup_go_steps" "$derived_steps" "$forwarding_steps"
