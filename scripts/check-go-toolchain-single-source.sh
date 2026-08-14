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

# D0: the source exists AND names a version.
#
# Without this, deleting the directive is a silent regression: actions/setup-go
# falls back to the `go` directive, CI quietly builds with the compatibility
# floor again, and every other detector still passes because every setup-go step
# is still deriving -- from a source that no longer says what it used to.
#
# Counting the directive is not enough, because `toolchain default` is valid Go
# and means "no pin": it is the deletion spelled as a declaration. Measured, on
# this module, with everything else unchanged:
#
#   toolchain go1.26.6 -> go env GOVERSION = go1.26.6
#   toolchain default  -> go env GOVERSION = go1.26.5
#
# That second line is the drift this whole gate exists to stop, reached without
# removing a single line. So the directive has to NAME a toolchain, not merely
# be present.
toolchain_count="$(awk '/^toolchain /{n++} END{print n+0}' go.mod)"
if ((toolchain_count != 1)); then
	printf 'go toolchain check: go.mod declares %d `toolchain` directives; it must declare exactly 1\n' \
		"$toolchain_count" >&2
	printf 'go toolchain check: add e.g. `toolchain go1.26.6` to go.mod; it is the single source every setup-go step reads\n' >&2
	exit 1
fi
toolchain="$(awk '/^toolchain /{print $2; exit}' go.mod)"
case "$toolchain" in
go[0-9]*.[0-9]*) ;;
*)
	printf 'go toolchain check: go.mod declares `toolchain %s`, which names no toolchain\n' "$toolchain" >&2
	printf 'go toolchain check: `toolchain default` is the deletion spelled as a declaration -- it drops CI back to the `go` directive, the compatibility floor\n' >&2
	printf 'go toolchain check: declare a concrete toolchain, e.g. `toolchain go1.26.6`\n' >&2
	exit 1
	;;
esac

# The composite action manifests. D1 and D2 share the list so the one place an
# opaque expression is tolerated and the place its value is pinned are talking
# about the same set of files.
action_manifests="$(git ls-files '.github/actions/*/action.yml' '.github/actions/*/action.yaml')"

is_action_manifest() {
	printf '%s\n' "$action_manifests" | grep -qxF -- "$1"
}

# The forwarding shapes are ENUMERATED, not characterized.
#
# Every earlier attempt described the permitted expression instead of listing
# it -- "contains inputs.", then "is built only out of inputs, the empty string
# and the operators that choose between them" -- and each description admitted
# an expression nobody intended:
#
#   ${{ inputs.go-version || '1.25.0' }}
#       mentions an input and pins 1.25.0 whenever it is empty, its default.
#   ${{ inputs.go-version-file == '' && inputs.go-version-file || inputs.go-version }}
#       is built entirely from the permitted vocabulary and mentions the root
#       input, and still evaluates to the EMPTY go-version input, so setup-go
#       reads no module at all.
#
# The second one is the lesson: no rule stated over the SET of inputs an
# expression mentions can decide what the expression returns. Only the shape
# can. Two shapes are needed, so two shapes are accepted and everything else is
# refused:
#
#   go-version:       ${{ inputs.<V> }}
#   go-version-file:  ${{ inputs.<V> == '' && inputs.<F> || '' }}
#
# Whitespace is normalized away first, so the manifest may lay either out
# however it likes; nothing else about them is negotiable.

# forwarded_version_input prints the input name a `go-version:` value forwards,
# or nothing if the value is not the accepted shape.
forwarded_version_input() {
	local compact
	compact="$(printf '%s' "$1" | tr -d '[:space:]')"
	printf '%s' "$compact" | grep -qE '^\$\{\{inputs\.[A-Za-z0-9_-]+\}\}$' || return 0
	printf '%s' "$compact" | sed -e 's/^\${{inputs\.//' -e 's/}}$//'
}

# forwarded_file_inputs prints "<V> <F>" for a `go-version-file:` value in one of
# the two accepted shapes, or nothing. <F> is the input that names the module.
# <V> is the input the condition tests, empty for the bare forward.
#
#   ${{ inputs.<F> }}                              a plain forward
#   ${{ inputs.<V> == '' && inputs.<F> || '' }}    blanked when <V> is set
#
# The second exists because setup-go prefers a non-empty go-version and ignores
# go-version-file entirely; an action offering both has to clear one of them. An
# action offering only the module file has no such problem and writes the first.
forwarded_file_inputs() {
	local compact
	compact="$(printf '%s' "$1" | tr -d '[:space:]')"

	if printf '%s' "$compact" | grep -qE '^\$\{\{inputs\.[A-Za-z0-9_-]+\}\}$'; then
		printf ' %s\n' "$(printf '%s' "$compact" | sed -e 's/^\${{inputs\.//' -e 's/}}$//')"
		return 0
	fi

	printf '%s' "$compact" |
		grep -qE "^\\\$\{\{inputs\.[A-Za-z0-9_-]+==''&&inputs\.[A-Za-z0-9_-]+\|\|''\}\}$" || return 0
	printf '%s %s\n' \
		"$(printf '%s' "$compact" | sed -e 's/^\${{inputs\.//' -e "s/==''.*//")" \
		"$(printf '%s' "$compact" | sed -e 's/.*&&inputs\.//' -e "s/||''}}$//")"
}

# Strip surrounding whitespace and quotes so 'go.mod', "go.mod" and go.mod are
# judged the same way.
strip_value() {
	printf '%s' "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^["'"'"']//' -e 's/["'"'"']$//'
}

# A block scalar keeps its value on the FOLLOWING lines: `default: >-` with
# 1.25.0 underneath is the string "1.25.0", but a line-oriented read sees `>-`
# and judges a value that is not there. Every shape test in this file then says
# "not a version" and reports nothing.
#
# Rather than grow a YAML parser to chase the folded, literal, keep, strip and
# indentation variants -- which would be one more round of covering the spellings
# someone thought of -- an indicator in any of these positions is refused
# outright. Nothing this gate reads has a reason to be a block scalar: a Go
# version, a module path and an input default are short single-line scalars. A
# value the gate cannot see through is not a value it may pass.
is_block_scalar() {
	printf '%s' "$1" | grep -qE '^[|>][0-9+-]*$'
}

# input_default prints "line:value" for a named composite-action input's default,
# or nothing when it declares none. The input name is unquoted before it is
# compared, because YAML lets a mapping key be quoted and an unmatched key would
# otherwise carry the previous input's name across the block.
input_default() {
	awk -v want="$2" '
		/^inputs:/ { in_inputs = 1; next }
		/^[^[:space:]#]/ { in_inputs = 0 }
		in_inputs && /^  ["'"'"']?[A-Za-z0-9_-]+["'"'"']?:[[:space:]]*$/ {
			input = $1
			sub(/:$/, "", input)
			gsub(/^["'"'"']|["'"'"']$/, "", input)
			next
		}
		in_inputs && input == want && /^[[:space:]]+default:/ {
			value = $0
			sub(/^[[:space:]]*default:[[:space:]]*/, "", value)
			print NR ":" value
			exit
		}
	' "$1"
}

# D1: every setup-go step derives its version, and derives it from THE source.
#
# The step is read from its `uses:` line, quoted or not, down to the first later
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

# "manifest:input" for every input a setup-go step forwards. D1c judges their
# defaults; D1 is the only place that knows which inputs those are.
forwarded_inputs=""

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
	done < <(awk -v start="$line" -v q="'" '
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
		{
			# The key is taken by splitting on the first colon and unquoting what
			# is left of it, not by matching `go-version:` as text. YAML lets a
			# mapping key be quoted -- `"go-version": "1.25.0"` is the same key --
			# and a pattern that only reads the bare spelling never sets the flag,
			# so the pin sits in a step the gate has already called compliant.
			colon = index($0, ":")
			if (colon == 0) { next }
			key = substr($0, 1, colon - 1)
			value = substr($0, colon + 1)
			gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
			gsub("^[\"" q "]|[\"" q "]$", "", key)
			if (key == "go-version-file") { print "file:" NR ":" value; next }
			if (key == "go-version") { print "version:" NR ":" value; next }
		}
	' "$file")

	step_ok=1
	step_forwards=0

	gv_input=""
	gvf_cond_input=""
	gvf_file_input=""

	if ((gv_seen == 1)) && [[ -n $gv_value ]]; then
		case "$gv_value" in
		'${{'*'}}')
			gv_input="$(forwarded_version_input "$gv_value")"
			if [[ -n $gv_input ]] && is_action_manifest "$file"; then
				step_forwards=1
			else
				step_ok=0
				fail "$file" "$gv_line" \
					"setup-go step takes its Go version from the expression '$gv_value'. An expression shows only that a value is derived, never from what, so exactly one shape is accepted, and only in a composite action manifest: \${{ inputs.<name> }}. Write 'go-version-file: go.mod'; the toolchain is declared once, in go.mod (toolchain $toolchain)."
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
			gvf_pair="$(forwarded_file_inputs "$gvf_value")"
			if [[ -n $gvf_pair ]] && is_action_manifest "$file"; then
				step_forwards=1
				gvf_cond_input="${gvf_pair%% *}"
				gvf_file_input="${gvf_pair##* }"
			else
				step_ok=0
				fail "$file" "$gvf_line" \
					"setup-go step takes its module file from the expression '$gvf_value'. An expression shows only that a value is derived, never from what, so exactly one shape is accepted, and only in a composite action manifest: \${{ inputs.<V> == '' && inputs.<F> || '' }}. Elsewhere, name the module: 'go-version-file: go.mod'."
			fi
			;;
		*)
			step_ok=0
			fail "$file" "$gvf_line" \
				"setup-go step reads '$gvf_value', which is not the module that declares the toolchain. Use 'go-version-file: go.mod'; only the root go.mod carries 'toolchain $toolchain'."
			;;
		esac
	fi

	# The condition has to test the very input go-version forwards. Testing a
	# different one decides the fallback on a value that has nothing to do with
	# what setup-go was handed.
	if [[ -n $gvf_cond_input && -n $gv_input && $gvf_cond_input != "$gv_input" ]]; then
		step_ok=0
		fail "$file" "$gvf_line" \
			"the go-version-file fallback tests 'inputs.$gvf_cond_input' while go-version forwards 'inputs.$gv_input'. The condition has to test the input the step actually received, or it decides the fallback on an unrelated value."
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
		# Record the ROLE each input plays, which the accepted shape makes
		# unambiguous: one input is handed to setup-go as the version, one names
		# the module. Recording only the set of inputs mentioned cannot say which
		# is which, and a rule stated over that set cannot decide what the
		# expression returns.
		[[ -z $gv_input ]] || forwarded_inputs="${forwarded_inputs}${file}:version:${gv_input}"$'\n'
		[[ -z $gvf_file_input ]] || forwarded_inputs="${forwarded_inputs}${file}:file:${gvf_file_input}"$'\n'
	fi
	# The reference is matched with optional YAML quoting. `uses:` accepts
	# actions/setup-go@v7, "actions/setup-go@v7" and 'actions/setup-go@v7'
	# identically, so a plain substring search enumerates only the spelling
	# somebody happened to use -- and a step this scan never finds is a step
	# whose version nothing judges, while the vacuity threshold stays satisfied
	# by its neighbors.
done < <(git grep -niE 'uses:[[:space:]]*["'"'"']?actions/setup-go@' -- .github)

# D1b: the enumeration above is a census, not a sample.
#
# Widening the pattern only ever covers the spellings someone thought of, and
# this gate has already been caught out twice by exactly that -- once on a
# quoted `uses:` value, once on a quoted input default. So the shape match is
# paired with a count that needs no shape: every line under .github that mentions
# the action at all must have been enumerated as a step.
#
# The anti-vacuity floor below cannot do this job. It fires when the scan
# collapses, and the dangerous case is the scan losing ONE step: 26 of 27 still
# clears the floor, and the version key inside the step that left is read by
# nothing. A disagreement here is a failure, so the next spelling that escapes
# the pattern reddens the gate rather than shrinking the sample.
setup_go_mentions="$(git grep -ci 'actions/setup-go@' -- .github | awk -F: '{n += $2} END {print n+0}')"
if ((setup_go_mentions != setup_go_steps)); then
	printf 'go toolchain check: %d lines under .github mention actions/setup-go@ but %d were enumerated as steps. A reference spelled in a way the scan does not reach leaves its version key unjudged\n' \
		"$setup_go_mentions" "$setup_go_steps" >&2
	exit 1
fi

# D1c: each forwarded input's default is judged by the ROLE the shape gives it.
#
# D2 selects the defaults it judges BY NAME -- inputs whose name mentions go or
# toolchain -- and a forwarded input can be called anything, so a rename put its
# default outside D2's reach while the forward stayed genuine. What earns the
# exemption is that a setup-go step forwards this input, so that is what selects
# the default to judge, and the accepted shape says which of the two roles it
# plays:
#
#   the input handed to setup-go as the version -> must default to EMPTY
#   the input that names the module             -> must default to the root go.mod
#
# "Empty", not "not a version". `stable`, `oldstable` and `1.x` are all valid
# setup-go selectors and none of them looks numeric; any of them, non-empty,
# makes setup-go prefer go-version and ignore go-version-file entirely. The
# forwarded version input exists to be overridden by a caller, so its own default
# has to select nothing at all.
#
# The module input's default must also EXIST. An absent default arrives as the
# empty string, and an empty go-version-file names no module -- which is the
# whole failure this exemption has to rule out.
while IFS=: read -r forwarding_manifest forwarded_key forwarded_input; do
	[[ -n $forwarding_manifest && -n $forwarded_key && -n $forwarded_input ]] || continue
	default_row="$(input_default "$forwarding_manifest" "$forwarded_input")"

	if [[ -z $default_row ]]; then
		if [[ $forwarded_key == file ]]; then
			fail "$forwarding_manifest" "1" \
				"input '$forwarded_input' names the module setup-go reads, but declares no default. An absent default arrives as the empty string, and an empty go-version-file names no module; default it to 'go.mod'."
		fi
		continue
	fi

	default_line="${default_row%%:*}"
	default_clean="$(strip_value "${default_row#*:}")"

	if is_block_scalar "$default_clean"; then
		fail "$forwarding_manifest" "$default_line" \
			"input '$forwarded_input' is forwarded to setup-go and declares its default as the block scalar '$default_clean', whose value is on the following lines and cannot be read here. Write a plain single-line default."
		continue
	fi

	if [[ $forwarded_key == file ]]; then
		case "$default_clean" in
		go.mod | ./go.mod) ;;
		*)
			fail "$forwarding_manifest" "$default_line" \
				"input '$forwarded_input' names the module setup-go reads and defaults to '$default_clean', which is not the module that declares the toolchain. Default it to 'go.mod'; only the root go.mod carries 'toolchain $toolchain'."
			;;
		esac
		continue
	fi

	if [[ -n $default_clean ]]; then
		fail "$forwarding_manifest" "$default_line" \
			"input '$forwarded_input' is handed to setup-go as the Go version and defaults to '$default_clean'. Any non-empty value here -- 'stable' and '1.x' as much as '1.25.0' -- makes setup-go prefer it and ignore go-version-file, so the toolchain stops being read from go.mod. Default it to the empty string."
	fi
done < <(printf '%s' "$forwarded_inputs" | sort -u)

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
	# The shape decision lives here rather than in the awk, so that the version
	# test and the block-scalar refusal are made in one place on one value. The
	# awk reports every default it finds on a go-ish input; what counts as
	# unacceptable is decided once, the same way D1c and D3 decide it.
	while IFS=: read -r line input text; do
		clean="$(strip_value "$text")"
		if is_block_scalar "$clean"; then
			fail "$action_file" "$line" \
				"composite action input '$input' declares its default as the block scalar '$clean', whose value is on the following lines and cannot be read here. Write a plain single-line default."
			continue
		fi
		case "$clean" in
		[0-9]*.[0-9]*)
			fail "$action_file" "$line" \
				"composite action input '$input' defaults to a Go version literal ($clean). Leave it empty and read the caller's module through a go-version-file input."
			;;
		esac
	done < <(awk '
		/^inputs:/ { in_inputs = 1; next }
		/^[^[:space:]#]/ { in_inputs = 0 }
		in_inputs && /^  ["'"'"']?[A-Za-z0-9_-]+["'"'"']?:[[:space:]]*$/ {
			input = $1
			sub(/:$/, "", input)
			gsub(/^["'"'"']|["'"'"']$/, "", input)
			next
		}
		# The quoting comes off BEFORE the value is judged, never as part of a
		# pattern. YAML spells the same scalar `1.26.5`, "1.26.5" and
		# '"'"'1.26.5'"'"', so a pattern that admits only one of them lets a
		# version through under either of the others -- and a pinned, non-empty
		# go-version makes setup-go ignore go-version-file entirely, which puts
		# the action back on a literal with this gate still green.
		in_inputs && input ~ /[Gg][Oo]|[Tt]oolchain/ && /^[[:space:]]+default:/ {
			value = $0
			sub(/^[[:space:]]*default:[[:space:]]*/, "", value)
			print NR ":" input ":" value
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
	if grep -qiE 'uses:[[:space:]]*["'"'"']?actions/setup-go@' "$action_file"; then
		# Collected with a read loop rather than `mapfile`, which is a bash 4
		# builtin: macOS still ships bash 3.2, and a developer running this
		# locally would otherwise get exit 127 instead of a verdict.
		gvf_defaults=()
		while IFS= read -r gvf_row; do
			gvf_defaults+=("$gvf_row")
		done < <(awk '
			/^inputs:/ { in_inputs = 1; next }
			/^[^[:space:]#]/ { in_inputs = 0 }
			in_inputs && /^  ["'"'"']?[A-Za-z0-9_-]+["'"'"']?:[[:space:]]*$/ {
				input = $1
				sub(/:$/, "", input)
				gsub(/^["'"'"']|["'"'"']$/, "", input)
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
			clean="$(strip_value "$value")"
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
#
# The scalar is judged after its quoting comes off, for the same reason as D2:
# `go: 1.26.6`, `go: "1.26.6"` and `go: '1.26.6'` are one value in three
# spellings, and a detector that recognizes only some of them stays silent on
# the rest.
if [[ -f .golangci.yml ]]; then
	while IFS=: read -r file line text; do
		clean="$(strip_value "$text")"
		if is_block_scalar "$clean"; then
			fail "$file" "$line" \
				"golangci-lint declares run.go as the block scalar '$clean', whose value is on the following lines and cannot be read here. Delete run.go; its documented default is already the go directive from go.mod."
			continue
		fi
		case "$clean" in
		[0-9]*.[0-9]*)
			fail "$file" "$line" \
				"golangci-lint declares run.go ($clean). Delete it; its documented default is already the go directive from go.mod."
			;;
		esac
	done < <(awk '/^run:/{in_run=1; next} /^[^[:space:]#]/{in_run=0} in_run && /^[[:space:]]+go:/{value = $0; sub(/^[[:space:]]*go:[[:space:]]*/, "", value); print FILENAME ":" NR ":" value}' .golangci.yml)
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
