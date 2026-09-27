#!/usr/bin/env bash

set -euo pipefail

# Every Go module in this repository must be linted by CI in both build-tag
# contours, and this checks that the workflow says so for each one.
#
# The Makefile discovers its modules (see scripts/list-go-modules.sh), so
# `make lint` cannot miss one. The golangci-lint step cannot discover: the
# action lints one directory per invocation, so the workflow names the modules
# it visits. A hand-written list is exactly what went stale before -- until this
# check landed, golangci-lint ran from the repository root and therefore linted
# only the root module, leaving a nested published module and
# examples/orm-loaders/gorm unlinted, while nolintguard named each by hand
# in six places and qtlint discovered each by itself. Three tools, three
# different answers to the same question.
#
# A new module therefore fails here until the workflow visits it, which is the
# moment the decision is cheap.
#
# The contours are the two runs `make lint-golangci` makes in every module:
# none, and `--build-tags=integration`. A file behind `//go:build integration`
# is outside the default build, so one run never reads it. Before this check
# knew about contours, every golangci-lint step here passed no tags, and 84
# findings sat in integration/ with the lint job green (stokaro/ptah#3882).

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

workflow=".github/workflows/go-lint.yml"

if [[ ! -f $workflow ]]; then
	printf 'go module lint coverage: %s not found\n' "$workflow" >&2
	exit 1
fi

modules="$(scripts/list-go-modules.sh)"
if [[ -z $modules ]]; then
	printf 'go module lint coverage: no modules discovered; refusing to report a vacuous pass\n' >&2
	exit 1
fi

# Each golangci-lint step, as "<job>\t<module>\t<contour>". The module is the
# step's working-directory, or `.` when it names none; the contour is the value
# of --build-tags, or `default` when it passes none. A job is a key under the
# top-level `jobs:`, and a step is an item of its `steps:` list, ending at the
# next item or where the list does.
#
# The step is the unit because the property belongs to it. Counting lines over
# the whole file cannot tell a job that lints the root in both contours from
# one that lints it twice in the same contour.
steps="$(awk '
	function flush() {
		if (lint) {
			printf "%s\t%s\t%s\n", job, (dir == "" ? "." : dir), (tags == "" ? "default" : tags)
		}
		lint = 0
		dir = ""
		tags = ""
	}
	{
		line = $0
		if (line ~ /^[ \t]*#/ || line ~ /^[ \t]*$/) {
			next
		}
		sub(/[ \t]+#.*$/, "", line)
		match(line, /^ */)
		indent = RLENGTH
		body = substr(line, indent + 1)
		if (in_steps && indent <= steps_indent) {
			flush()
			in_steps = 0
		}
		if (indent == 0) {
			in_jobs = (body ~ /^jobs:[ \t]*$/)
			job = ""
			job_indent = -1
			next
		}
		if (in_jobs && !in_steps) {
			if (job_indent < 0) {
				job_indent = indent
			}
			if (indent == job_indent) {
				job = body
				sub(/:.*$/, "", job)
				next
			}
		}
		if (body ~ /^steps:[ \t]*$/) {
			in_steps = 1
			steps_indent = indent
			item_indent = -1
			next
		}
		if (!in_steps) {
			next
		}
		if (body ~ /^- /) {
			if (item_indent < 0) {
				item_indent = indent
			}
			if (indent == item_indent) {
				flush()
				body = substr(body, 3)
			}
		}
		if (body ~ /^uses:[ \t]*golangci\/golangci-lint-action@/) {
			lint = 1
		}
		if (body ~ /^working-directory:/) {
			dir = body
			sub(/^working-directory:[ \t]*/, "", dir)
			gsub(/["\047]/, "", dir)
		}
		if (match(body, /--build-tags[= ][^ \t"\047]+/)) {
			tags = substr(body, RSTART, RLENGTH)
			sub(/^--build-tags[= ]/, "", tags)
		}
	}
	END {
		flush()
	}
' "$workflow")"

if [[ -z $steps ]]; then
	printf 'go module lint coverage: %s runs golangci-lint in no job at all\n' "$workflow" >&2
	exit 1
fi

# Every job that runs golangci-lint at all -- two today, Ubuntu and Windows --
# has to lint every module, the root included, in every contour.
#
# Asking each job rather than the file matters: searched over the file, a module
# dropped from one job stays green on the strength of the other, which is the
# same "somewhere in the file" reasoning that let the modules go uncovered in
# the first place.
contours=(default integration)
tab=$'\t'
jobs="$(cut -f1 <<<"$steps" | sort -u)"

missing=0
while read -r job; do
	while read -r module; do
		name="$module"
		where="\`working-directory: $module\`"
		if [[ $module == "." ]]; then
			name="the root module"
			where="no \`working-directory:\`"
		fi
		for contour in "${contours[@]}"; do
			if grep -qxF "${job}${tab}${module}${tab}${contour}" <<<"$steps"; then
				continue
			fi
			printf 'go module lint coverage: job %s in %s does not lint %s in the %s contour\n' \
				"$job" "$workflow" "$name" "$contour" >&2
			if [[ $contour == "default" ]]; then
				printf '  it needs a golangci-lint step with %s and no --build-tags\n' "$where" >&2
			else
				printf '  it needs a golangci-lint step with %s and `--build-tags=%s`\n' "$where" "$contour" >&2
			fi
			missing=1
		done
	done <<<"$modules"
done <<<"$jobs"

if [[ $missing -ne 0 ]]; then
	exit 1
fi

printf 'go module lint coverage: OK (%d modules in %d contours across %d jobs)\n' \
	"$(printf '%s\n' "$modules" | wc -l | tr -d ' ')" "${#contours[@]}" \
	"$(printf '%s\n' "$jobs" | wc -l | tr -d ' ')"
