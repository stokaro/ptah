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
# The contours are the two runs `make lint-golangci` makes in every module. The
# default contour passes no build tags. The tagged contour passes every opt-in
# tag the tree uses at once, as scripts/list-build-tags.sh lists them:
# `--build-tags=integration,observability` today. A file behind a tag is
# outside the default build, so one run never reads it: without the tagged
# steps, no golangci-lint run reads the integration tests or the OTLP exporter
# (stokaro/ptah#3882, stokaro/ptah#3895).
#
# The tag list is discovered, so a tag added to a file fails here until the
# workflow lints it. A step's tags are compared as a set, in any order.
#
# go-security.yml is read too, for its gosec code-scanning run. That job scans
# the root module only, so only the contours are required of it: without the
# tagged run, code scanning shows none of the tagged files (stokaro/ptah#3895).

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

modules="$(scripts/list-go-modules.sh)"
if [[ -z $modules ]]; then
	printf 'go module lint coverage: no modules discovered; refusing to report a vacuous pass\n' >&2
	exit 1
fi

tagged="$(scripts/list-build-tags.sh --joined)"
if [[ -z $tagged ]]; then
	printf 'go module lint coverage: no opt-in build tag discovered; refusing to report a vacuous pass\n' >&2
	exit 1
fi

# lint_steps prints each golangci-lint step of a workflow, as
# "<job>\t<module>\t<contour>". The module is the step's working-directory, or
# `.` when it names none; the contour is the value of --build-tags, or `default`
# when it passes none. A job is a key under the top-level `jobs:`, and a step is
# an item of its `steps:` list, ending at the next item or where the list does.
#
# The step is the unit because the property belongs to it. Counting lines over
# the whole file cannot tell a job that lints the root in both contours from
# one that lints it twice in the same contour.
lint_steps() {
	awk '
	# sorted returns a comma-separated list sorted, so two spellings of one
	# set compare equal. An insertion sort: asort is not in every awk.
	function sorted(list,    parts, n, i, j, v, out) {
		n = split(list, parts, ",")
		for (i = 2; i <= n; i++) {
			v = parts[i]
			for (j = i - 1; j >= 1 && parts[j] > v; j--) {
				parts[j + 1] = parts[j]
			}
			parts[j + 1] = v
		}
		out = parts[1]
		for (i = 2; i <= n; i++) {
			out = out "," parts[i]
		}
		return out
	}
	function flush() {
		if (lint) {
			printf "%s\t%s\t%s\n", job, (dir == "" ? "." : dir), (tags == "" ? "default" : sorted(tags))
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
	' "$1"
}

contours=(default "$tagged")
tab=$'\t'
missing=0
summary=()

# require_coverage checks that every job in the workflow that runs golangci-lint
# at all lints each of the given modules in every contour. It reports each gap
# and sets missing.
#
# Asking each job rather than the file matters: searched over the file, a module
# dropped from one job stays green on the strength of the other, which is the
# same "somewhere in the file" reasoning that let the modules go uncovered in
# the first place.
require_coverage() {
	local workflow=$1 modules=$2 steps jobs job module name where contour
	if [[ ! -f $workflow ]]; then
		printf 'go module lint coverage: %s not found\n' "$workflow" >&2
		missing=1
		return
	fi
	steps="$(lint_steps "$workflow")"
	if [[ -z $steps ]]; then
		printf 'go module lint coverage: %s runs golangci-lint in no job at all\n' "$workflow" >&2
		missing=1
		return
	fi
	jobs="$(cut -f1 <<<"$steps" | sort -u)"
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
				if [[ $contour == "default" ]]; then
					printf 'go module lint coverage: job %s in %s does not lint %s without build tags\n' \
						"$job" "$workflow" "$name" >&2
					printf '  it needs a golangci-lint step with %s and no --build-tags\n' "$where" >&2
				else
					printf 'go module lint coverage: job %s in %s does not lint %s with --build-tags=%s\n' \
						"$job" "$workflow" "$name" "$contour" >&2
					printf '  it needs a golangci-lint step with %s and `--build-tags=%s`\n' "$where" "$contour" >&2
					printf '  the tags are every opt-in tag scripts/list-build-tags.sh finds, in any order\n' >&2
				fi
				missing=1
			done
		done <<<"$modules"
	done <<<"$jobs"
	summary+=("$(printf '%s: modules %d, contours %d, jobs %d' "$workflow" \
		"$(printf '%s\n' "$modules" | wc -l | tr -d ' ')" "${#contours[@]}" \
		"$(printf '%s\n' "$jobs" | wc -l | tr -d ' ')")")
}

# Every job in go-lint.yml that runs golangci-lint -- two today, Ubuntu and
# Windows -- lints every module, the root included, in every contour.
require_coverage .github/workflows/go-lint.yml "$modules"

# The gosec job in go-security.yml scans the root module for code scanning.
require_coverage .github/workflows/go-security.yml .

if [[ $missing -ne 0 ]]; then
	exit 1
fi

printf 'go module lint coverage: OK\n'
printf '  %s\n' "${summary[@]}"
