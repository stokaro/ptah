#!/usr/bin/env bash

set -euo pipefail

# Every container image the developer stack starts must be pinned to a tag that
# CI also pins somewhere under .github/workflows.
#
# This is a drift check, not a registry check: it runs offline, so it cannot ask
# Docker Hub whether a tag exists. It catches the same failure a different way.
# docker-compose.yaml pinned clickhouse/clickhouse-server:26 while CI pinned
# :26.7, and ClickHouse publishes no major-only tag, so `docker compose up`
# answered "not found" for anyone who ran the developer stack. CI never noticed,
# because CI does not read docker-compose.yaml. A tag no workflow pins is a tag
# nothing tests, which is what makes it free to rot.
#
# An image that legitimately belongs to the developer stack alone -- something
# CI has no job for -- needs an exemption added here with the reason, so the
# decision is written down rather than inferred from the check passing.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

compose_file="docker-compose.yaml"
workflow_dir=".github/workflows"

if [[ ! -f $compose_file ]]; then
	printf 'compose image pin check: %s not found\n' "$compose_file" >&2
	exit 1
fi
if [[ ! -d $workflow_dir ]]; then
	printf 'compose image pin check: %s not found\n' "$workflow_dir" >&2
	exit 1
fi

status=0
checked=0
workflow_pins="$(mktemp)"
trap 'rm -f "$workflow_pins"' EXIT

# Only executable workflow image declarations count. Searching the workflow
# text lets a comment, an echo, or an unused variable keep a stale Compose pin
# green. The extractor reads job container and service image fields plus the
# image operand of docker run commands; later command arguments, and the lines
# a heredoc passes to a command as data, do not count.
if [[ -n ${PTAH_WORKFLOW_IMAGE_PINS:-} ]]; then
	"$PTAH_WORKFLOW_IMAGE_PINS" "$workflow_dir" >"$workflow_pins"
else
	go run ./internal/cmd/workflowimagepins "$workflow_dir" >"$workflow_pins"
fi

while IFS= read -r image; do
	[[ -z $image ]] && continue
	checked=$((checked + 1))

	if [[ $image != *:* ]]; then
		printf 'compose image pin check: %s has no tag; pin an exact version\n' "$image" >&2
		status=1
		continue
	fi

	# The match must end at a token boundary. A substring match reports
	# clickhouse/clickhouse-server:26 as present because the workflows pin
	# :26.7 -- which is precisely the drift this check exists to catch, so a
	# grep -F here would pass green on the defect that motivated the file.
	escaped="$(printf '%s' "$image" | sed 's/[][\.^$*+?(){}|\/]/\\&/g')"
	if ! grep -qE -- "(^|[[:space:]\"'])${escaped}([[:space:]\"']|\$)" "$workflow_pins"; then
		printf 'compose image pin check: %s is pinned in %s but in no workflow under %s\n' \
			"$image" "$compose_file" "$workflow_dir" >&2
		status=1
	fi
done < <(grep -oE '^[[:space:]]+image:[[:space:]]+[^[:space:]]+' "$compose_file" | awk '{print $2}')

if ((checked == 0)); then
	printf 'compose image pin check: no image pins found in %s; the check would pass vacuously\n' \
		"$compose_file" >&2
	exit 1
fi

if ((status != 0)); then
	exit "$status"
fi

printf 'compose image pin check: %d image pins agree with %s\n' "$checked" "$workflow_dir"
