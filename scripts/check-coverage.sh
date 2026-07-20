#!/usr/bin/env bash
set -euo pipefail

threshold="${PTAH_COVERAGE_THRESHOLD:-70.0}"
profile="${PTAH_COVERAGE_PROFILE:-coverage.out}"

packages="$(
	go list ./config/... ./core/... ./migration/... ./dbschema/... ./internal/... |
		grep -vE '/core/ast/mocks$|/core/goschema/testutil$|/core/renderer/internal/dialects/internal/bufwriter$|/internal/dbschema/dbtest$|/internal/examples(/|$)|/internal/testutils$|/migration/generator/example$|/migration/internal/typechange$'
)"

go test -covermode=atomic -coverprofile="$profile" $packages

coverage="$(go tool cover -func="$profile" | awk '/^total:/ {gsub(/%/, "", $3); print $3}')"

awk -v coverage="$coverage" -v threshold="$threshold" 'BEGIN {
	if (coverage + 0 < threshold + 0) {
		printf "coverage %.1f%% is below threshold %.1f%%\n", coverage, threshold
		exit 1
	}
	printf "coverage %.1f%% meets threshold %.1f%%\n", coverage, threshold
}'
