#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-compose-image-pins.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-compose-pins.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

go build -o "$work_dir/workflowimagepins" ./internal/cmd/workflowimagepins

write_repo() {
	local workflow=$1

	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/.github/workflows" "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-compose-image-pins.sh"
	printf 'services:\n  database:\n    image: example/database:1.2.3\n' >"$work_dir/repo/docker-compose.yaml"
	printf '%s\n' "$workflow" >"$work_dir/repo/.github/workflows/test.yml"
}

assert_rejected() {
	local name=$1
	if (cd "$work_dir/repo" && PTAH_WORKFLOW_IMAGE_PINS="$work_dir/workflowimagepins" \
		scripts/check-compose-image-pins.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'compose image pin self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	grep -qF 'example/database:1.2.3 is pinned' "$work_dir/err"
}

assert_accepted() {
	(cd "$work_dir/repo" && PTAH_WORKFLOW_IMAGE_PINS="$work_dir/workflowimagepins" \
		scripts/check-compose-image-pins.sh) >/dev/null
}

assert_guard_error() {
	local name=$1
	local expected=$2
	if (cd "$work_dir/repo" && PTAH_WORKFLOW_IMAGE_PINS="$work_dir/workflowimagepins" \
		scripts/check-compose-image-pins.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'compose image pin self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	grep -qF "$expected" "$work_dir/err"
}

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo "example/database:1.2.3 "'
assert_rejected 'echo text is not an invocation'

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: docker run alpine:3 echo example/database:1.2.3'
assert_rejected 'a docker command argument is not its image operand'

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    # old image: example/database:1.2.3
    steps:
      - run: true'
assert_rejected 'comment text is not an invocation'

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    env:
      image: example/database:1.2.3
    steps:
      - run: true'
assert_rejected 'an environment key named image is not a container declaration'

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - if: ${{ false }}
        run: docker run example/database:1.2.3'
assert_rejected 'a statically disabled step is not an executable pin'

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    services:
      database:
        image: example/database:1.2.3'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: docker run -dit example/database:1.2.3'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    container:
      image: example/database:1.2.3
    steps:
      - run: true'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: docker run example/database:1.2.3'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: "docker run example/database:1.2.3"'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: >-
          docker run
          example/database:1.2.3'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: |
          docker run --detach --name database \
            --publish 1234:1234 \
            example/database:1.2.3 \
            serve'
assert_accepted

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: docker run --definitely-invalid=value example/database:1.2.3 || true'
assert_guard_error 'an unknown attached docker option fails closed' 'unsupported docker run option'

write_repo 'jobs:
  test:
    runs-on: ubuntu-latest
    services:
      database:
        image: example/database:1.2.30'
assert_rejected 'a longer tag is not the same pin'

printf 'compose image pin self-test: executable workflow invocations are enforced\n'
