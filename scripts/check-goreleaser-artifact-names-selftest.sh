#!/usr/bin/env bash
# Proves check-goreleaser-artifact-names.sh rejects each way a snapshot version
# can put a path separator into an artifact name.
#
# The failure it exists to stop happened: a tag named `prototype/canonical-core`
# was pushed as a marker, `git describe` reached it, and from that moment every
# branch's release job died with "could not open ... for writing: No such file
# or directory" -- a message naming a missing directory and saying nothing about
# the tag that produced it (stokaro/ptah#2509 moves the fixtures here).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-goreleaser-artifact-names.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-goreleaser-names.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# write_repo builds a throwaway repository holding one config. The gate asks git
# for the root, so the fixture has to be a repository.
write_repo() {
	local config=$1
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-goreleaser-artifact-names.sh"
	printf '%s\n' "$config" >"$work_dir/repo/.goreleaser.yaml"
}

assert_rejected() {
	local name=$1 expected=$2
	if (cd "$work_dir/repo" && scripts/check-goreleaser-artifact-names.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'goreleaser artifact name self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'goreleaser artifact name self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	local name=$1
	if ! (cd "$work_dir/repo" && scripts/check-goreleaser-artifact-names.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'goreleaser artifact name self-test: %s was rejected:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

# No template at all: goreleaser derives the version from the nearest tag, which
# is the state the failure happened in.
write_repo 'builds:
  - id: thing
snapshot: {}'
assert_rejected 'no snapshot version template' 'snapshot.version_template is not set'

# The four values that carry whatever somebody typed into a ref.
for field in .Version .Tag .Branch .Summary; do
	write_repo "snapshot:
  version_template: \"0.0.0-SNAPSHOT-{{ ${field} }}\""
	assert_rejected "a template reading ${field}" "version_template reads ${field}"
done

# A slash written straight into the template needs no ref to break the build.
write_repo 'snapshot:
  version_template: "nightly/0.0.0"'
assert_rejected 'a literal slash' 'contains a literal slash'

# The control. Without it a gate refusing every template satisfies every row
# above, and the repository could not release at all.
write_repo 'snapshot:
  version_template: "0.0.0-SNAPSHOT-{{ .ShortCommit }}"'
assert_accepted 'the commit-derived version this repository uses'

# Quoting is not the rule. The gate strips it, and a reader who wrote the same
# value unquoted must get the same answer.
write_repo 'snapshot:
  version_template: 0.0.0-SNAPSHOT-{{ .ShortCommit }}'
assert_accepted 'an unquoted template'

printf 'goreleaser artifact name self-test: six rejections and two accepted spellings of the commit-derived version\n'
