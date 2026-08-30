#!/usr/bin/env bash
# Prove that Ptah's canonical schema-source fixtures reach the same normalized
# live schema through the built command, not only through a shared loader.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
fixture="${PTAH_SOURCE_FIXTURE:-$repo_root/docs/site/fixtures/source-equivalence}"

if [ ! -d "$fixture" ]; then
	echo "check-source-equivalence: fixture directory does not exist: $fixture" >&2
	exit 1
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-source-equivalence.XXXXXX")"
cleanup() {
	rm -rf "$work_dir"
}
trap cleanup EXIT

ptah_bin="${PTAH_BIN:-$work_dir/ptah}"
if [ -z "${PTAH_BIN:-}" ]; then
	(
		cd "$repo_root"
		go build -o "$ptah_bin" ./cmd/ptah
	)
fi

render_and_inspect() {
	local name="$1"
	shift
	local rendered="$work_dir/$name.sql"
	local normalized="$work_dir/$name.json"
	local database="$work_dir/$name.db"

	"$ptah_bin" schema render "$@" --dialect sqlite >"$rendered"
	"$ptah_bin" schema inspect \
		--schema-file "$rendered" \
		--dev-url "sqlite://$database" \
		--format json >"$normalized"

	if [ ! -s "$normalized" ]; then
		echo "check-source-equivalence: $name produced no normalized schema" >&2
		exit 1
	fi
	for marker in '"name":"authors"' '"name":"books"' '"name":"tags"' \
		'"name":"book_tags"' '"name":"idx_authors_name"' '"name":"idx_book_tags_pair"'; do
		if ! grep -Fq "$marker" "$normalized"; then
			echo "check-source-equivalence: $name omitted $marker" >&2
			exit 1
		fi
	done

	if [ ! -f "$work_dir/baseline.json" ]; then
		cp "$normalized" "$work_dir/baseline.json"
	elif ! cmp -s "$work_dir/baseline.json" "$normalized"; then
		echo "check-source-equivalence: $name differs from the canonical SQL source" >&2
		diff -u "$work_dir/baseline.json" "$normalized" >&2 || true
		exit 1
	fi
}

render_and_inspect sql --schema-file "$fixture/schema.sql"
render_and_inspect yaml --schema-file "$fixture/schema.yaml"
render_and_inspect hcl --schema-file "$fixture/schema.hcl"
render_and_inspect dbml --schema-file "$fixture/schema.dbml"
render_and_inspect go --root-dir "$fixture/models"
render_and_inspect external --schema-cmd "$fixture/external-schema.sh" --schema-format sql
(
	cd "$fixture"
	render_and_inspect configured-external \
		--config ptah.yaml \
		--allow-external-schema
)

echo "check-source-equivalence: OK (SQL, YAML, HCL, DBML, Go, explicit external, and configured external)"
