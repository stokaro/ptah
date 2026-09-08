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

# The diagram carries the entities and the relationships, which is the part of
# the schema every source describes the same way. Columns are left out on
# purpose: a type keeps the spelling of the document it was written in --
# `integer` in the HCL and DBML fixtures, `INTEGER` in the SQL and YAML ones --
# so --include-columns would compare the fixtures' prose rather than what Ptah
# derived from them.
viz_diagram() {
	local name="$1"
	shift
	local drawn="$work_dir/viz-$name.mmd"

	"$ptah_bin" viz "$@" --dialect sqlite >"$drawn"

	if [ ! -s "$drawn" ]; then
		echo "check-source-equivalence: viz $name produced no diagram" >&2
		exit 1
	fi
	for marker in 'authors {' 'books {' 'tags {' 'book_tags {' \
		'authors ||--o{ books' 'books ||--o{ book_tags' 'tags ||--o{ book_tags'; do
		if ! grep -Fq "$marker" "$drawn"; then
			echo "check-source-equivalence: viz $name omitted $marker" >&2
			exit 1
		fi
	done

	if [ ! -f "$work_dir/viz-baseline.mmd" ]; then
		cp "$drawn" "$work_dir/viz-baseline.mmd"
	elif ! cmp -s "$work_dir/viz-baseline.mmd" "$drawn"; then
		echo "check-source-equivalence: viz $name differs from the canonical SQL source" >&2
		diff -u "$work_dir/viz-baseline.mmd" "$drawn" >&2 || true
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

viz_diagram sql --schema-file "$fixture/schema.sql"
viz_diagram yaml --schema-file "$fixture/schema.yaml"
viz_diagram hcl --schema-file "$fixture/schema.hcl"
viz_diagram dbml --schema-file "$fixture/schema.dbml"
viz_diagram go --root-dir "$fixture/models"

echo "check-source-equivalence: OK (SQL, YAML, HCL, DBML, Go, explicit external, and configured external; ptah viz draws one diagram from the five local sources)"
