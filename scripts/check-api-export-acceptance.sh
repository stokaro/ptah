#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ptah_bin="${PTAH_BIN:-$repo_root/bin/ptah}"
workspace="$(mktemp -d "${TMPDIR:-/tmp}/ptah-api-export-acceptance.XXXXXX")"
trap 'rm -rf "$workspace"' EXIT

if [[ ! -x "$ptah_bin" ]]; then
	printf 'ptah binary is not executable: %s\n' "$ptah_bin" >&2
	exit 1
fi

"$ptah_bin" schema export \
	--to openapi-v3 \
	--root-dir "$repo_root/stubs" \
	--out "$workspace/openapi.yaml"
"$ptah_bin" schema export \
	--to graphql \
	--root-dir "$repo_root/stubs" \
	--out "$workspace/schema.graphql"

npx -y @redocly/cli@1 lint "$workspace/openapi.yaml"

mkdir -p "$workspace/graphql"
(
	cd "$workspace/graphql"
	npm init --yes >/dev/null 2>&1
)
npm install --silent --prefix "$workspace/graphql" graphql@16

# check_graphql <file> <executable|types-only>
#
# Both modes require parse + buildSchema. The second argument says whether the
# document is also a complete executable schema: graphql-js reports a missing
# Query root from validateSchema, not from buildSchema, so a types-only document
# would otherwise pass a check that never looked at the root at all.
check_graphql() {
	NODE_PATH="$workspace/graphql/node_modules" node - "$1" "$2" <<'JS'
const fs = require("fs");
const { buildSchema, parse, validateSchema } = require("graphql");

const [file, mode] = process.argv.slice(2);
const sdl = fs.readFileSync(file, "utf8");
parse(sdl);
const errors = validateSchema(buildSchema(sdl)).map((e) => e.message);

if (mode === "executable") {
  if (errors.length !== 0) {
    console.error(`${file}: expected a valid executable schema, got: ${errors.join("; ")}`);
    process.exit(1);
  }
} else if (mode === "types-only") {
  // Exactly one complaint, and it is the absent root operation type. Anything
  // else means the type half of the document is broken on its own.
  if (errors.length !== 1 || !errors[0].includes("Query root type must be provided")) {
    console.error(`${file}: expected only a missing Query root, got: ${errors.join("; ") || "no errors"}`);
    process.exit(1);
  }
} else {
  console.error(`unknown mode ${mode}`);
  process.exit(1);
}
console.log(`graphql-js parse + buildSchema + validateSchema (${mode}): OK ${file}`);
JS
}

# refuse_if_present <file> <marker>
refuse_if_present() {
	if grep -qF -- "$2" "$1"; then
		printf '%s must not contain %q\n' "$1" "$2" >&2
		exit 1
	fi
}

# require_present <file> <marker>
require_present() {
	if ! grep -qF -- "$2" "$1"; then
		printf '%s must contain %q\n' "$1" "$2" >&2
		exit 1
	fi
}

# The default export is types-only (issue #906). Ptah generates no resolvers,
# authorization, or data access, so an operation-shaped schema is opt-in.
check_graphql "$workspace/schema.graphql" types-only
for marker in 'type Query' 'input ' 'Connection' 'Edge' 'PageInfo'; do
	refuse_if_present "$workspace/schema.graphql" "$marker"
done
require_present "$workspace/schema.graphql" 'type Product {'

# Every supported operation profile builds. The profiles that select a query
# shape must additionally be complete executable schemas.
profiles=(
	"list:executable"
	"by-id:executable"
	"create-input:types-only"
	"update-input:types-only"
	"list,by-id:executable"
	"create-input,update-input:types-only"
	"list,by-id,create-input,update-input:executable"
)
for entry in "${profiles[@]}"; do
	profile="${entry%:*}"
	mode="${entry##*:}"
	out="$workspace/schema-${profile//,/_}.graphql"
	"$ptah_bin" schema export \
		--to graphql \
		--root-dir "$repo_root/stubs" \
		--graphql-operations "$profile" \
		--out "$out"
	check_graphql "$out" "$mode"
	require_present "$out" 'Ptah generates no resolvers'
done

# The selected shapes are the ones that appear, and no others.
require_present "$workspace/schema-list.graphql" 'type ProductConnection {'
refuse_if_present "$workspace/schema-list.graphql" 'input '
require_present "$workspace/schema-by-id.graphql" 'product(id: ID!): Product'
refuse_if_present "$workspace/schema-by-id.graphql" 'Connection'
require_present "$workspace/schema-create-input.graphql" 'input ProductCreateInput {'
refuse_if_present "$workspace/schema-create-input.graphql" 'type Query'
require_present "$workspace/schema-update-input.graphql" 'input ProductUpdateInput {'
refuse_if_present "$workspace/schema-update-input.graphql" 'ProductCreateInput'

# "none" is the spelling of the default, and it must produce the same bytes.
"$ptah_bin" schema export \
	--to graphql \
	--root-dir "$repo_root/stubs" \
	--graphql-operations none \
	--out "$workspace/schema-none.graphql"
diff -u "$workspace/schema.graphql" "$workspace/schema-none.graphql"

# An unrecognized shape is refused rather than silently dropped, and the run
# writes nothing.
set +e
"$ptah_bin" schema export \
	--to graphql \
	--root-dir "$repo_root/stubs" \
	--graphql-operations mutations \
	--out "$workspace/schema-refused.graphql" >"$workspace/refused.out" 2>"$workspace/refused.err"
refused_code=$?
set -e
if [[ $refused_code -ne 2 ]]; then
	printf 'expected exit 2 for an unknown operation shape, got %d\n' "$refused_code" >&2
	exit 1
fi
if [[ -e "$workspace/schema-refused.graphql" ]]; then
	printf 'a refused selection must not write an export\n' >&2
	exit 1
fi
grep -qF 'unknown GraphQL operation "mutations"' "$workspace/refused.err"

echo "graphql operation profiles: OK"
