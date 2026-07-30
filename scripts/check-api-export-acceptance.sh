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
NODE_PATH="$workspace/graphql/node_modules" node - "$workspace/schema.graphql" <<'JS'
const fs = require("fs");
const { buildSchema, parse } = require("graphql");

const sdl = fs.readFileSync(process.argv[2], "utf8");
parse(sdl);
buildSchema(sdl);
console.log("graphql-js parse + buildSchema: OK");
JS
