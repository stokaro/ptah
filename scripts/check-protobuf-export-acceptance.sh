#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ptah_bin="${PTAH_BIN:-$repo_root/bin/ptah}"
workspace="$(mktemp -d "${TMPDIR:-/tmp}/ptah-protobuf-export-acceptance.XXXXXX")"
trap 'rm -rf "$workspace"' EXIT

proto_package="acme.inventory.v1"
proto_relative_path="acme/inventory/v1/schema.proto"
go_package="example.com/ptahprotoacceptance/acme/inventory/v1;inventoryv1"

require_command() {
	if ! command -v "$1" >/dev/null 2>&1; then
		printf 'required command is unavailable: %s\n' "$1" >&2
		exit 1
	fi
}

for command in buf protoc protoc-gen-go python3; do
	require_command "$command"
done
if [[ ! -x "$ptah_bin" ]]; then
	printf 'ptah binary is not executable: %s\n' "$ptah_bin" >&2
	exit 1
fi
if [[ -z "${PROTOC_INCLUDE:-}" || ! -d "$PROTOC_INCLUDE" ]]; then
	printf 'PROTOC_INCLUDE must name the include directory shipped with protoc: %s\n' \
		"${PROTOC_INCLUDE:-<unset>}" >&2
	exit 1
fi

write_buf_config() {
	local proto_root="$1"
	cat >"$proto_root/buf.yaml" <<'YAML'
version: v2
modules:
  - path: .
lint:
  use:
    - STANDARD
breaking:
  use:
    - WIRE_JSON
YAML
	cat >"$proto_root/buf.gen.yaml" <<'YAML'
version: v2
plugins:
  - local: protoc-gen-go
    out: ../gen
    opt: paths=source_relative
YAML
}

export_schema() {
	local source_root="$1"
	local proto_root="$2"
	shift 2
	"$ptah_bin" schema export \
		--to protobuf \
		--root-dir "$source_root" \
		--out "$proto_root/$proto_relative_path" \
		--proto-package "$proto_package" \
		--go-package "$go_package" \
		"$@"
}

check_proto_module() {
	local proto_root="$1"
	(
		cd "$proto_root"
		buf lint
		buf format --diff --exit-code
		buf build -o /dev/null
	)
}

assert_changed() {
	local before="$1"
	local after="$2"
	if cmp -s "$before" "$after"; then
		printf 'expected protobuf export to change, but it remained byte-identical\n' >&2
		exit 1
	fi
	diff -u "$before" "$after" || true
}

base_proto="$workspace/proto"
mkdir -p "$(dirname "$base_proto/$proto_relative_path")"
write_buf_config "$base_proto"
export_schema "$repo_root/stubs" "$base_proto"
check_proto_module "$base_proto"

(
	cd "$base_proto"
	protoc \
		--proto_path=. \
		--proto_path="$PROTOC_INCLUDE" \
		--descriptor_set_out="$workspace/schema.pb" \
		"$proto_relative_path"
	buf generate
)

go_language_version="$(awk '/^go /{print $2; exit}' "$repo_root/go.mod")"
cat >"$workspace/gen/go.mod" <<EOF
module example.com/ptahprotoacceptance

go ${go_language_version}

require google.golang.org/protobuf ${PROTOC_GEN_GO_VERSION:-v1.36.11}
EOF
(
	cd "$workspace/gen"
	go mod tidy
	go build ./...
)

cp "$base_proto/$proto_relative_path" "$workspace/schema.first.proto"
export_schema "$repo_root/stubs" "$base_proto"
cmp "$workspace/schema.first.proto" "$base_proto/$proto_relative_path"

baseline="$workspace/proto-baseline"
cp -R "$base_proto" "$baseline"

run_compatible_case() {
	local name="$1"
	local mutation="$2"
	shift 2
	local case_root="$workspace/cases/$name"
	local source_root="$case_root/stubs"
	local proto_root="$case_root/proto"

	mkdir -p "$case_root"
	cp -R "$repo_root/stubs" "$source_root"
	cp -R "$baseline" "$proto_root"
	python3 "$repo_root/scripts/mutate-protobuf-export-fixture.py" "$mutation" "$source_root"
	export_schema "$source_root" "$proto_root" "$@"
	check_proto_module "$proto_root"
	(
		cd "$proto_root"
		buf breaking --against "$baseline"
	)

	if [[ "$mutation" == "reorder-columns" ]]; then
		cmp "$baseline/$proto_relative_path" "$proto_root/$proto_relative_path"
	else
		assert_changed "$baseline/$proto_relative_path" "$proto_root/$proto_relative_path"
	fi
}

run_compatible_case additive add-column
run_compatible_case reordered reorder-columns
run_compatible_case removed-field remove-column
run_compatible_case tombstoned-type remove-type --proto-type-removal tombstone

breaking_root="$workspace/cases/incompatible-type"
cp -R "$repo_root/stubs" "$breaking_root-stubs"
cp -R "$baseline" "$breaking_root-proto"
python3 "$repo_root/scripts/mutate-protobuf-export-fixture.py" change-type "$breaking_root-stubs"
export_schema "$breaking_root-stubs" "$breaking_root-proto" \
	--proto-on-incompatible-change renumber
check_proto_module "$breaking_root-proto"
assert_changed "$baseline/$proto_relative_path" "$breaking_root-proto/$proto_relative_path"
if (
	cd "$breaking_root-proto"
	buf breaking --against "$baseline"
); then
	printf 'buf breaking unexpectedly accepted an incompatible field-type change\n' >&2
	exit 1
fi

printf 'Protobuf export acceptance: OK\n'
