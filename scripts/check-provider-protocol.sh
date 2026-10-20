#!/usr/bin/env bash
# Regenerate every tracked SDK protocol with the pinned Protobuf toolchain.
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
workspace="$(mktemp -d "${TMPDIR:-/tmp}/ptah-provider-protocol.XXXXXX")"
trap 'rm -rf "$workspace"' EXIT
protos=()
while IFS= read -r -d '' source; do
  protos+=("$source")
done < <(git ls-files -z 'provider-sdk/protocol/*.proto')
if [[ ${#protos[@]} -eq 0 ]]; then
  printf 'provider protocol inventory is empty\n' >&2
  exit 1
fi
protoc -I provider-sdk \
  --go_out="$workspace" --go_opt=paths=source_relative \
  --go-grpc_out="$workspace" --go-grpc_opt=paths=source_relative \
  "${protos[@]}"
checked=0
while IFS= read -r -d '' generated; do
  relative="${generated#provider-sdk/}"
  if ! cmp -s "$generated" "$workspace/$relative"; then
    printf 'provider binding differs from generated output: %s\n' "$generated" >&2
    exit 1
  fi
  checked=$((checked + 1))
done < <(git ls-files -z 'provider-sdk/protocol/*.pb.go')
if [[ $checked -ne $((${#protos[@]} * 2)) ]]; then
  printf 'provider protocol binding inventory is incomplete\n' >&2
  exit 1
fi
printf 'Provider protocol bindings match (%d files).\n' "$checked"
