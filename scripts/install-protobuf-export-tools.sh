#!/usr/bin/env bash
set -euo pipefail

tools_dir="${1:?usage: install-protobuf-export-tools.sh <tools-directory>}"
buf_version="${BUF_VERSION:-v1.72.0}"
protoc_version="${PROTOC_VERSION:-35.1}"
protoc_gen_go_version="${PROTOC_GEN_GO_VERSION:-v1.36.11}"

case "$(uname -s)-$(uname -m)" in
Linux-x86_64)
	protoc_platform="linux-x86_64"
	protoc_sha256="6930ebf62bd4ea607b98fff052596c6ee564b9835b4ce172c75a3f53ae9d91b7"
	;;
Linux-aarch64 | Linux-arm64)
	protoc_platform="linux-aarch_64"
	protoc_sha256="01bf9d08808c7f96678b63f4bd8efa559bb4f83d5a7a270d5edaf507f9d5d9cf"
	;;
Darwin-x86_64)
	protoc_platform="osx-x86_64"
	protoc_sha256="537d73604a344ded6fc94e98e07e529d4fe3e4a0b09e59905353950fafc2a1f7"
	;;
Darwin-arm64)
	protoc_platform="osx-aarch_64"
	protoc_sha256="193289af0470c6a1aada357d4fba0bbf8d78bfaac8b5e42ca30af2ef75583de2"
	;;
*)
	printf 'unsupported protoc platform: %s-%s\n' "$(uname -s)" "$(uname -m)" >&2
	exit 1
	;;
esac

mkdir -p "$tools_dir/bin"

GOBIN="$tools_dir/bin" go install "github.com/bufbuild/buf/cmd/buf@${buf_version}"
GOBIN="$tools_dir/bin" go install "google.golang.org/protobuf/cmd/protoc-gen-go@${protoc_gen_go_version}"

archive="$tools_dir/protoc.zip"
curl --fail --location --silent --show-error \
	"https://github.com/protocolbuffers/protobuf/releases/download/v${protoc_version}/protoc-${protoc_version}-${protoc_platform}.zip" \
	--output "$archive"
actual_sha256="$(shasum -a 256 "$archive" | awk '{print $1}')"
if [[ "$actual_sha256" != "$protoc_sha256" ]]; then
	printf 'protoc archive checksum mismatch: expected %s, got %s\n' \
		"$protoc_sha256" "$actual_sha256" >&2
	exit 1
fi
unzip -q "$archive" -d "$tools_dir"
rm "$archive"

actual_buf_version="$("$tools_dir/bin/buf" --version)"
actual_protoc_version="$("$tools_dir/bin/protoc" --version)"
actual_protoc_gen_go_version="$("$tools_dir/bin/protoc-gen-go" --version)"
if [[ "$actual_buf_version" != "${buf_version#v}" ||
	"$actual_protoc_version" != "libprotoc ${protoc_version}" ||
	"$actual_protoc_gen_go_version" != "protoc-gen-go ${protoc_gen_go_version}" ]]; then
	printf 'installed Protobuf tool versions do not match the requested versions:\n' >&2
	printf '  buf: %s\n  protoc: %s\n  protoc-gen-go: %s\n' \
		"$actual_buf_version" "$actual_protoc_version" "$actual_protoc_gen_go_version" >&2
	exit 1
fi

if [[ -n "${GITHUB_PATH:-}" ]]; then
	printf '%s\n' "$tools_dir/bin" >>"$GITHUB_PATH"
fi
if [[ -n "${GITHUB_ENV:-}" ]]; then
	printf 'PROTOC_INCLUDE=%s\n' "$tools_dir/include" >>"$GITHUB_ENV"
fi

printf 'Installed buf %s, protoc %s, and protoc-gen-go %s in %s\n' \
	"$buf_version" "$protoc_version" "$protoc_gen_go_version" "$tools_dir"
