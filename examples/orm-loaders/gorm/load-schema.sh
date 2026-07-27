#!/bin/sh

set -eu

script_dir=$(
	CDPATH=
	cd -- "$(dirname -- "$0")"
	pwd
)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ptah-gorm-loader.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
trap 'exit 1' HUP INT TERM

cp "$script_dir/go.mod" "$script_dir/go.sum" "$work_dir/"
cp -R "$script_dir/models" "$work_dir/"

cd "$work_dir"
GOWORK=off go mod edit -require=ariga.io/atlas-provider-gorm@v0.6.1
GOWORK=off go run -mod=mod ariga.io/atlas-provider-gorm \
	load \
	--path ./models \
	--dialect postgres
